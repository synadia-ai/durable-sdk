// @synadia-ai/durable — the payload-codec seam (encryption at rest), pure: no NATS.
//
// The bar here is the one the offload tranche set: prove the mechanism, then prove each way it
// could be SILENTLY wrong is loud instead. The dangerous failure is not "encryption breaks" — it is
// "a reader without the codec decodes a transformed payload as if it were the value".

import { describe, expect, it } from "vitest";
import { PayloadCodecError, SerializationError } from "../../src/errors.js";
import { InMemoryBlobStore, resolveEntry, withBlobRef } from "../../src/journal/blobs.js";
import { jsonCodec } from "../../src/journal/codec.js";
import { type JournalEntry, stamp } from "../../src/journal/entry.js";
import { aesGcmPayloadCodec, type PayloadCodec } from "../../src/journal/payload-codec.js";
import { InMemoryJournal } from "../../src/journal/store.js";
import { shrinkWorkItem, type WorkItem } from "../../src/nats/protocol.js";
import { WIRE_VERSION, WIRE_VERSION_BLOBS, WIRE_VERSION_ENCODED } from "../../src/version.js";

const KEY = new Uint8Array(32).fill(7);
const OTHER_KEY = new Uint8Array(32).fill(9);
const codec = (): PayloadCodec => aesGcmPayloadCodec(KEY);

/** A visible, reversible stand-in when the assertion is about plumbing rather than crypto. */
const rot: PayloadCodec = {
  encode: (b) => b.map((x) => (x + 1) & 0xff),
  decode: (b) => b.map((x) => (x - 1) & 0xff),
};

const TS = 1_700_000_000_000;
const startEntry = (input: unknown, seq = 0): JournalEntry =>
  stamp({ seq, kind: "start", wf: "w", wfVersion: 1, input }, TS);
const stepEnd = (value: unknown, seq = 1): JournalEntry =>
  stamp({ seq, kind: "step-end", name: "s", value }, TS);

async function appended(j: InMemoryJournal, runId: string, e: JournalEntry, expectLast: number) {
  return j.append(runId, {
    entry: e,
    msgID: `${runId}:${e.kind}:${e.seq}`,
    expectLastSeq: expectLast,
  });
}

describe("payload codec — round trip", () => {
  it("a transformed payload comes back byte-identical through load()", async () => {
    const j = new InMemoryJournal(jsonCodec, { payloadCodec: codec() });
    const input = { card: "4242424242424242", cvc: "123", nested: [1, { deep: true }] };
    await appended(j, "r1", startEntry(input), 0);
    const [loaded] = await j.load("r1");
    expect((loaded?.entry as { input: unknown }).input).toEqual(input);
  });

  it("every payload-carrying kind round-trips (the slot map is covered, not just `input`)", async () => {
    const j = new InMemoryJournal(jsonCodec, { payloadCodec: codec() });
    const kinds: JournalEntry[] = [
      stamp({ seq: 0, kind: "start", wf: "w", wfVersion: 1, input: { a: 1 } }, TS),
      stamp({ seq: 1, kind: "step-end", name: "s", value: { b: 2 } }, TS),
      stamp({ seq: 2, kind: "signal", name: "sig", data: { c: 3 }, signalId: "i" }, TS),
      stamp({ seq: 3, kind: "call", name: "c", childRunId: "k", wf: "w2", input: { d: 4 } }, TS),
      stamp({ seq: 4, kind: "resolve", resolvesSeq: 3, value: { e: 5 } }, TS),
      stamp({ seq: 5, kind: "resolve", resolvesSeq: 3, error: { name: "E", message: "m" } }, TS),
      stamp({ seq: 6, kind: "result", value: { f: 6 } }, TS),
    ];
    let last = 0;
    for (const e of kinds) last = (await appended(j, "r2", e, last)).streamSeq;
    const loaded = await j.load("r2");
    // `v` legitimately moves 1 -> 3 on the way through; everything else must survive untouched.
    expect(loaded.map((l) => ({ ...l.entry, v: WIRE_VERSION }))).toEqual(kinds);
    expect(loaded.map((l) => l.entry.v)).toEqual(kinds.map(() => WIRE_VERSION_ENCODED));
  });

  it("leaves the envelope readable — only the payload slot is transformed", async () => {
    const j = new InMemoryJournal(jsonCodec, { payloadCodec: rot });
    await appended(j, "r3", stepEnd({ secret: "s3cr3t" }), 0);
    // reach the STORED bytes the way an observer with read rights would
    const raw = JSON.parse(
      new TextDecoder().decode(
        (
          j as unknown as {
            logs: Map<string, { data: Uint8Array }[]>;
          }
        ).logs.get("r3")![0]!.data,
      ),
    ) as Record<string, unknown>;

    expect(raw.kind).toBe("step-end");
    expect(raw.name).toBe("s"); // step names stay legible: the dashboard still works
    expect(raw.seq).toBe(1);
    expect(raw.v).toBe(WIRE_VERSION_ENCODED);
    expect(raw.value).toBeUndefined(); // the plaintext slot is GONE
    expect(typeof raw.valueEnc).toBe("string");
    expect(JSON.stringify(raw)).not.toContain("s3cr3t");
  });

  it("entries with no payload slot are untouched and stay v:1", async () => {
    const j = new InMemoryJournal(jsonCodec, { payloadCodec: codec() });
    const e = stamp({ seq: 0, kind: "now", value: 1700000000000 }, TS) as JournalEntry;
    await appended(j, "r4", e, 0);
    const [loaded] = await j.load("r4");
    expect(loaded?.entry.v).toBe(WIRE_VERSION);
    expect((loaded?.entry as { value: unknown }).value).toBe(1700000000000);
  });
});

describe("payload codec — the silent-corruption guards", () => {
  it("A READER WITHOUT THE CODEC PARKS, it does not hand the workflow base64", async () => {
    const store = new InMemoryJournal(jsonCodec, { payloadCodec: codec() });
    await appended(store, "r5", startEntry({ secret: 1 }), 0);
    // same underlying log, no payloadCodec — this is the misconfigured peer
    const blind = new InMemoryJournal(jsonCodec, {});
    (blind as unknown as { logs: unknown }).logs = (store as unknown as { logs: unknown }).logs;

    await expect(blind.load("r5")).rejects.toThrow(PayloadCodecError);
    await expect(blind.load("r5")).rejects.toThrow(/no payloadCodec configured/);
  });

  it("CONTROL: without the transform the same blind reader reads the value happily", async () => {
    // Proves the guard above is doing the work — not that load() simply always throws.
    const store = new InMemoryJournal(jsonCodec, {});
    await appended(store, "r6", startEntry({ secret: 1 }), 0);
    const blind = new InMemoryJournal(jsonCodec, {});
    (blind as unknown as { logs: unknown }).logs = (store as unknown as { logs: unknown }).logs;
    const [loaded] = await blind.load("r6");
    expect((loaded?.entry as { input: unknown }).input).toEqual({ secret: 1 });
  });

  it("the WRONG KEY parks — it never yields a plausible-looking value", async () => {
    const store = new InMemoryJournal(jsonCodec, { payloadCodec: aesGcmPayloadCodec(KEY) });
    await appended(store, "r7", startEntry({ amount: 100 }), 0);
    const wrong = new InMemoryJournal(jsonCodec, { payloadCodec: aesGcmPayloadCodec(OTHER_KEY) });
    (wrong as unknown as { logs: unknown }).logs = (store as unknown as { logs: unknown }).logs;

    await expect(wrong.load("r7")).rejects.toThrow(PayloadCodecError);
    await expect(wrong.load("r7")).rejects.toThrow(/wrong key, or the bytes were altered/);
  });

  it("a TAMPERED ciphertext parks (AES-GCM is authenticated, so this is detected)", async () => {
    const j = new InMemoryJournal(jsonCodec, { payloadCodec: codec() });
    await appended(j, "r8", startEntry({ amount: 100 }), 0);
    const logs = (j as unknown as { logs: Map<string, { data: Uint8Array }[]> }).logs;
    const slot = logs.get("r8")![0]!;
    const obj = JSON.parse(new TextDecoder().decode(slot.data)) as Record<string, unknown>;
    const b = Buffer.from(obj.inputEnc as string, "base64");
    b[b.length - 1] = (b[b.length - 1] ?? 0) ^ 0xff; // flip a bit in the auth tag
    obj.inputEnc = b.toString("base64");
    slot.data = new TextEncoder().encode(JSON.stringify(obj));

    await expect(j.load("r8")).rejects.toThrow(PayloadCodecError);
  });

  it("a malformed *Enc (not base64) parks rather than producing empty bytes", async () => {
    const j = new InMemoryJournal(jsonCodec, { payloadCodec: codec() });
    await appended(j, "r9", startEntry({ a: 1 }), 0);
    const logs = (j as unknown as { logs: Map<string, { data: Uint8Array }[]> }).logs;
    const slot = logs.get("r9")![0]!;
    const obj = JSON.parse(new TextDecoder().decode(slot.data)) as Record<string, unknown>;
    obj.inputEnc = "!!!not base64!!!";
    slot.data = new TextEncoder().encode(JSON.stringify(obj));
    await expect(j.load("r9")).rejects.toThrow(PayloadCodecError);
  });

  it("a codec that throws on ENCODE surfaces as PayloadCodecError, not a lost append", async () => {
    const boom: PayloadCodec = {
      encode: () => {
        throw new Error("kms unavailable");
      },
      decode: (b) => b,
    };
    const j = new InMemoryJournal(jsonCodec, { payloadCodec: boom });
    await expect(appended(j, "r10", startEntry({ a: 1 }), 0)).rejects.toThrow(PayloadCodecError);
    expect(await j.load("r10")).toEqual([]); // nothing was written
  });

  it("an UNENCODABLE value still fails as SerializationError (class not swallowed by the codec)", async () => {
    const j = new InMemoryJournal(jsonCodec, { payloadCodec: codec() });
    await expect(appended(j, "r11", startEntry({ bad: NaN }), 0)).rejects.toThrow(
      SerializationError,
    );
  });
});

describe("payload codec × offload", () => {
  const big = { blob: "x".repeat(5000) };

  it("an oversized transformed payload offloads, and the BLOB holds ciphertext (not plaintext)", async () => {
    const blobs = new InMemoryBlobStore();
    const j = new InMemoryJournal(jsonCodec, {
      blobs,
      offloadThresholdBytes: 1024,
      payloadCodec: codec(),
    });
    await appended(j, "r12", startEntry(big), 0);

    const stored = [...(blobs as unknown as { objects: Map<string, Uint8Array> }).objects.values()];
    expect(stored).toHaveLength(1);
    expect(new TextDecoder().decode(stored[0])).not.toContain("xxxxx");

    const [loaded] = await j.load("r12");
    expect((loaded?.entry as { input: unknown }).input).toEqual(big);
    expect(loaded?.entry.v).toBe(WIRE_VERSION_ENCODED); // v:3 survives offload — the reader must untransform
  });

  it("CONTROL: without a payload codec the same blob holds readable plaintext", async () => {
    const blobs = new InMemoryBlobStore();
    const j = new InMemoryJournal(jsonCodec, { blobs, offloadThresholdBytes: 1024 });
    await appended(j, "r13", startEntry(big), 0);
    const stored = [...(blobs as unknown as { objects: Map<string, Uint8Array> }).objects.values()];
    expect(new TextDecoder().decode(stored[0])).toContain("xxxxx");
    const [loaded] = await j.load("r13");
    expect(loaded?.entry.v).toBe(WIRE_VERSION_BLOBS);
  });

  it("an offloaded v:3 entry parks for a reader with the blobs but no codec", async () => {
    const blobs = new InMemoryBlobStore();
    const j = new InMemoryJournal(jsonCodec, {
      blobs,
      offloadThresholdBytes: 1024,
      payloadCodec: codec(),
    });
    await appended(j, "r14", startEntry(big), 0);
    const blind = new InMemoryJournal(jsonCodec, { blobs, offloadThresholdBytes: 1024 });
    (blind as unknown as { logs: unknown }).logs = (j as unknown as { logs: unknown }).logs;
    await expect(blind.load("r14")).rejects.toThrow(PayloadCodecError);
  });
});

describe("payload codec × work items", () => {
  // `shrinkWorkItem` is the one payload path that does not run through a journal append: an
  // oversized signal/child-done is offloaded BEFORE any entry exists to transform. A blob is at
  // rest, so it has to be transformed here or encryption would silently depend on payload size.
  const bigSecret = "pan-4242424242424242" + "x".repeat(4000);
  const offloadOf = (blobs: InMemoryBlobStore) => ({ blobs, thresholdBytes: 1024 });
  const stored = (blobs: InMemoryBlobStore): string[] =>
    [...(blobs as unknown as { objects: Map<string, Uint8Array> }).objects.values()].map((b) =>
      new TextDecoder().decode(b),
    );

  const signalItem = (): WorkItem => ({
    runId: "r",
    kind: "signal",
    signal: { name: "card", data: bigSecret, id: "s1" },
  });

  it("an oversized SIGNAL offloads ciphertext and marks the item payloadEnc", async () => {
    const blobs = new InMemoryBlobStore();
    const out = await shrinkWorkItem(signalItem(), jsonCodec, offloadOf(blobs), aesGcmPayloadCodec(KEY)); // prettier-ignore
    expect(out.signal?.data).toBeUndefined();
    expect(out.signal?.dataRef?.name).toMatch(/^r\/sha256:[0-9a-f]{64}$/);
    expect(out.payloadEnc).toBe(true);
    expect(stored(blobs)).toHaveLength(1);
    expect(stored(blobs)[0]).not.toContain("4242424242424242");
  });

  it("CONTROL: without a codec the same item's blob is plaintext and carries no flag", async () => {
    const blobs = new InMemoryBlobStore();
    const out = await shrinkWorkItem(signalItem(), jsonCodec, offloadOf(blobs));
    expect(out.signal?.dataRef).toBeDefined();
    expect(out.payloadEnc).toBeUndefined();
    expect(stored(blobs)[0]).toContain("4242424242424242");
  });

  it("an oversized child-done VALUE offloads ciphertext", async () => {
    const blobs = new InMemoryBlobStore();
    const item: WorkItem = {
      runId: "r",
      kind: "child-done",
      resolvesSeq: 3,
      child: { id: "r_0", ok: true, value: bigSecret },
    };
    const out = await shrinkWorkItem(item, jsonCodec, offloadOf(blobs), aesGcmPayloadCodec(KEY));
    expect(out.child).toMatchObject({ id: "r_0", ok: true });
    expect(out.payloadEnc).toBe(true);
    expect(stored(blobs)[0]).not.toContain("4242424242424242");
  });

  it("an oversized child-done ERROR offloads ciphertext (the failure path leaks too)", async () => {
    const blobs = new InMemoryBlobStore();
    const item: WorkItem = {
      runId: "r",
      kind: "child-done",
      resolvesSeq: 3,
      child: { id: "r_0", ok: false, error: { name: "Error", message: bigSecret } },
    };
    const out = await shrinkWorkItem(item, jsonCodec, offloadOf(blobs), aesGcmPayloadCodec(KEY));
    expect(out.payloadEnc).toBe(true);
    expect(stored(blobs)[0]).not.toContain("4242424242424242");
  });

  it("an item UNDER the threshold is untouched — no blob, no flag", async () => {
    const blobs = new InMemoryBlobStore();
    const item: WorkItem = {
      runId: "r",
      kind: "signal",
      signal: { name: "card", data: "small", id: "s1" },
    };
    const out = await shrinkWorkItem(item, jsonCodec, offloadOf(blobs), aesGcmPayloadCodec(KEY));
    expect(out).toEqual(item); // the journal append encrypts this one instead
    expect(stored(blobs)).toHaveLength(0);
  });

  it("a codec that throws surfaces as PayloadCodecError, and NOTHING is written to the store", async () => {
    // Blob-before-pointer means a written blob is a commitment; a failed transform must abort first.
    const blobs = new InMemoryBlobStore();
    const boom: PayloadCodec = {
      encode: () => {
        throw new Error("kms unavailable");
      },
      decode: (b) => b,
    };
    await expect(shrinkWorkItem(signalItem(), jsonCodec, offloadOf(blobs), boom)).rejects.toThrow(
      PayloadCodecError,
    );
    expect(stored(blobs)).toHaveLength(0);
  });
});

describe("the v:2 / v:3 flag on a blob ref is load-bearing", () => {
  // The work item tells the worker whether its blob is transformed; the worker stamps the entry
  // accordingly. Both ways of getting that wrong must be LOUD — a reader that guesses would either
  // hand a workflow base64 ciphertext or decrypt plaintext into nonsense.
  const codec = aesGcmPayloadCodec(KEY);

  async function refTo(blobs: InMemoryBlobStore, bytes: Uint8Array) {
    const digest = `sha256:${[...new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256", bytes))].map((b) => b.toString(16).padStart(2, "0")).join("")}`; // prettier-ignore
    const name = `r/${digest}`;
    await blobs.put(name, bytes);
    return { name, digest, size: bytes.byteLength };
  }

  it("a v:2 ref pointing at CIPHERTEXT parks — it never yields the base64 as a value", async () => {
    const blobs = new InMemoryBlobStore();
    const ref = await refTo(blobs, await codec.encode(jsonCodec.encodeValue({ card: "4242" })));
    const entry = withBlobRef(stepEnd({ card: "4242" }), "value", ref, false); // the WRONG flag
    expect(entry.v).toBe(WIRE_VERSION_BLOBS);
    await expect(resolveEntry("r", entry, jsonCodec, blobs, undefined, codec)).rejects.toThrow(
      PayloadCodecError,
    );
  });

  it("a v:3 ref pointing at PLAINTEXT parks too (the mirror-image mistake)", async () => {
    const blobs = new InMemoryBlobStore();
    const ref = await refTo(blobs, jsonCodec.encodeValue({ card: "4242" }));
    const entry = withBlobRef(stepEnd({ card: "4242" }), "value", ref, true); // the WRONG flag
    expect(entry.v).toBe(WIRE_VERSION_ENCODED);
    await expect(resolveEntry("r", entry, jsonCodec, blobs, undefined, codec)).rejects.toThrow(
      PayloadCodecError,
    );
  });

  it("CONTROL: each flag paired with the bytes it describes resolves to the value", async () => {
    const blobs = new InMemoryBlobStore();
    const plain = await refTo(blobs, jsonCodec.encodeValue({ card: "4242" }));
    const cipher = await refTo(blobs, await codec.encode(jsonCodec.encodeValue({ card: "4242" })));
    for (const [ref, flag] of [
      [plain, false],
      [cipher, true],
    ] as const) {
      const resolved = await resolveEntry(
        "r",
        withBlobRef(stepEnd({ card: "4242" }), "value", ref, flag),
        jsonCodec,
        blobs,
        undefined,
        codec,
      );
      expect((resolved as { value: unknown }).value).toEqual({ card: "4242" });
    }
  });
});

describe("aesGcmPayloadCodec", () => {
  it("rejects a key that is not 32 bytes, at first use", async () => {
    const c = aesGcmPayloadCodec(new Uint8Array(16));
    await expect(Promise.resolve(c.encode(new Uint8Array([1])))).rejects.toThrow(/32 bytes/);
  });

  it("is non-deterministic: the same plaintext encrypts differently each time", async () => {
    const c = codec();
    const p = new TextEncoder().encode("same");
    const a = await c.encode(p);
    const b = await c.encode(p);
    expect(Buffer.from(a).equals(Buffer.from(b))).toBe(false);
    expect(new TextDecoder().decode(await c.decode(a))).toBe("same");
    expect(new TextDecoder().decode(await c.decode(b))).toBe("same");
  });

  it("rejects ciphertext too short to carry an IV", async () => {
    await expect(Promise.resolve(codec().decode(new Uint8Array(4)))).rejects.toThrow(
      PayloadCodecError,
    );
  });
});

describe("payload codec — defensive branches", () => {
  // Reachable only through a newer writer, a hand-edited stream, or a codec that is not actually
  // reversible. Each one is a place where the wrong answer is "quietly produce a value".

  it("a v:3 entry carrying NEITHER an *Enc nor a *Ref parks (uninterpretable, not empty)", async () => {
    const j = new InMemoryJournal(jsonCodec, { payloadCodec: codec() });
    await appended(j, "d1", startEntry({ a: 1 }), 0);
    const logs = (j as unknown as { logs: Map<string, { data: Uint8Array }[]> }).logs;
    const slot = logs.get("d1")![0]!;
    const obj = JSON.parse(new TextDecoder().decode(slot.data)) as Record<string, unknown>;
    delete obj.inputEnc; // v:3 stays, both stand-ins gone
    slot.data = new TextEncoder().encode(JSON.stringify(obj));
    await expect(j.load("d1")).rejects.toThrow(/neither an inline nor an offloaded payload slot/);
  });

  it("a non-string *Enc parks (a number is not a base64 payload)", async () => {
    const j = new InMemoryJournal(jsonCodec, { payloadCodec: codec() });
    await appended(j, "d2", startEntry({ a: 1 }), 0);
    const logs = (j as unknown as { logs: Map<string, { data: Uint8Array }[]> }).logs;
    const slot = logs.get("d2")![0]!;
    const obj = JSON.parse(new TextDecoder().decode(slot.data)) as Record<string, unknown>;
    obj.inputEnc = 42;
    slot.data = new TextEncoder().encode(JSON.stringify(obj));
    await expect(j.load("d2")).rejects.toThrow(/expected a base64 string/);
  });

  it("a codec whose output does not decode back to a value parks, rather than yielding garbage", async () => {
    // A lossy/mismatched codec (wrong algorithm, a compression change) decrypts "successfully" into
    // bytes that are not an encoded value. That must be loud, not a mystery value in a workflow.
    const lossy: PayloadCodec = { encode: (b) => b, decode: () => new TextEncoder().encode("{{{") };
    const j = new InMemoryJournal(jsonCodec, { payloadCodec: lossy });
    await appended(j, "d3", startEntry({ a: 1 }), 0);
    await expect(j.load("d3")).rejects.toThrow(/not a valid encoded value/);
  });

  it("accepts an already-imported WebCrypto key, not just raw bytes", async () => {
    const imported = await globalThis.crypto.subtle.importKey("raw", KEY, "AES-GCM", false, [
      "encrypt",
      "decrypt",
    ]);
    const c = aesGcmPayloadCodec(imported);
    const round = await c.decode(await c.encode(new TextEncoder().encode("hi")));
    expect(new TextDecoder().decode(round)).toBe("hi");
  });

  it("does not transform an entry twice (a pass-through append is written verbatim)", async () => {
    // Work-item payloads are journaled verbatim; double-transforming one would make it unreadable.
    const j = new InMemoryJournal(jsonCodec, { payloadCodec: rot });
    const pre = { ...(startEntry({ a: 1 }) as unknown as Record<string, unknown>) };
    delete pre.input;
    pre.inputEnc = "YWxyZWFkeQ==";
    pre.v = WIRE_VERSION_ENCODED;
    await appended(j, "d4", pre as unknown as JournalEntry, 0);
    const logs = (j as unknown as { logs: Map<string, { data: Uint8Array }[]> }).logs;
    const obj = JSON.parse(new TextDecoder().decode(logs.get("d4")![0]!.data)) as Record<
      string,
      unknown
    >;
    expect(obj.inputEnc).toBe("YWxyZWFkeQ=="); // untouched
  });
});
