// Payload encryption at rest, end to end against a real nats-server.
//
// The unit suite proves the transform and its guards. What only a real server can prove is the
// claim an operator actually cares about: run a workflow through a real worker and a real client,
// then read the journal stream the way a principal with `subscribe de.journal.>` would — and find
// no secret in it.

import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import { readSubject } from "../harness/shell/direct.js";
import { listObjects, readBlob } from "../harness/blobs.js";
import { maxPayload } from "../harness/payload.js";
import { type NatsConnection } from "@nats-io/transport-node";
import { connect } from "../harness/existing.js";
import { afterAll, beforeAll, describe, expect, inject, it } from "vitest";
import { defineWorkflow } from "../../src/core/workflow.js";
import { jsonCodec } from "../../src/journal/codec.js";
import { aesGcmPayloadCodec } from "../../src/journal/payload-codec.js";
import { createClient } from "../harness/engine.js";
import { NatsJournal } from "../harness/shell/journal-store.js";
import { names } from "../harness/shell/names.js";
import { createWorker } from "../harness/engine.js";
import { WIRE_VERSION_BLOBS, WIRE_VERSION_ENCODED } from "../../src/version.js";
import { WAN } from "../harness/mode.js";

const natsUrl = inject("natsUrl");
const KEY = new Uint8Array(32).fill(42);
const MIB = 1024 * 1024;

// One resource set per CONFIGURATION, not per test: each prefix reserves max_bytes across six
// streams, and the harness's JetStream store is a fixed budget shared by every e2e prefix.
async function waitUntil(cond: () => Promise<boolean>, timeoutMs: number): Promise<void> {
  timeoutMs *= WAN; // durable-sdk: × the round-trip factor (harness/mode.ts, F29)
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await cond()) return;
    if (Date.now() > deadline) throw new Error("waitUntil timed out");
    await new Promise((r) => setTimeout(r, 50));
  }
}

const ENC = "encA";
const PLAIN = "encB";

/** Every blob this run owns, as raw bytes — what a reader of the `DE_BLOBS` bucket would see. */
async function rawBlobs(nc: NatsConnection, prefix: string, runId: string): Promise<string[]> {
  const bucket = names(prefix).blobsBucket;
  const mine = (await listObjects(nc, bucket)).filter((o) => o.name.startsWith(`${runId}/`));
  const out: string[] = [];
  for (const o of mine) {
    const bytes = await readBlob(nc, bucket, o.name);
    if (bytes) out.push(new TextDecoder().decode(bytes));
  }
  return out;
}

/** One decoded journal entry of a given kind, in its STORED form (refs unresolved). */
async function rawEntry(
  nc: NatsConnection,
  prefix: string,
  runId: string,
  kind: string,
): Promise<Record<string, unknown>> {
  const jsm = await jetstreamManager(nc);
  const journal = new NatsJournal(jetstream(nc), jsm, names(prefix), jsonCodec);
  const found = (await journal.loadRaw(runId)).find((l) => l.entry.kind === kind);
  if (!found) throw new Error(`run ${runId}: no ${kind} entry on the journal`);
  return found.entry as unknown as Record<string, unknown>;
}

/**
 * Every raw byte on a run's journal subject, exactly as a reader of `de.journal.>` would see it —
 * through a direct get, so the sweep itself creates no consumer. It used to fetch through an
 * ephemeral pull consumer it never deleted; the server expired that after five seconds, which
 * hid it from the end-of-run leftover check and not from the per-test one (setup-each.ts).
 */
async function rawJournal(nc: NatsConnection, prefix: string, runId: string): Promise<string[]> {
  const n = names(prefix);
  const jsm = await jetstreamManager(nc);
  const read = await readSubject(jsm, n.journalStream, n.journalSubject(runId), 100);
  return read.out.map((m) => new TextDecoder().decode(m.data));
}

describe.skipIf(!natsUrl)("payload encryption at rest (real nats-server)", () => {
  let nc: NatsConnection;
  beforeAll(async () => {
    nc = await connect({ servers: natsUrl! });
  });
  afterAll(async () => {
    await nc.drain();
  });

  const SECRET = "pan-4242424242424242";

  // prettier-ignore
  it("a run completes normally, and NOTHING on the journal stream contains the secret", async () => {
    const payloadCodec = aesGcmPayloadCodec(KEY);
    const wf = defineWorkflow("enc_e2e", async (ctx, input: { card: string }) => {
      const auth = await ctx.run("authorize", () => `auth-for-${input.card}`);
      return { auth };
    });
    const prefix = ENC;
    const worker = createWorker({ nc, workflows: [wf], prefix, payloadCodec });
    await worker.start();
    try {
      const client = createClient({ nc, prefix, payloadCodec });
      const h = await client.start(wf, { card: SECRET });
      // the workflow SEES plaintext — encryption is at rest, not in the programming model
      expect(await h.result(20_000)).toEqual({ auth: `auth-for-${SECRET}` });

      const raw = await rawJournal(nc, prefix, h.runId);
      expect(raw.length).toBeGreaterThan(2);
      const all = raw.join("\n");
      expect(all).not.toContain(SECRET);
      expect(all).not.toContain("auth-for-");

      // the envelope is still legible — this is what keeps the dashboard and replay working
      expect(all).toContain('"kind":"step-begin"');
      expect(all).toContain('"name":"authorize"');
      const parsed = raw.map((r) => JSON.parse(r) as Record<string, unknown>);
      expect(
        parsed.some((e) => e.v === WIRE_VERSION_ENCODED && typeof e.inputEnc === "string"),
      ).toBe(true);
      expect(
        parsed.some((e) => e.v === WIRE_VERSION_ENCODED && typeof e.valueEnc === "string"),
      ).toBe(true);

      // …and the STATUS KV, which is the weakest read right we hand out (the dashboard's, the
      // read-only observer's), must not hold the finished value in the clear either.
      const kv = await new Kvm(nc).open(names(prefix).runsKv);
      const rec = await kv.get(h.runId);
      const hint = JSON.parse(new TextDecoder().decode(rec!.value)) as Record<string, unknown>;
      expect(hint.status).toBe("completed"); // the shape of the run is still visible
      expect(hint.wf).toBe("enc_e2e");
      expect(hint.result).toBeUndefined(); // the value is not
      expect(JSON.stringify(hint)).not.toContain("auth-for-");
    } finally {
      await worker.stop();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // prettier-ignore
  it("CONTROL: without the codec the very same run leaks the secret in plaintext", async () => {
    // Without this the test above proves nothing — a typo'd secret would also "not appear".
    const wf = defineWorkflow("enc_e2e_plain", async (ctx, input: { card: string }) => {
      const auth = await ctx.run("authorize", () => `auth-for-${input.card}`);
      return { auth };
    });
    const prefix = PLAIN;
    const worker = createWorker({ nc, workflows: [wf], prefix });
    await worker.start();
    try {
      const client = createClient({ nc, prefix });
      const h = await client.start(wf, { card: SECRET });
      await h.result(20_000);
      const all = (await rawJournal(nc, prefix, h.runId)).join("\n");
      expect(all).toContain(SECRET);
      expect(all).toContain("auth-for-");
      const kv = await new Kvm(nc).open(names(prefix).runsKv);
      const hint = new TextDecoder().decode((await kv.get(h.runId))!.value);
      expect(hint).toContain("auth-for-"); // the status KV leaks the result too, without a codec
    } finally {
      await worker.stop();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // prettier-ignore
  it("a client WITHOUT the codec cannot read the result — it fails loud, never returns base64", async () => {
    const payloadCodec = aesGcmPayloadCodec(KEY);
    const wf = defineWorkflow("enc_e2e_blind", async (ctx, input: { card: string }) =>
      ctx.run("mask", () => `masked:${input.card.slice(-4)}`),
    );
    const prefix = ENC; // shares the encrypted resource set
    const worker = createWorker({ nc, workflows: [wf], prefix, payloadCodec });
    await worker.start();
    try {
      const good = createClient({ nc, prefix, payloadCodec });
      const h = await good.start(wf, { card: SECRET });
      expect(await h.result(20_000)).toBe("masked:4242");

      const blind = createClient({ nc, prefix });
      await expect(blind.get(h.runId).result(5_000)).rejects.toThrow(/payloadCodec|wire version/);
    } finally {
      await worker.stop();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // prettier-ignore
  it("a WORKER without the codec PARKS the run — no terminal — and it heals when one arrives", async () => {
    // The feature's central claim, and a MUST in PROTOCOL §5.7. Parking rather than failing is the
    // whole point: a misconfigured worker must not destroy recoverable work, so this asserts both
    // halves — nothing terminal is written, AND a correctly-configured worker finishes the run.
    const payloadCodec = aesGcmPayloadCodec(KEY);
    const wf = defineWorkflow("enc_e2e_park", async (ctx, input: { card: string }) =>
      ctx.run("mask", () => `masked:${input.card.slice(-4)}`),
    );

    // A client WITH the codec writes a v:3 start entry…
    const client = createClient({ nc, prefix: ENC, payloadCodec });
    const h = await client.start(wf, { card: SECRET });

    // …which a worker WITHOUT it cannot read.
    const events: { name: string; fields: Record<string, unknown> }[] = [];
    const blind = createWorker({
      nc,
      workflows: [wf],
      prefix: ENC,
      metrics: { onEvent: (name, fields) => events.push({ name, fields }) },
    });
    await blind.start();
    try {
      await waitUntil(
        () =>
          Promise.resolve(
            events.some((e) => e.name === "work.done" && e.fields.outcome === "payload-codec"),
          ),
        15_000,
      );
      const kinds = (await rawJournal(nc, ENC, h.runId)).map(
        (r) => (JSON.parse(r) as { kind: string }).kind,
      );
      expect(kinds).toContain("start");
      expect(kinds).not.toContain("result");
      expect(kinds).not.toContain("fail"); // parked, NOT failed — the run is still recoverable
      expect((await h.status()).state).not.toBe("failed");
    } finally {
      await blind.stop();
    }

    // It heals the moment a worker with the codec picks the run up.
    const good = createWorker({ nc, workflows: [wf], prefix: ENC, payloadCodec });
    await good.start();
    try {
      expect(await h.result(20_000)).toBe("masked:4242");
    } finally {
      await good.stop();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // prettier-ignore
  it("survives a crash: a fresh worker with the codec replays an encrypted journal", async () => {
    const payloadCodec = aesGcmPayloadCodec(KEY);
    let executions = 0;
    const wf = defineWorkflow("enc_e2e_replay", async (ctx, input: { card: string }) => {
      const a = await ctx.run("charge", () => {
        executions++;
        return `receipt-${input.card.slice(-4)}`;
      });
      const b = await ctx.run("ship", () => `${a}/shipped`);
      return b;
    });
    const prefix = ENC; // shares the encrypted resource set
    const w1 = createWorker({ nc, workflows: [wf], prefix, payloadCodec });
    await w1.start();
    let runId: string;
    try {
      const client = createClient({ nc, prefix, payloadCodec });
      const h = await client.start(wf, { card: SECRET });
      runId = h.runId;
      expect(await h.result(20_000)).toBe("receipt-4242/shipped");
    } finally {
      await w1.stop();
    }
    const after = executions;

    // A second worker re-driving the same (terminal) run must read the encrypted journal without
    // re-running the step — replay over ciphertext is still replay.
    const w2 = createWorker({ nc, workflows: [wf], prefix, payloadCodec });
    await w2.start();
    try {
      const client2 = createClient({ nc, prefix, payloadCodec });
      expect(await client2.get(runId).result(10_000)).toBe("receipt-4242/shipped");
      expect(executions).toBe(after);
    } finally {
      await w2.stop();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)
});

// The payload paths that do NOT go through a journal append.
//
// A signal's data and a child's outcome ride `de.work.*` as work items, and an oversized one is
// offloaded to `DE_BLOBS` by `shrinkWorkItem` BEFORE any journal append exists to encrypt it — the
// worker then journals the ref verbatim. Work items themselves are plaintext by design (in flight,
// workqueue retention, consumed and gone), and that boundary is asserted below. A BLOB is not: it
// sits in the bucket until GC, which is exactly the "at rest" the codec exists for.
//
// If the shrink path skipped the codec, whether a payload were encrypted would depend on its SIZE,
// which nothing in the programming model says and nothing in the journal shows — the entry reads
// "offloaded", not "unencrypted".
describe.skipIf(!natsUrl)("payload encryption — the work-item boundary", () => {
  let nc: NatsConnection;
  beforeAll(async () => {
    nc = await connect({ servers: natsUrl! });
  });
  afterAll(async () => {
    await nc.drain();
  });

  const SECRET = "pan-4111111111111111";
  /** Twice this server's max_payload, so the work item MUST offload to be publishable at all. */
  let bigSecret = "";
  beforeAll(() => {
    bigSecret = `${SECRET}-${"s".repeat(2 * maxPayload(nc))}`;
  });

  // durable-sdk: docs/findings.md F6, answered by the engine's F20 (an inline `signal.dataEnc` on
  // the work item, journaled verbatim).
  // prettier-ignore
  it("a SMALL signal is encrypted inline — the baseline the large one has to match", async () => {
    const payloadCodec = aesGcmPayloadCodec(KEY);
    const wf = defineWorkflow("enc_sig_small", async (ctx) => {
      const data = await ctx.await<string>("card");
      return data.length;
    });
    const worker = createWorker({ nc, workflows: [wf], prefix: ENC, payloadCodec });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: ENC, payloadCodec });
      const h = await client.start(wf, {});
      await waitUntil(async () => (await h.status()).state === "suspended", 15_000);
      await client.signal(h.runId, "card", SECRET);
      expect(await h.result(20_000)).toBe(SECRET.length);

      const sig = await rawEntry(nc, ENC, h.runId, "signal");
      expect(sig.v).toBe(WIRE_VERSION_ENCODED);
      expect(sig.data).toBeUndefined();
      expect(typeof sig.dataEnc).toBe("string");
      expect((await rawJournal(nc, ENC, h.runId)).join("\n")).not.toContain(SECRET);
    } finally {
      await worker.stop();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // durable-sdk: docs/findings.md F1, answered by the engine's F25 (put and get) and the SDK's
  // chunked transport (src/engine/blobs.ts).
  // prettier-ignore
  it("a LARGE signal's BLOB holds ciphertext too — encryption cannot depend on payload size", async () => {
    const payloadCodec = aesGcmPayloadCodec(KEY);
    const wf = defineWorkflow("enc_sig_big", async (ctx) => {
      const data = await ctx.await<string>("card");
      return data.length;
    });
    const worker = createWorker({ nc, workflows: [wf], prefix: ENC, payloadCodec });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: ENC, payloadCodec });
      const h = await client.start(wf, {});
      await waitUntil(async () => (await h.status()).state === "suspended", 15_000);
      await client.signal(h.runId, "card", bigSecret);
      // The workflow still sees plaintext: this is encryption at rest, not in the model.
      expect(await h.result(20_000)).toBe(bigSecret.length);

      const blobs = await rawBlobs(nc, ENC, h.runId);
      expect(blobs).toHaveLength(1);
      expect(blobs[0]).not.toContain(SECRET);

      // …and the entry must SAY the blob is transformed (v:3), or a reader would hand the
      // workflow ciphertext instead of decrypting it.
      const sig = await rawEntry(nc, ENC, h.runId, "signal");
      expect(sig.v).toBe(WIRE_VERSION_ENCODED);
      expect(sig.data).toBeUndefined();
      expect((sig.dataRef as { name: string } | undefined)?.name).toMatch(
        new RegExp(`^${h.runId}/sha256:`),
      );
    } finally {
      await worker.stop();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // durable-sdk: docs/findings.md F1, answered by the engine's F25 (put and get) and the SDK's
  // chunked transport (src/engine/blobs.ts).
  // prettier-ignore
  it("CONTROL: without a codec the very same large signal lands in the bucket in the clear", async () => {
    // Proves the assertion above is load-bearing rather than a secret that never reached the blob.
    const wf = defineWorkflow("plain_sig_big", async (ctx) => {
      const data = await ctx.await<string>("card");
      return data.length;
    });
    const worker = createWorker({ nc, workflows: [wf], prefix: PLAIN });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: PLAIN });
      const h = await client.start(wf, {});
      await waitUntil(async () => (await h.status()).state === "suspended", 15_000);
      await client.signal(h.runId, "card", bigSecret);
      expect(await h.result(20_000)).toBe(bigSecret.length);

      const blobs = await rawBlobs(nc, PLAIN, h.runId);
      expect(blobs).toHaveLength(1);
      expect(blobs[0]).toContain(SECRET);
      expect((await rawEntry(nc, PLAIN, h.runId, "signal")).v).toBe(WIRE_VERSION_BLOBS);
    } finally {
      await worker.stop();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // durable-sdk: docs/findings.md F1, answered by the engine's F25 (put and get) and the SDK's
  // chunked transport (src/engine/blobs.ts).
  // prettier-ignore
  it("a LARGE child outcome is encrypted in the PARENT's blob, not only in the child's journal", async () => {
    // The second shrink path, and the easier one to miss: the child's own `result` entry is
    // encrypted by its journal append, but `notifyParent` rebuilds the work item from the RESOLVED
    // (decrypted) value, so the copy stored under the parent's runId is written by a different code
    // path entirely.
    const payloadCodec = aesGcmPayloadCodec(KEY);
    const child = defineWorkflow("enc_kid", async (ctx) =>
      ctx.run("expand", () => `${SECRET}-${"k".repeat(2 * maxPayload(nc))}`),
    );
    const parent = defineWorkflow("enc_dad", async (ctx) => (await ctx.call(child, {})).length);
    const worker = createWorker({ nc, workflows: [parent, child], prefix: ENC, payloadCodec });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: ENC, payloadCodec });
      const h = await client.start(parent, {});
      expect(await h.result(30_000)).toBe(SECRET.length + 1 + 2 * maxPayload(nc));

      // Both copies — the child's own, and the one the parent's journal references.
      for (const runId of [h.runId, `${h.runId}_0`]) {
        for (const blob of await rawBlobs(nc, ENC, runId)) {
          expect(blob).not.toContain(SECRET);
        }
      }
      const resolve = await rawEntry(nc, ENC, h.runId, "resolve");
      expect(resolve.v).toBe(WIRE_VERSION_ENCODED);
      expect(resolve.value).toBeUndefined();
    } finally {
      await worker.stop();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // prettier-ignore
  it("work items on de.work.* stay PLAINTEXT — the documented boundary, asserted so it cannot drift", async () => {
    // Deliberate: `DE_WORK` is a workqueue stream, so an item exists only until it is consumed, and
    // the engine reads the item's own fields (kind, runId, signal name) to dispatch. Encrypting it
    // would buy little and cost dispatch. What matters is that this is a DECISION, visible in a
    // test: the durable copy is the journal entry the worker appends, and that one is encrypted.
    const payloadCodec = aesGcmPayloadCodec(KEY);
    const wf = defineWorkflow("enc_workitem", async (ctx) => {
      const data = await ctx.await<string>("card");
      return data.length;
    });
    const worker = createWorker({ nc, workflows: [wf], prefix: ENC, payloadCodec });
    await worker.start();
    const client = createClient({ nc, prefix: ENC, payloadCodec });
    const h = await client.start(wf, {});
    await waitUntil(async () => (await h.status()).state === "suspended", 15_000);
    await worker.stop(); // stop consuming, so the item stays on the queue to be read

    await client.signal(h.runId, "card", SECRET);
    const n = names(ENC);
    const jsm = await jetstreamManager(nc);
    const stored = await jsm.streams.getMessage(n.workStream, {
      last_by_subj: n.workSubject("enc_workitem"),
    });
    const item = new TextDecoder().decode(stored!.data);
    // durable-sdk: assertion touched, `toContain(SECRET)` → `not.toContain(SECRET)`. The boundary
    // moved: the SDK encrypts a signal's data before it leaves the process (`dataEnc`), and the
    // engine, which never holds the key, queues the ciphertext (as a blob ref, F6). Nothing is
    // plaintext in flight any more.
    expect(item).not.toContain(SECRET);
    expect(item).toContain('"kind":"signal"');

    // …and the moment it is consumed, the durable form is encrypted.
    const resumed = createWorker({ nc, workflows: [wf], prefix: ENC, payloadCodec });
    await resumed.start();
    try {
      expect(await h.result(20_000)).toBe(SECRET.length);
      expect((await rawJournal(nc, ENC, h.runId)).join("\n")).not.toContain(SECRET);
    } finally {
      await resumed.stop();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)
});

// Encryption crossed with the three features that had never been run against it. They all reuse the
// same `encodePayload` / `resolveEntry` choke points, so they *should* be fine — but "should" is
// what a test is for, and each one journals through a path the plain charge workflow never touches:
// a child's own journal, a rolled-over generation, a terminal written by the cancel path.
describe.skipIf(!natsUrl)("payload encryption × fan-out, continue-as-new and cancellation", () => {
  let nc: NatsConnection;
  beforeAll(async () => {
    nc = await connect({ servers: natsUrl! });
  });
  afterAll(async () => {
    await nc.drain();
  });

  const SECRET = "pan-4444333322221111";
  const codec = (): ReturnType<typeof aesGcmPayloadCodec> => aesGcmPayloadCodec(KEY);

  /** Every entry of a run AND of every child run it spawned, as raw text off the stream. */
  async function journalsOf(runId: string, childCount: number): Promise<string> {
    const ids = [runId, ...Array.from({ length: childCount }, (_, i) => `${runId}_${i}`)];
    const chunks: string[] = [];
    for (const id of ids) chunks.push(...(await rawJournal(nc, ENC, id)));
    return chunks.join("\n");
  }

  // prettier-ignore
  it("FAN-OUT: three children, and no plaintext in the parent's journal or in any child's", async () => {
    const child = defineWorkflow("enc_fan_kid", async (ctx, input: { card: string; n: number }) =>
      ctx.run("mask", () => `${input.n}:${input.card.slice(-4)}`),
    );
    const parent = defineWorkflow("enc_fan_dad", async (ctx, input: { card: string }) => {
      const parts = await Promise.all(
        [0, 1, 2].map((n) => ctx.startCall(child, { card: input.card, n })),
      );
      return parts.join("|");
    });
    const worker = createWorker({
      nc,
      workflows: [parent, child],
      prefix: ENC,
      payloadCodec: codec(),
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: ENC, payloadCodec: codec() });
      const h = await client.start(parent, { card: SECRET });
      expect(await h.result(30_000)).toBe("0:1111|1:1111|2:1111");

      const all = await journalsOf(h.runId, 3);
      expect(all).not.toContain(SECRET);
      expect(all).not.toContain("1111"); // the masked value is a payload too
      // Each child's `start` carries the parent's secret as its INPUT — written by the parent's
      // worker through NatsChildren, a different append site from the client's start.
      const childStarts = (await rawJournal(nc, ENC, `${h.runId}_0`)).map(
        (r) => JSON.parse(r) as Record<string, unknown>,
      );
      const start = childStarts.find((e) => e.kind === "start");
      expect(start?.v).toBe(WIRE_VERSION_ENCODED);
      expect(start?.input).toBeUndefined();
      expect(typeof start?.inputEnc).toBe("string");
      // …and the envelope a dashboard needs is still legible.
      expect(start?.wf).toBe("enc_fan_kid");
    } finally {
      await worker.stop();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // prettier-ignore
  it("CONTINUE-AS-NEW: the secret crosses generations encrypted, in every generation's journal", async () => {
    const wf = defineWorkflow("enc_can", async (ctx, input: { card: string; n: number }) => {
      const seen = await ctx.run(`gen${input.n}`, () => `gen${input.n}:${input.card.slice(-4)}`);
      if (input.n < 2) await ctx.continueAsNew({ card: input.card, n: input.n + 1 });
      return seen;
    });
    const worker = createWorker({ nc, workflows: [wf], prefix: ENC, payloadCodec: codec() });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: ENC, payloadCodec: codec() });
      const h = await client.start(wf, { card: SECRET, n: 0 });
      expect(await h.result(30_000)).toBe("gen2:1111");

      // Each generation is its own run with its own journal; the rollover writes the NEXT
      // generation's start entry from inside the worker, which is the append site under test.
      for (const id of [h.runId, `${h.runId}_g1`, `${h.runId}_g2`]) {
        const raw = await rawJournal(nc, ENC, id);
        expect(raw.length).toBeGreaterThan(0);
        expect(raw.join("\n")).not.toContain(SECRET);
        const start = JSON.parse(raw[0]!) as Record<string, unknown>;
        expect(start.kind).toBe("start");
        expect(start.v).toBe(WIRE_VERSION_ENCODED);
        expect(typeof start.inputEnc).toBe("string");
      }
      expect((await client.get(`${h.runId}_g1`).status()).state).toBe("continued");
    } finally {
      await worker.stop();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // prettier-ignore
  it("CANCEL: compensation runs, the terminal is `cancelled`, and the reason stays legible", async () => {
    // The cancel path writes its terminal WITHOUT replaying a result, so it is the one terminal
    // append that never goes near a payload slot — and the `reason` is deliberately envelope, not
    // payload (an operator has to be able to read why a run was killed). Asserted both ways so
    // neither can drift.
    const refunded: string[] = [];
    const wf = defineWorkflow("enc_cancel", async (ctx, input: { card: string }) => {
      ctx.onCancel(() => ctx.run("refund", () => void refunded.push(input.card)));
      await ctx.run("charge", () => `charged:${input.card}`);
      await ctx.await("never");
      return "unreachable";
    });
    const worker = createWorker({ nc, workflows: [wf], prefix: ENC, payloadCodec: codec() });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: ENC, payloadCodec: codec() });
      const h = await client.start(wf, { card: SECRET });
      await waitUntil(async () => (await h.status()).state === "suspended", 15_000);

      await client.cancel(h.runId, { reason: "chargeback opened" });
      await expect(h.result(15_000)).rejects.toMatchObject({ name: "CancelledError" });
      expect((await h.status()).state).toBe("cancelled");
      expect(refunded).toEqual([SECRET]); // the handler saw plaintext, as user code always does

      const raw = (await rawJournal(nc, ENC, h.runId)).join("\n");
      expect(raw).not.toContain(SECRET);
      expect(raw).not.toContain("charged:");
      expect(raw).toContain('"kind":"cancelled"');
      expect(raw).toContain("chargeback opened"); // envelope: an operator must be able to read this
    } finally {
      await worker.stop();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // prettier-ignore
  it("a FAILURE's message is a payload — it is encrypted, and result() still rejects with it", async () => {
    // Easy to miss: `error` is in the payload slot map, so a failing workflow's message is
    // transformed too. If it were not, the most common place a secret leaks (an exception carrying
    // the input) would be exactly the place the codec did not cover.
    const wf = defineWorkflow("enc_fail", async (ctx, input: { card: string }) => {
      await ctx.run("try", () => 1);
      throw new Error(`declined for ${input.card}`);
    });
    const worker = createWorker({ nc, workflows: [wf], prefix: ENC, payloadCodec: codec() });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: ENC, payloadCodec: codec() });
      const h = await client.start(wf, { card: SECRET });
      await expect(h.result(20_000)).rejects.toMatchObject({
        name: "WorkflowFailure",
        message: expect.stringContaining(SECRET) as unknown,
      });

      const raw = (await rawJournal(nc, ENC, h.runId)).join("\n");
      expect(raw).not.toContain(SECRET);
      expect(raw).toContain('"kind":"fail"');
      const fail = await rawEntry(nc, ENC, h.runId, "fail");
      expect(fail.v).toBe(WIRE_VERSION_ENCODED);
      expect(fail.error).toBeUndefined();
      expect(typeof fail.errorEnc).toBe("string");
      // …and the status KV must not hold the message either.
      const kv = await new Kvm(nc).open(names(ENC).runsKv);
      const hint = new TextDecoder().decode((await kv.get(h.runId))!.value);
      expect(hint).toContain('"failed"');
      expect(hint).not.toContain(SECRET);
    } finally {
      await worker.stop();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)
});

// Multi-worker chaos with a codec configured — the combination neither SDK had run.
//
// The interaction worth proving is not "does it still work" but a specific one: the transform is
// deliberately NON-deterministic (a fresh nonce per call, so equal payloads do not produce equal
// bytes), while the engine's exactly-once guarantee rests on seq + CAS + msgID and never on
// comparing bytes. Two workers racing the same step therefore write DIFFERENT ciphertext for the
// same value, and the loser must adopt what was recorded rather than notice a mismatch.
describe.skipIf(!natsUrl)("payload encryption × multi-worker chaos", () => {
  let nc: NatsConnection;
  beforeAll(async () => {
    nc = await connect({ servers: natsUrl! });
  });
  afterAll(async () => {
    await nc.drain();
  });

  const SECRET = "pan-9999888877776666";

  /** Read a run's journal raw and count what has to be countable, whatever raced. */
  async function audit(runId: string): Promise<{
    terminals: string[];
    duplicateStepEnds: number[];
    duplicateSignals: string[];
    entries: Record<string, unknown>[];
  }> {
    const entries = (await rawJournal(nc, ENC, runId)).map(
      (r) => JSON.parse(r) as Record<string, unknown>,
    );
    const stepEnds = new Map<number, number>();
    const signals = new Map<string, number>();
    for (const e of entries) {
      if (e.kind === "step-end") stepEnds.set(e.seq as number, (stepEnds.get(e.seq as number) ?? 0) + 1); // prettier-ignore
      if (e.kind === "signal") signals.set(e.signalId as string, (signals.get(e.signalId as string) ?? 0) + 1); // prettier-ignore
    }
    return {
      terminals: entries
        .map((e) => e.kind as string)
        .filter((k) => ["result", "fail", "cancelled", "continued"].includes(k)),
      duplicateStepEnds: [...stepEnds].filter(([, n]) => n > 1).map(([seq]) => seq),
      duplicateSignals: [...signals].filter(([, n]) => n > 1).map(([id]) => id),
      entries,
    };
  }

  // prettier-ignore
  it("a signal storm across two workers: exactly-once journaling holds, and nothing leaks", async () => {
    const executions: string[] = [];
    const wf = defineWorkflow("enc_chaos", async (ctx, input: { card: string }) => {
      await ctx.await("go");
      const step = (name: string) => () => {
        executions.push(name);
        return `${name}:${input.card}`;
      };
      const a = await ctx.run("s1", step("s1"));
      const b = await ctx.run("s2", step("s2"));
      return `${a}/${b}`;
    });

    // lease OFF on purpose — this pins the contract the engine holds on its own, which is where a
    // codec could break something (the lease only makes the overlap rarer).
    const opts = { nc, workflows: [wf], prefix: ENC, payloadCodec: aesGcmPayloadCodec(KEY), lease: false }; // prettier-ignore
    const w1 = createWorker(opts);
    const w2 = createWorker(opts);
    await w1.start();
    await w2.start();
    try {
      const client = createClient({ nc, prefix: ENC, payloadCodec: aesGcmPayloadCodec(KEY) });
      const handles = await Promise.all([0, 1, 2].map(() => client.start(wf, { card: SECRET })));
      await new Promise((r) => setTimeout(r, 500)); // let them park, so the storm lands on parked runs
      await Promise.all(
        handles.flatMap((h) => Array.from({ length: 5 }, () => client.signal(h.runId, "go", {}))),
      );

      for (const h of handles) {
        expect(await h.result(30_000)).toBe(`s1:${SECRET}/s2:${SECRET}`);
      }
      // At-least-once EXECUTION is the contract; under overlap some steps re-ran.
      expect(new Set(executions)).toEqual(new Set(["s1", "s2"]));

      for (const h of handles) {
        const a = await audit(h.runId);
        expect(a.terminals).toEqual(["result"]); // exactly one, whatever raced
        expect(a.duplicateStepEnds).toEqual([]); // a step is JOURNALED once
        expect(a.duplicateSignals).toEqual([]); // and so is each signal, by msgID
        expect(JSON.stringify(a.entries)).not.toContain(SECRET);
        // Every payload-bearing entry is transformed — no entry slipped through in the clear
        // because the worker that wrote it happened to be racing.
        for (const e of a.entries) {
          if (["start", "step-end", "result"].includes(e.kind as string)) {
            expect(e.v).toBe(WIRE_VERSION_ENCODED);
          }
        }
      }
    } finally {
      await w1.stop();
      await w2.stop();
    }
  }, 90_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // prettier-ignore
  it("racing workers write DIFFERENT ciphertext for the same value — and the run is still exact", async () => {
    // The property the chaos test above depends on, isolated: encrypting the same value twice
    // yields different bytes, so a design that compared recorded bytes to fresh ones (rather than
    // consulting the journal by seq) would report a false non-determinism under any overlap.
    const codec = aesGcmPayloadCodec(KEY);
    const same = new TextEncoder().encode(JSON.stringify({ card: SECRET }));
    const [x, y] = [await codec.encode(same), await codec.encode(same)];
    expect(Buffer.from(x).equals(Buffer.from(y))).toBe(false);

    // …and the engine survives it: two workers, one run, a step forced to re-execute by a
    // redelivery, must still land ONE step-end and return the recorded value.
    let attempts = 0;
    const wf = defineWorkflow("enc_race", async (ctx, input: { card: string }) => {
      const v = await ctx.run("charge", () => {
        attempts++;
        return `receipt:${input.card}`;
      });
      return v;
    });
    const opts = { nc, workflows: [wf], prefix: ENC, payloadCodec: aesGcmPayloadCodec(KEY), lease: false }; // prettier-ignore
    const w1 = createWorker(opts);
    const w2 = createWorker(opts);
    await w1.start();
    await w2.start();
    try {
      const client = createClient({ nc, prefix: ENC, payloadCodec: aesGcmPayloadCodec(KEY) });
      const h = await client.start(wf, { card: SECRET });
      expect(await h.result(30_000)).toBe(`receipt:${SECRET}`);
      expect(attempts).toBeGreaterThanOrEqual(1);
      const a = await audit(h.runId);
      expect(a.duplicateStepEnds).toEqual([]);
      expect(a.terminals).toEqual(["result"]);
    } finally {
      await w1.stop();
      await w2.stop();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)
});

// The completeness sweep: every way a payload reaches durable storage, in ONE run, checked against
// EVERY store the deployment owns.
//
// The structural guards (test/unit/payload-paths.test.ts) make a fourth write route hard to add by
// accident. This is the behavioural counterpart, and it is deliberately broad rather than targeted:
// it does not name the places a secret could be, it enumerates the places a secret could END UP and
// insists it is in none of them. That includes the two stores no other test reads — DE_TIMERS and
// DE_PARKED — which carry no payload today and would go unnoticed for a long time if they started.
describe.skipIf(!natsUrl)("payload encryption — the completeness sweep", () => {
  let nc: NatsConnection;
  beforeAll(async () => {
    nc = await connect({ servers: natsUrl! });
  });
  afterAll(async () => {
    await nc.drain();
  });

  const SWEEP = "encsweep";
  // Every payload path at 1.5 × max_payload, some of them twice (child copies, a generation
  // crossing): the bucket must hold them all or the run parks on a full bucket.
  const SIZED = { streamMaxBytes: 512 * MIB };
  const SECRET = "pan-5555444433332222";
  let BIG = ""; // 1.5 × this server's max_payload, so it has to offload
  beforeAll(() => {
    BIG = `${SECRET}-${"b".repeat(Math.ceil(1.5 * maxPayload(nc)))}`;
  });

  /** Every message on a stream, as raw text — no filtering, no per-run guessing. */
  // Both sweeps are direct gets (wildcard filters are accepted): no consumer created, so the
  // harness's leftover check stays meaningful for the library code under test.
  async function allMessages(stream: string, filter: string): Promise<string> {
    const jsm = await jetstreamManager(nc);
    const info = await jsm.streams.info(stream);
    if (info.state.messages === 0) return "";
    if (info.config.allow_direct === true) {
      const read = await readSubject(jsm, stream, filter, 500);
      return read.out.map((m) => new TextDecoder().decode(m.data)).join("\n");
    }
    // The work and timers streams take no direct gets: an ephemeral pull consumer, DELETED here.
    const c = await jsm.consumers.add(stream, {
      filter_subject: filter,
      ack_policy: "none",
      inactive_threshold: 30_000_000_000,
    });
    try {
      const consumer = await jsm.jetstream().consumers.get(stream, c.name);
      const msgs = await consumer.fetch({ max_messages: 500, expires: 2000 });
      const out: string[] = [];
      for await (const m of msgs) out.push(new TextDecoder().decode(m.data));
      return out.join("\n");
    } finally {
      await jsm.consumers.delete(stream, c.name);
    }
  }

  /** Every LIVE value in a KV bucket (the last message per key, minus delete/purge markers). */
  async function allKv(bucket: string): Promise<string> {
    const jsm = await jetstreamManager(nc);
    const out: string[] = [];
    try {
      const it = await jsm.direct.getLastMessagesFor(`KV_${bucket}`, { multi_last: [`$KV.${bucket}.>`] }); // prettier-ignore
      for await (const m of it) {
        if (m.header.get("KV-Operation") !== "") continue; // DEL / PURGE markers carry no value
        out.push(`${m.subject}=${new TextDecoder().decode(m.data)}`);
      }
    } catch (e) {
      if (!(e instanceof Error && /not found|no results/i.test(e.message))) throw e;
    }
    return out.join("\n");
  }

  // durable-sdk: docs/findings.md F1, answered by the engine's F25 (put and get) and the SDK's
  // chunked transport (src/engine/blobs.ts).
  // prettier-ignore
  it("a run touching EVERY payload path leaves the secret in no durable store", async () => {
    const payloadCodec = aesGcmPayloadCodec(KEY);
    const kidSmall = defineWorkflow("sweep_kid_s", async (ctx, input: { card: string }) =>
      ctx.run("echo", () => input.card),
    );
    const kidBig = defineWorkflow("sweep_kid_b", async (ctx, input: { card: string }) =>
      ctx.run("expand", () => `${input.card}-${"k".repeat(Math.ceil(1.5 * maxPayload(nc)))}`),
    );
    const wf = defineWorkflow(
      "sweep_wf",
      async (ctx, input: { card: string; big: string; gen: number }) => {
        if (input.gen > 0) return `done:${input.card.slice(-4)}`; // generation 1: a result payload
        await ctx.run("small-step", () => input.card); // inline v:3
        await ctx.run("big-step", () => input.big); // v:3 + blob
        await ctx.sleep("nap", 200); // DE_TIMERS + DE_PARKED
        await ctx.await<string>("small-sig"); // inline signal
        await ctx.await<string>("big-sig"); // work-item blob
        await ctx.call(kidSmall, { card: input.card }); // child + child-done
        await ctx.call(kidBig, { card: input.card }); // child-done blob
        await ctx.continueAsNew({ ...input, gen: 1 }); // rollover input
        return "unreachable";
      },
    );

    const worker = createWorker({
      nc,
      workflows: [wf, kidSmall, kidBig],
      prefix: SWEEP,
      payloadCodec,
      ...SIZED,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: SWEEP, payloadCodec, ...SIZED });
      const h = await client.start(wf, { card: SECRET, big: BIG, gen: 0 });
      // Early signals: both are journaled when the worker reaches each await, so no state polling.
      await new Promise((r) => setTimeout(r, 300));
      await client.signal(h.runId, "small-sig", SECRET);
      await client.signal(h.runId, "big-sig", BIG);

      expect(await client.get(`${h.runId}_g1`).result(60_000)).toBe(`done:2222`);

      const n = names(SWEEP);
      const places: Array<[string, string]> = [
        ["DE_JOURNAL (every run, every generation, every child)", await allMessages(n.journalStream, n.journalSubjectAll)], // prettier-ignore
        ["DE_TIMERS", await allMessages(n.timersStream, n.timerSubjectAll)],
        ["DE_RUNS", await allKv(n.runsKv)],
        ["DE_PARKED", await allKv(n.parkedKv)],
        ["DE_LEASES", await allKv(n.leasesKv)],
      ];
      for (const [where, text] of places) {
        expect(`${where}: ${text.includes(SECRET) ? "LEAKED" : "clean"}`).toBe(`${where}: clean`);
      }
      // The blob bucket, object by object.
      const objects = await listObjects(nc, n.blobsBucket);
      expect(objects.length).toBeGreaterThanOrEqual(3); // big step, big signal, big child outcome
      for (const o of objects) {
        const bytes = await readBlob(nc, n.blobsBucket, o.name);
        expect(`${o.name}: ${new TextDecoder().decode(bytes ?? undefined).includes(SECRET) ? "LEAKED" : "clean"}`).toBe(`${o.name}: clean`); // prettier-ignore
      }

      // …and the one store that IS plaintext by design is empty, so "in flight until consumed" is
      // an observed fact rather than a claim: nothing durable is left on the work queue.
      const jsm = await jetstreamManager(nc);
      expect((await jsm.streams.info(n.workStream)).state.messages).toBe(0);
    } finally {
      await worker.stop();
    }
  }, 180_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // durable-sdk: docs/findings.md F1, answered by the engine's F25 (put and get) and the SDK's
  // chunked transport (src/engine/blobs.ts).
  // prettier-ignore
  it("CONTROL: the same run without a codec leaves the secret in journal, blobs AND the hint", async () => {
    // Proves the sweep is looking in places the secret actually reaches. Without this, a typo in a
    // subject filter would make every assertion above pass by reading nothing at all.
    const PLAINSWEEP = "plainsweep";
    const wf = defineWorkflow("psweep_wf", async (ctx, input: { card: string; big: string }) => {
      await ctx.run("small-step", () => input.card);
      await ctx.run("big-step", () => input.big);
      return input.card;
    });
    const worker = createWorker({ nc, workflows: [wf], prefix: PLAINSWEEP, ...SIZED });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: PLAINSWEEP, ...SIZED });
      const h = await client.start(wf, { card: SECRET, big: BIG });
      expect(await h.result(60_000)).toBe(SECRET);

      const n = names(PLAINSWEEP);
      expect(await allMessages(n.journalStream, n.journalSubjectAll)).toContain(SECRET);
      expect(await allKv(n.runsKv)).toContain(SECRET);
      const objects = await listObjects(nc, n.blobsBucket);
      expect(objects.length).toBeGreaterThan(0);
      const leaked = await Promise.all(
        objects.map(async (o) => new TextDecoder().decode((await readBlob(nc, n.blobsBucket, o.name)) ?? undefined)), // prettier-ignore
      );
      expect(leaked.some((b) => b.includes(SECRET))).toBe(true);
    } finally {
      await worker.stop();
    }
  }, 180_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)
});
