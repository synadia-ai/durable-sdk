// The structural guards on how a user payload reaches durable storage.
//
// A payload gets persisted by three different routes — a journal append, a work item's shrink, the
// status hint's shrink — and each one used to decide for itself whether the payload codec applied.
// One of them silently didn't, so the same value was encrypted when it fit inline and stored in the
// clear when it did not. Every test in the rest of the suite checks a BEHAVIOUR; these check that
// the shape of the bug is unavailable, so a fourth route cannot reintroduce it by omission.

import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { InMemoryBlobStore, storePayloadBlob } from "../../src/journal/blobs.js";
import { jsonCodec } from "../../src/journal/codec.js";
import { aesGcmPayloadCodec } from "../../src/journal/payload-codec.js";

const srcDir = fileURLToPath(new URL("../../src", import.meta.url));

function sources(dir: string, out: Array<{ path: string; text: string }> = []) {
  for (const e of readdirSync(dir, { withFileTypes: true })) {
    const full = join(dir, e.name);
    if (e.isDirectory()) sources(full, out);
    else if (e.name.endsWith(".ts"))
      out.push({ path: full.slice(srcDir.length + 1), text: readFileSync(full, "utf8") });
  }
  return out;
}
const files = sources(srcDir);

describe("one way in: the payload-blob choke point", () => {
  it("only journal/blobs.ts writes a payload blob — nothing else reaches the store directly", () => {
    // `blob-store.ts` is the NATS adapter that implements the seam, so it is where `putBlob`'s
    // object-store call lives; every payload-shaped write must go through `storePayloadBlob`, which
    // is the only function that can apply the codec. A module that called `.put()` itself would be
    // a fourth route, invisible to every behavioural test until somebody read a bucket.
    const allowed = new Set(["journal/blobs.ts", "nats/blob-store.ts"]);
    const offenders = files.filter(
      (f) => !allowed.has(f.path) && /\bblobs\s*\.\s*put\s*\(/.test(f.text),
    );
    expect(offenders.map((f) => f.path)).toEqual([]);
  });

  it("storePayloadBlob is defined exactly once", () => {
    const definitions = files.filter((f) =>
      /export async function storePayloadBlob\b/.test(f.text),
    );
    expect(definitions.map((f) => f.path)).toEqual(["journal/blobs.ts"]);
  });

  it("every caller outside the choke point is a KNOWN payload route", () => {
    // Not a restriction — a roster. If a fourth route appears, this fails and whoever added it has
    // to come and say what it is, which is the conversation that did not happen last time.
    const callers = files
      .filter((f) => f.path !== "journal/blobs.ts" && /storePayloadBlob\s*\(/.test(f.text))
      .map((f) => f.path)
      .sort();
    // durable-sdk: `nats/runs.ts` (the DE_RUNS status hint) is the engine's now and not in src/.
    expect(callers).toEqual([
      "nats/protocol.ts", // a work item's signal data / child outcome
    ]);
  });
});

describe("the codec decision cannot be reached by omission", () => {
  const blobs = new InMemoryBlobStore();

  it("omitting the payload codec is a COMPILE error, not a silent plaintext write", () => {
    // The guard itself: `@ts-expect-error` fails `npm run typecheck` if this call ever becomes
    // legal again — i.e. if somebody makes the parameter optional. That is precisely the diff that
    // let an oversized signal be offloaded in the clear, and it type-checked cleanly at the time.
    // @ts-expect-error - payloadCodec is required; `undefined` must be written down deliberately.
    void storePayloadBlob("r", "a payload", { a: 1 }, jsonCodec, blobs);
    expect(true).toBe(true);
  });

  // The second half of the same bug was a marker kept in PARALLEL with the write: the entry said
  // v:2 while the blob held ciphertext. `transformed` comes back from the write itself, so there is
  // only one source for it and the bytes and the marker cannot disagree.
  it("transformed is false without a codec and true with one — from the write, not the caller", async () => {
    const plain = await storePayloadBlob("r", "x", { a: 1 }, jsonCodec, blobs, undefined);
    expect(plain.transformed).toBe(false);
    const enc = await storePayloadBlob(
      "r",
      "x",
      { a: 1 },
      jsonCodec,
      blobs,
      aesGcmPayloadCodec(new Uint8Array(32).fill(7)),
    );
    expect(enc.transformed).toBe(true);
    // …and the two land under DIFFERENT content-addressed names, because one is ciphertext.
    expect(enc.ref.name).not.toBe(plain.ref.name);
  });

  it("the plaintext write really is plaintext and the transformed one really is not", async () => {
    const store = new InMemoryBlobStore();
    const secret = { card: "4242424242424242" };
    const plain = await storePayloadBlob("r", "x", secret, jsonCodec, store, undefined);
    const enc = await storePayloadBlob(
      "r",
      "x",
      secret,
      jsonCodec,
      store,
      aesGcmPayloadCodec(new Uint8Array(32).fill(7)),
    );
    const dec = new TextDecoder();
    expect(dec.decode(await store.get(plain.ref.name))).toContain("4242424242424242");
    expect(dec.decode(await store.get(enc.ref.name))).not.toContain("4242424242424242");
  });
});
