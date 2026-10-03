// Cross-SDK conformance, from this side: the vectors in conformance/vectors/ are generated from THIS
// codec (conformance/generate-vectors.ts), and the Python SDK asserts it reproduces them byte for
// byte. That only means anything if this side is pinned too — otherwise a change here would silently
// redefine the golden and the "agreement" would be circular.
//
// A failure here is not a broken test: it is the wire format changing. That is a breaking change for
// every other implementation (regenerate the vectors, and say so in PROTOCOL.md).

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { VALID_KINDS, jsonCodec } from "../../src/journal/codec.js";
import { type JournalEntry } from "../../src/journal/entry.js";

interface Vector {
  name: string;
  note?: string;
  entry: JournalEntry;
  encoded: string;
}

const vectorsPath = fileURLToPath(
  new URL("../../conformance/vectors/journal-entries.json", import.meta.url),
);
const doc = JSON.parse(readFileSync(vectorsPath, "utf8")) as { vectors: Vector[] };
const vectors = doc.vectors;

describe("cross-SDK wire vectors", () => {
  it("the vector file is present and populated", () => {
    expect(vectors.length).toBeGreaterThanOrEqual(20);
  });

  it.each(vectors.map((v) => [v.name, v] as const))("%s encodes to the golden bytes", (_n, v) => {
    expect(new TextDecoder().decode(jsonCodec.encode(v.entry))).toBe(v.encoded);
  });

  it.each(vectors.map((v) => [v.name, v] as const))("%s decodes from the golden bytes", (_n, v) => {
    expect(jsonCodec.decode(new TextEncoder().encode(v.encoded))).toEqual(v.entry);
  });

  it("the vectors cover every wire kind", () => {
    const covered = new Set(vectors.map((v) => v.entry.kind));
    expect([...covered].sort()).toEqual([...VALID_KINDS].sort());
  });

  it("the vectors include the offloaded v:2 form", () => {
    const offloaded = vectors.filter((v) => v.entry.v === 2);
    expect(offloaded.length).toBeGreaterThan(0);
    for (const v of offloaded) {
      expect(Object.keys(v.entry).some((k) => k.endsWith("Ref"))).toBe(true);
    }
  });

  // The stored forms are where a decoder goes subtly wrong, and where the two SDKs drifted for real:
  // Python once stamped a work-item blob ref v:1 where TypeScript wrote v:2. Nothing in this corpus
  // could have caught it, because only ONE stored-form vector existed and it was hand-written — with
  // the ref placed before `v`/`ts`, a key order neither engine emits. The vectors are generated from
  // `encodePayload`/`offloadEntry` now, so these assertions are about coverage of that corpus.
  const fieldsOf = (v: Vector): string[] => Object.keys(v.entry);

  it("every payload SLOT is pinned in its offloaded form, not just one of them", () => {
    const refs = new Set(vectors.flatMap(fieldsOf).filter((k) => k.endsWith("Ref")));
    expect([...refs].sort()).toEqual(["dataRef", "errorRef", "inputRef", "valueRef"]);
  });

  it("every payload SLOT is pinned in its transformed form too", () => {
    const encs = new Set(vectors.flatMap(fieldsOf).filter((k) => k.endsWith("Enc")));
    expect([...encs].sort()).toEqual(["dataEnc", "errorEnc", "inputEnc", "valueEnc"]);
  });

  it("the v:3-WITH-a-ref combination is pinned — the shape a large encrypted payload takes", () => {
    // Transformed AND offloaded: the blob holds codec output, so a reader must untransform after
    // fetching. Getting the `v` wrong here is the difference between a value and ciphertext, and it
    // is exactly what an oversized signal or child outcome produces (PROTOCOL §5.7, §6.1).
    const both = vectors.filter(
      (v) => v.entry.v === 3 && fieldsOf(v).some((k) => k.endsWith("Ref")),
    );
    expect(both.length).toBeGreaterThanOrEqual(2);
    for (const v of both) expect(fieldsOf(v).some((k) => k.endsWith("Enc"))).toBe(false);
  });

  it("a stored form NEVER carries the plaintext slot it stands in for", () => {
    // The silent-corruption shape: an entry with both would let a reader pick the wrong one, and a
    // reader that preferred the plaintext would quietly bypass encryption entirely.
    const slots = ["input", "value", "data", "error"] as const;
    for (const v of vectors) {
      const keys = new Set(fieldsOf(v));
      for (const slot of slots) {
        const stand = [`${slot}Ref`, `${slot}Enc`].filter((k) => keys.has(k));
        expect(stand.length).toBeLessThanOrEqual(1); // never both a ref AND an enc
        if (stand.length === 1) expect(keys.has(slot)).toBe(false);
      }
    }
  });

  it("`v` matches the form the entry is actually in", () => {
    for (const v of vectors) {
      const keys = fieldsOf(v);
      const transformed = keys.some((k) => k.endsWith("Enc"));
      const refd = keys.some((k) => k.endsWith("Ref"));
      if (transformed) expect(v.entry.v).toBe(3);
      else if (refd)
        expect(v.entry.v).toBeGreaterThanOrEqual(2); // v:2, or v:3 when the blob is codec output
      else expect(v.entry.v).toBe(1);
    }
  });
});
