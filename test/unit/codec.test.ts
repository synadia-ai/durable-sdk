import { describe, expect, it } from "vitest";
import { jsonCodec, VALID_KINDS } from "../../src/journal/codec.js";
import { type JournalEntry, NON_COMMAND_SEQ, stamp } from "../../src/journal/entry.js";

// Every shape the wire format (v:1) can carry. Kept in lockstep with VALID_KINDS via the completeness
// assertion below — a new EntryKind that lacks a round-trip case fails the suite loudly.
const SHAPES: JournalEntry[] = [
  stamp({ kind: "start", seq: 0, wf: "w", wfVersion: 1, input: { a: 1 } }, 100),
  // start carrying the optional sub-workflow + continue-as-new chain fields:
  stamp(
    { kind: "start", seq: 0, wf: "w", wfVersion: 1, input: { a: 1 }, parent: { runId: "p", seq: 3, wf: "pw" }, generation: 2, base: "p" }, // prettier-ignore
    100,
  ),
  stamp({ kind: "step-begin", seq: 1, name: "s", key: "r:1" }, 100),
  stamp({ kind: "step-end", seq: 1, name: "s", value: { ok: true } }, 100),
  stamp({ kind: "now", seq: 2, value: 12345 }, 100),
  stamp({ kind: "random", seq: 3, value: 0.5 }, 100),
  stamp({ kind: "uuid", seq: 4, value: "abc-123" }, 100),
  stamp({ kind: "sleep", seq: 5, name: "nap", dueMs: 5000 }, 100),
  stamp({ kind: "sleep", seq: 5, name: "nap", dueMs: 5000, schedSubj: "de.timer.sched.r.5" }, 100), // with schedule subj
  stamp({ kind: "await", seq: 6, name: "go" }, 100), // no timeout
  stamp({ kind: "await", seq: 7, name: "go", dueMs: 6000 }, 100), // with timeout
  stamp({ kind: "signal", seq: -1, name: "go", data: { ok: true }, signalId: "s1" }, 100),
  stamp(
    { kind: "call", seq: 8, name: "child", childRunId: "r_8", wf: "child", input: { x: 1 } },
    100,
  ),
  stamp({ kind: "resolve", seq: -1, resolvesSeq: 8, value: 42 }, 100), // child/timer success
  stamp({ kind: "resolve", seq: -1, resolvesSeq: 9, error: { name: "E", message: "m" } }, 100), // child failure
  stamp({ kind: "resolve", seq: -1, resolvesSeq: 5 }, 100), // sleep/timer wake (no value, no error)
  stamp({ kind: "result", seq: -1, value: { done: true } }, 100),
  stamp({ kind: "fail", seq: -1, error: { name: "E", message: "m" } }, 100),
  stamp(
    { kind: "continued", seq: -1, nextRunId: "r_g1", input: { n: 2 }, generation: 1, base: "r" },
    100,
  ), // no next-gen version (pre-versioning / same-version) // prettier-ignore
  stamp(
    {
      kind: "continued",
      seq: -1,
      nextRunId: "r_g1",
      input: { n: 2 },
      generation: 1,
      base: "r",
      version: 2,
    },
    100,
  ), // migrating next gen onto v2 // prettier-ignore
  stamp({ kind: "cancel-requested", seq: NON_COMMAND_SEQ }, 100), // no reason
  stamp({ kind: "cancel-requested", seq: NON_COMMAND_SEQ, reason: "user bailed" }, 100), // with reason
  stamp({ kind: "cancelled", seq: NON_COMMAND_SEQ }, 100), // no reason
  stamp({ kind: "cancelled", seq: NON_COMMAND_SEQ, reason: "user bailed" }, 100), // with reason
];

describe("journal codec", () => {
  it("round-trips entries of every shape", () => {
    for (const e of SHAPES) {
      expect(jsonCodec.decode(jsonCodec.encode(e))).toEqual(e);
    }
  });

  it("covers EVERY decodable kind (no wire kind escapes a round-trip case)", () => {
    // The codec's decode allow-list is the source of truth; assert SHAPES exercises all of it. A new
    // EntryKind added to VALID_KINDS without a round-trip case here trips this immediately.
    expect(new Set(SHAPES.map((e) => e.kind))).toEqual(VALID_KINDS);
  });

  it("drops an undefined value on the wire; it decodes as absent (the engine reads absent as undefined)", () => {
    // JSON.stringify omits an undefined property, so a step/result carrying `undefined` is not stored as
    // an explicit null — it is simply absent. The replay engine returns the absent field as undefined,
    // so a step or workflow that returns undefined replays identically (proven in replay.test.ts).
    const stepEnd = stamp({ kind: "step-end", seq: 1, name: "s", value: undefined }, 100);
    const decoded = jsonCodec.decode(jsonCodec.encode(stepEnd)) as { value?: unknown };
    expect("value" in decoded).toBe(false);
    expect(decoded.value).toBeUndefined();
  });

  it("rejects an unsupported wire version", () => {
    const bad = new TextEncoder().encode(JSON.stringify({ v: 999, seq: 0, kind: "start" }));
    expect(() => jsonCodec.decode(bad)).toThrow(/wire version/);
  });

  it("rejects an invalid kind", () => {
    const bad = new TextEncoder().encode(JSON.stringify({ v: 1, seq: 0, kind: "bogus" }));
    expect(() => jsonCodec.decode(bad)).toThrow(/invalid kind/);
  });

  it("encode throws on a value JSON cannot serialize (e.g. a BigInt)", () => {
    // The root of audit finding A2: a non-serializable step result makes encode throw. The NATS shell
    // wraps this as a SerializationError so the core fails the run instead of retrying forever.
    const entry = stamp({ kind: "step-end", seq: 1, name: "s", value: BigInt(10) }, 100);
    expect(() => jsonCodec.encode(entry)).toThrow();
  });

  it("encode REJECTS a non-finite number (NaN/Infinity), nested too — symmetric with BigInt", () => {
    // JSON.stringify would silently coerce NaN/Infinity to null (first-run vs replay divergence). The
    // codec rejects them so the NATS shell wraps it as a SerializationError and the run fails loud.
    for (const bad of [NaN, Infinity, -Infinity]) {
      expect(() => jsonCodec.encode(stamp({ kind: "step-end", seq: 1, name: "s", value: bad }, 1))) //
        .toThrow(/non-finite/);
    }
    // nested inside an object/array is caught too (the replacer visits every value):
    const nested = stamp({ kind: "result", seq: -1, value: { items: [1, NaN, 3] } }, 1);
    expect(() => jsonCodec.encode(nested)).toThrow(/non-finite/);
  });
});
