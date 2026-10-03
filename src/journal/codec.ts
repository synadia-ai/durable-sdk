// @synadia-ai/durable — journal codec (pluggable; JSON default). Pure, transport-free.

import { WIRE_VERSION, WIRE_VERSION_BLOBS, WIRE_VERSION_ENCODED } from "../version.js";
import { type EntryKind, type JournalEntry } from "./entry.js";

export interface Codec {
  encode(entry: JournalEntry): Uint8Array;
  decode(data: Uint8Array): JournalEntry;
  /**
   * Encode/decode a single VALUE (one payload slot) for blob offload. MUST apply the same fidelity
   * rules as `encode` (a value that would corrupt the entry encoding must fail here too), so an
   * offloaded value round-trips exactly as it would have inline.
   */
  encodeValue(value: unknown): Uint8Array;
  decodeValue(data: Uint8Array): unknown;
}

/** The canonical set of decodable entry kinds. Exported so the codec test can assert that every kind
 *  the wire format accepts is proven to round-trip (a new kind can't slip in untested). */
export const VALID_KINDS = new Set<EntryKind>([
  "start",
  "step-begin",
  "step-end",
  "now",
  "random",
  "uuid",
  "sleep",
  "await",
  "signal",
  "call",
  "resolve",
  "result",
  "fail",
  "continued",
  "cancel-requested",
  "cancelled",
]);

const enc = new TextEncoder();
const dec = new TextDecoder();

// Reject non-finite numbers (NaN, ±Infinity) ANYWHERE in the value. JSON.stringify would silently
// coerce them to null, so the FIRST execution would return NaN but a REPLAY would read null — a
// value-level (and potentially control-flow) divergence. Failing loud here surfaces as a
// SerializationError at the journal boundary, terminally failing the run — symmetric with how a
// BigInt already makes JSON.stringify throw. (A custom Codec may choose to represent them.)
const rejectNonFinite = (_k: string, v: unknown): unknown => {
  if (typeof v === "number" && !Number.isFinite(v)) {
    throw new TypeError(`cannot journal a non-finite number (${String(v)})`);
  }
  return v;
};

export const jsonCodec: Codec = {
  encode(entry: JournalEntry): Uint8Array {
    return enc.encode(JSON.stringify(entry, rejectNonFinite));
  },
  decode(data: Uint8Array): JournalEntry {
    const obj: unknown = JSON.parse(dec.decode(data));
    if (typeof obj !== "object" || obj === null) {
      throw new Error("journal decode: not an object");
    }
    const e = obj as Partial<JournalEntry>;
    // v:1 is the base layout; v:2 is the same layout with a payload slot offloaded to a blob ref;
    // v:3 is the same layout with the payload slot put through a PayloadCodec (`*Enc` inline, or a
    // `*Ref` whose blob holds transformed bytes). Each is a distinct version so an older reader
    // fails loud instead of replaying a stripped or transformed slot as if it were the value.
    //
    // Structural decode accepts v:3 unconditionally — whether a payloadCodec is actually configured
    // is enforced one layer up, where the slot is restored (blobs.ts), so the failure names the real
    // problem ("no payloadCodec configured") instead of "unsupported version".
    if (e.v !== WIRE_VERSION && e.v !== WIRE_VERSION_BLOBS && e.v !== WIRE_VERSION_ENCODED) {
      throw new Error(
        `journal decode: unsupported wire version ${String(e.v)} (expected ${WIRE_VERSION}, ${WIRE_VERSION_BLOBS} or ${WIRE_VERSION_ENCODED})`,
      );
    }
    if (typeof e.kind !== "string" || !VALID_KINDS.has(e.kind)) {
      throw new Error(`journal decode: invalid kind ${String(e.kind)}`);
    }
    if (typeof e.seq !== "number") {
      throw new Error("journal decode: missing seq");
    }
    return obj as JournalEntry;
  },
  encodeValue(value: unknown): Uint8Array {
    const s = JSON.stringify(value, rejectNonFinite);
    // JSON.stringify(undefined) returns undefined (not a string); offload never encodes an absent
    // slot, so reaching this is a programming error — fail loud rather than store the 9-byte string
    // "undefined" that JSON.parse would then reject.
    if (s === undefined) {
      throw new TypeError("cannot encode undefined as a blob value");
    }
    return enc.encode(s);
  },
  decodeValue(data: Uint8Array): unknown {
    return JSON.parse(dec.decode(data)) as unknown;
  },
};
