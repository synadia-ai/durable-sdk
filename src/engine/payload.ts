// @synadia-ai/durable — the two payload forms of durable-engine docs/API.md "Payloads". Without a
// payload codec every value travels inline (`input`, `value`, `data`, `error`). With one, the
// engine never sees a plaintext payload:
//   - an appended entry carries its slot exactly as the library's journal writes it: `encodePayload`
//     of journal/blobs.ts applied after `stamp` (slot removed, `<slot>Enc` appended after `v` and
//     `ts`, `v:3`);
//   - a start's or a child's input and a signal's data travel as `inputEnc` / `dataEnc`, the
//     base64 of the codec's output over the value's JSON bytes, the same bytes `encodePayload`
//     would put in the entry the engine builds from them;
//   - every journal the engine hands out is reversed slot by slot (`resolveEntry`) before replay.
// Offload is the engine's for every value that fits a request. One that does not goes up in `put`
// parts (blobs.ts) and its request carries the ref: `fitEntry` and `fitSlot` below. A ref the
// engine hands back, for a value too large for a reply, is fetched with `get` by `resolveEntry`
// with the engine as its blob store.

import { PayloadCodecError, SerializationError } from "../errors.js";
import {
  type BlobBytesCache,
  type BlobStore,
  encodePayload,
  offloadEntry,
  resolveEntry,
  storePayloadBlob,
} from "../journal/blobs.js";
import { jsonCodec } from "../journal/codec.js";
import { type BlobRef, type JournalEntry, type LoadedEntry } from "../journal/entry.js";
import { fromBase64, type PayloadCodec, toBase64 } from "../journal/payload-codec.js";
import { bodyBytes, type Sizes } from "./blobs.js";

/** A journal as the engine answers it: `[{streamSeq, entry}]`. */
export type WireJournal = Array<{ streamSeq: number; entry: JournalEntry }>;

/** The entry an `append` carries: the stamped entry, its slot transformed under a codec. */
export async function wireEntry(
  runId: string,
  entry: JournalEntry,
  payloadCodec: PayloadCodec | undefined,
): Promise<JournalEntry> {
  const out = await encodePayload(runId, entry, jsonCodec, payloadCodec);
  try {
    // The library's codec: rejects a non-finite number anywhere, as the library's journal does,
    // so the first execution and a replay can never read different values.
    jsonCodec.encode(out);
  } catch (e) {
    throw new SerializationError(
      runId,
      `run ${runId}: cannot encode ${entry.kind} entry (seq ${entry.seq}): ${String(e)}`,
      { cause: e },
    );
  }
  return out;
}

/**
 * The entry an `append` carries, fitted to `request_max`: when the request (`envelope` with the
 * entry) would pass it, the entry's slot goes up in parts first and the entry carries its ref in
 * the library's stored form (`offloadEntry`: slot removed, `<slot>Ref`, `v:2`, or `v:3` over codec
 * output). An entry whose metadata alone is too large is left as it is, for the engine to refuse.
 * `sizes` undefined is `offload: false`: nothing goes up, and a value too large fails loud.
 */
export async function fitEntry(
  runId: string,
  entry: JournalEntry,
  envelope: Record<string, unknown>,
  sizes: (() => Promise<Sizes>) | undefined,
  blobs: BlobStore,
): Promise<JournalEntry> {
  if (sizes === undefined) return entry;
  const { requestMax } = await sizes();
  if (bodyBytes({ ...envelope, entry }) <= requestMax) return entry;
  const out = await offloadEntry(runId, entry, jsonCodec.encode(entry), jsonCodec, blobs, 0);
  return out.entry;
}

/**
 * A value for a request slot, fitted to `request_max`: `slot()`'s form when the request (`rest`
 * with it) fits; else the value is `put` under `runId` and the slot is `{[name + "Ref"]: ref}`,
 * with `payloadEnc: true` when the blob holds codec output.
 */
export async function fitSlot(
  name: "input" | "data",
  value: unknown,
  payloadCodec: PayloadCodec | undefined,
  runId: string,
  rest: Record<string, unknown>,
  sizes: (() => Promise<Sizes>) | undefined,
  blobs: BlobStore,
): Promise<Record<string, unknown>> {
  const inline = await slot(name, value, payloadCodec, runId);
  if (sizes === undefined) return inline;
  const { requestMax } = await sizes();
  if (bodyBytes({ ...rest, ...inline }) <= requestMax) return inline;
  const { ref, transformed } = await storePayloadBlob(runId, `the ${name}`, value, jsonCodec, blobs, payloadCodec); // prettier-ignore
  return { [`${name}Ref`]: ref, ...(transformed ? { payloadEnc: true } : {}) };
}

/**
 * Reverse every `<slot>Enc` of a journal the engine handed out, in order, and fetch every ref it
 * handed in its stored form (a value too large for a reply) from `blobs`.
 */
export async function reverseJournal(
  runId: string,
  journal: WireJournal,
  payloadCodec: PayloadCodec | undefined,
  blobs?: BlobStore,
  cache?: BlobBytesCache,
): Promise<LoadedEntry[]> {
  const out: LoadedEntry[] = [];
  for (const l of journal) {
    const entry = await resolveEntry(runId, l.entry, jsonCodec, blobs, cache, payloadCodec);
    out.push({ entry, streamSeq: l.streamSeq });
  }
  return out;
}

/**
 * A `result` reply's `valueRef` / `errorRef`: fetched, its digest checked, reversed through the
 * codec when `payloadEnc`, as `resolveEntry` resolves the same ref on a terminal entry.
 */
export async function unref(
  runId: string,
  slotName: "value" | "error",
  ref: BlobRef,
  payloadEnc: boolean,
  payloadCodec: PayloadCodec | undefined,
  blobs: BlobStore,
): Promise<unknown> {
  const entry = {
    kind: slotName === "value" ? "result" : "fail",
    seq: -1,
    v: payloadEnc ? 3 : 2,
    [`${slotName}Ref`]: ref,
  } as unknown as JournalEntry;
  const r = await resolveEntry(runId, entry, jsonCodec, blobs, undefined, payloadCodec);
  return (r as unknown as Record<string, unknown>)[slotName];
}

/**
 * A value for a request slot: `{[name]: value}` plain, `{[name + "Enc"]: base64}` under a codec.
 * `undefined` sends nothing (the engine reads an absent input as `null`, as the library's journal
 * drops an undefined member).
 */
export async function slot(
  name: "input" | "data",
  value: unknown,
  payloadCodec: PayloadCodec | undefined,
  runId = "",
): Promise<Record<string, unknown>> {
  if (value === undefined) return {};
  let plain: Uint8Array;
  try {
    plain = jsonCodec.encodeValue(value);
  } catch (e) {
    throw new SerializationError(runId, `run ${runId}: cannot encode the ${name}: ${String(e)}`, {
      cause: e,
    });
  }
  if (payloadCodec === undefined) return { [name]: value };
  let out: Uint8Array;
  try {
    out = await payloadCodec.encode(plain);
  } catch (e) {
    throw new PayloadCodecError("the configured payloadCodec failed to encode this payload", { runId }, { cause: e }); // prettier-ignore
  }
  return { [`${name}Enc`]: toBase64(out) };
}

/** Decode a `valueEnc` / `errorEnc` of a `result` reply. */
export async function unslot(
  runId: string,
  encoded: string,
  payloadCodec: PayloadCodec | undefined,
): Promise<unknown> {
  if (payloadCodec === undefined) {
    throw new PayloadCodecError(
      "the result is encoded (v:3) and this client has no payloadCodec — configure the deployment's codec",
      { runId },
    );
  }
  let bytes: Uint8Array;
  try {
    bytes = fromBase64(encoded);
  } catch (e) {
    throw new PayloadCodecError("malformed encoded result (expected base64)", { runId }, { cause: e }); // prettier-ignore
  }
  let plain: Uint8Array;
  try {
    plain = await payloadCodec.decode(bytes);
  } catch (e) {
    throw new PayloadCodecError("the payloadCodec could not decode the result (wrong key, or altered bytes)", { runId }, { cause: e }); // prettier-ignore
  }
  return jsonCodec.decodeValue(plain);
}
