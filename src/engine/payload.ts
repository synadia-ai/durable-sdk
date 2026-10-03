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
// Offload is the engine's: no blob store is reachable from here, and a ref would fail loud.

import { PayloadCodecError, SerializationError } from "../errors.js";
import { type BlobStore, encodePayload, resolveEntry } from "../journal/blobs.js";
import { jsonCodec } from "../journal/codec.js";
import { type JournalEntry, type LoadedEntry } from "../journal/entry.js";
import { fromBase64, type PayloadCodec, toBase64 } from "../journal/payload-codec.js";

/** A journal as the engine answers it: `[{streamSeq, entry}]`. */
export type WireJournal = Array<{ streamSeq: number; entry: JournalEntry }>;

/** No blob store, by construction: the engine inflates every ref before it answers. */
const NO_BLOBS: BlobStore | undefined = undefined;

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

/** Reverse every `<slot>Enc` of a journal the engine handed out, in order. */
export async function reverseJournal(
  runId: string,
  journal: WireJournal,
  payloadCodec: PayloadCodec | undefined,
): Promise<LoadedEntry[]> {
  const out: LoadedEntry[] = [];
  for (const l of journal) {
    const entry = await resolveEntry(runId, l.entry, jsonCodec, NO_BLOBS, undefined, payloadCodec);
    out.push({ entry, streamSeq: l.streamSeq });
  }
  return out;
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
