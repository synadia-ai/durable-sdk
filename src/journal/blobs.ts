// @synadia-ai/durable — large-payload offload (pure, transport-free). See PROTOCOL "large-payload
// offload" and DESIGN. When an encoded journal entry exceeds the offload threshold, its single
// payload slot (`input` / `value` / `data` / `error`) is written to a blob object store FIRST and
// the entry carries a content-addressed {@link BlobRef} instead, stamped v:2. Loading resolves refs
// back to inline values before entries reach the core — the replay engine never sees a ref.
//
// Why content-addressed names ({runId}/{digest}), not msgID-derived: step functions may legitimately
// be nondeterministic, so two CAS racers at the same seq can hold DIFFERENT bytes. With a
// deterministic per-seq name the loser's put could overwrite the winner's blob AFTER the winner's
// ref landed — silent corruption. Content-addressing makes that structurally impossible: divergent
// racers write different objects and the journal CAS picks the surviving ref; identical racers
// share one object. Corollary: a fenced writer MUST NOT delete its blob (the winner may reference
// the very same object). Orphans are inert and are GC'd with the run (docs/OPERATIONS.md).

import {
  BlobIntegrityError,
  BlobMissingError,
  PayloadCodecError,
  SerializationError,
} from "../errors.js";
import { WIRE_VERSION_BLOBS, WIRE_VERSION_ENCODED } from "../version.js";
import { type Codec } from "./codec.js";
import { type BlobRef, type JournalEntry } from "./entry.js";
import { fromBase64, type PayloadCodec, toBase64 } from "./payload-codec.js";

/**
 * The blob store seam: `put` overwrites (content-addressed names make a same-name overwrite carry
 * identical bytes), `get` returns null for a missing object and THROWS on transport failure (the
 * caller treats a throw as transient infra — retryable — and null as a definitive absence).
 */
export interface BlobStore {
  put(name: string, data: Uint8Array): Promise<void>;
  get(name: string): Promise<Uint8Array | null>;
}

/**
 * In-memory BlobStore double (unit tests; mirrors the NATS object-store shell). Stores copies, so
 * neither writer-side mutation nor reader-side mutation can alias the stored bytes.
 */
export class InMemoryBlobStore implements BlobStore {
  private readonly objects = new Map<string, Uint8Array>();

  put(name: string, data: Uint8Array): Promise<void> {
    this.objects.set(name, data.slice());
    return Promise.resolve();
  }
  get(name: string): Promise<Uint8Array | null> {
    const d = this.objects.get(name);
    return Promise.resolve(d ? d.slice() : null);
  }
  /** Test/GC helper (mirrors deleting the object out-of-band). */
  delete(name: string): void {
    this.objects.delete(name);
  }
  names(): string[] {
    return [...this.objects.keys()];
  }
}

/**
 * Safety margin subtracted from the server `max_payload` to form the default offload threshold:
 * an entry at exactly the threshold must still fit after publish headers (msgID,
 * expected-last-subject-sequence) are added. Generous on purpose — the cost of offloading a
 * borderline entry is negligible; the cost of a too-tight margin is a PayloadTooLargeError.
 */
export const OFFLOAD_MARGIN_BYTES = 16 * 1024;

/** Floor for a configured threshold: below this, every entry would offload (metadata alone is
 *  bigger) and the journal would degenerate into pure pointers. */
export const MIN_OFFLOAD_THRESHOLD_BYTES = 1024;

/** Clamp a configured/derived threshold into its sane range. */
export function effectiveThreshold(
  thresholdBytes: number | undefined,
  maxPayload: number | undefined,
): number | undefined {
  const auto = maxPayload !== undefined ? maxPayload - OFFLOAD_MARGIN_BYTES : undefined;
  const chosen =
    thresholdBytes !== undefined ? Math.min(thresholdBytes, auto ?? thresholdBytes) : auto;
  if (chosen === undefined) return undefined;
  return Math.max(MIN_OFFLOAD_THRESHOLD_BYTES, chosen);
}

/**
 * Large-payload offload configuration (createWorker/createClient `offload` option). Default ON:
 * `false` disables (an oversized payload then fails the run loud with PayloadTooLargeError, as
 * before offload existed), an object tunes it.
 */
export type OffloadOptions =
  | boolean
  | {
      /**
       * Encoded-entry size (bytes) above which the payload slot is offloaded. Default: the
       * connection's `max_payload` − 16 KiB — i.e. offload ONLY what could not have been published
       * inline. Lower it (e.g. 256 KiB) to keep journal entries lean at the cost of an extra
       * object-store round-trip per large value. Clamped to ≥ 1 KiB and to fit `max_payload`.
       */
      thresholdBytes?: number;
      /**
       * Byte budget for the per-instance resolved-blob cache (content-addressed, so never stale;
       * bytes are decoded fresh per replay). Default 64 MiB; 0 disables caching.
       */
      cacheBytes?: number;
      /** `DE_BLOBS` bucket max_bytes at provision time (explicit values reconcile). */
      maxBytes?: number;
    };

export const DEFAULT_BLOB_CACHE_BYTES = 64 * 1024 * 1024;

export interface ResolvedOffloadOptions {
  enabled: boolean;
  thresholdBytes?: number;
  cacheBytes: number;
  maxBytes?: number;
}

export function resolveOffloadOptions(o: OffloadOptions | undefined): ResolvedOffloadOptions {
  if (o === false) return { enabled: false, cacheBytes: 0 };
  const c = typeof o === "object" ? o : {};
  return {
    enabled: true,
    cacheBytes: c.cacheBytes ?? DEFAULT_BLOB_CACHE_BYTES,
    ...(c.thresholdBytes !== undefined ? { thresholdBytes: c.thresholdBytes } : {}),
    ...(c.maxBytes !== undefined ? { maxBytes: c.maxBytes } : {}),
  };
}

type PayloadSlot = "input" | "value" | "data" | "error";

/**
 * The single offloadable payload slot of an entry kind (undefined = the kind carries no payload —
 * an oversized one is pure metadata and cannot be helped by offload). `resolve` holds value XOR
 * error; the slot is picked from the populated one.
 */
export function payloadSlot(entry: JournalEntry): PayloadSlot | undefined {
  switch (entry.kind) {
    case "start":
    case "call":
    case "continued":
      return "input";
    case "step-end":
    case "result":
      return "value";
    case "signal":
      return "data";
    case "fail":
      return "error";
    case "resolve":
      return entry.error !== undefined ? "error" : "value";
    default:
      return undefined;
  }
}

const REF_FIELD_OF: Record<PayloadSlot, "inputRef" | "valueRef" | "dataRef" | "errorRef"> = {
  input: "inputRef",
  value: "valueRef",
  data: "dataRef",
  error: "errorRef",
};

const ALL_REF_FIELDS = ["inputRef", "valueRef", "dataRef", "errorRef"] as const;

const ENC_FIELD_OF: Record<PayloadSlot, "inputEnc" | "valueEnc" | "dataEnc" | "errorEnc"> = {
  input: "inputEnc",
  value: "valueEnc",
  data: "dataEnc",
  error: "errorEnc",
};

const ALL_ENC_FIELDS = ["inputEnc", "valueEnc", "dataEnc", "errorEnc"] as const;

/** The inline transformed slot an entry carries, if any (v:3 without a ref). */
function encSlotOf(entry: JournalEntry): { slot: PayloadSlot; encoded: unknown } | undefined {
  const rec = entry as unknown as Record<string, unknown>;
  for (const field of ALL_ENC_FIELDS) {
    if (rec[field] !== undefined) {
      return { slot: field.slice(0, -3) as PayloadSlot, encoded: rec[field] };
    }
  }
  return undefined;
}

/**
 * A v:3 entry whose payload slot is neither `*Enc` nor `*Ref` is one we cannot interpret. That is
 * only reachable through a newer writer or a hand-edited stream, and reading it as plaintext would
 * be the silent corruption the version bump exists to prevent — so fail loud (the worker parks).
 */
function assertNotEncodedWithoutCodec(
  runId: string,
  entry: JournalEntry,
  payloadCodec: PayloadCodec | undefined,
): void {
  if (entry.v < WIRE_VERSION_ENCODED) return;
  throw new PayloadCodecError(
    payloadCodec === undefined
      ? "the journal carries payload-codec output (v:3) but this reader has no payloadCodec configured"
      : "a v:3 entry carries neither an inline nor an offloaded payload slot",
    { runId, seq: entry.seq },
  );
}

function fromBase64Checked(runId: string, seq: number, s: string): Uint8Array {
  try {
    return fromBase64(s);
  } catch (e) {
    throw new PayloadCodecError("payload slot is not valid base64", { runId, seq }, { cause: e });
  }
}

/** Run the codec's reverse direction, mapping "no codec" and "codec threw" onto one parkable error. */
async function untransform(
  runId: string,
  seq: number,
  bytes: Uint8Array,
  payloadCodec: PayloadCodec | undefined,
): Promise<Uint8Array> {
  if (payloadCodec === undefined) {
    throw new PayloadCodecError(
      "the journal carries payload-codec output (v:3) but this reader has no payloadCodec configured",
      { runId, seq },
    );
  }
  try {
    return await payloadCodec.decode(bytes);
  } catch (e) {
    throw new PayloadCodecError(
      "the configured payloadCodec could not decode this payload (wrong key, or the bytes were altered)",
      { runId, seq },
      { cause: e },
    );
  }
}

function decodeValueChecked(runId: string, seq: number, bytes: Uint8Array, codec: Codec): unknown {
  try {
    return codec.decodeValue(bytes);
  } catch (e) {
    throw new PayloadCodecError(
      "payload decoded to bytes that are not a valid encoded value",
      { runId, seq },
      { cause: e },
    );
  }
}

/**
 * Put an entry's payload slot through the codec, producing the stored form: slot removed, the
 * base64 output in `*Enc`, stamped v:3. Returns the input untouched when there is no codec, no
 * slot, an absent slot, or the entry already arrived transformed (pass-through appends — a work
 * item's ref/enc is journaled verbatim and must not be transformed twice).
 */
export async function encodePayload(
  runId: string,
  entry: JournalEntry,
  codec: Codec,
  payloadCodec: PayloadCodec | undefined,
): Promise<JournalEntry> {
  if (payloadCodec === undefined) return entry;
  if (blobRefOf(entry) !== undefined || encSlotOf(entry) !== undefined) return entry;
  const slot = payloadSlot(entry);
  if (slot === undefined) return entry;
  const rec = entry as unknown as Record<string, unknown>;
  if (rec[slot] === undefined) return entry;

  let plain: Uint8Array;
  try {
    plain = codec.encodeValue(rec[slot]);
  } catch (e) {
    // Same class as any other unencodable value: deterministic, fails the run.
    throw new SerializationError(
      runId,
      `run ${runId}: cannot encode the ${slot} of a ${entry.kind} entry (seq ${entry.seq})`,
      { cause: e },
    );
  }
  let out: Uint8Array;
  try {
    out = await payloadCodec.encode(plain);
  } catch (e) {
    throw new PayloadCodecError(
      "the configured payloadCodec failed to encode this payload",
      { runId, seq: entry.seq },
      { cause: e },
    );
  }
  const stored: Record<string, unknown> = { ...rec };
  delete stored[slot];
  stored[ENC_FIELD_OF[slot]] = toBase64(out);
  stored.v = WIRE_VERSION_ENCODED;
  return stored as unknown as JournalEntry;
}

function isBlobRefShape(v: unknown): v is BlobRef {
  return (
    typeof v === "object" &&
    v !== null &&
    typeof (v as BlobRef).name === "string" &&
    typeof (v as BlobRef).digest === "string" &&
    typeof (v as BlobRef).size === "number"
  );
}

/** The blob ref an entry carries, if any (with the slot it stands in for). */
export function blobRefOf(entry: JournalEntry): { slot: PayloadSlot; ref: unknown } | undefined {
  const rec = entry as unknown as Record<string, unknown>;
  for (const field of ALL_REF_FIELDS) {
    if (rec[field] !== undefined) {
      return { slot: field.slice(0, -3) as PayloadSlot, ref: rec[field] };
    }
  }
  return undefined;
}

/** SHA-256 as lowercase hex (WebCrypto — available on Node ≥ 20 and Bun). */
export async function sha256Hex(data: Uint8Array): Promise<string> {
  const d = await globalThis.crypto.subtle.digest("SHA-256", data);
  return [...new Uint8Array(d)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/**
 * Build the offloaded (stored) form of an entry: slot removed, `*Ref` added, stamped v:2 — or v:3
 * when the blob holds payload-codec output (`transformed`), which is what tells a reader to
 * untransform the bytes after fetching them. Getting that flag wrong is not a corruption: a v:2 ref
 * to ciphertext decodes to garbage and PARKS the run (PayloadCodecError), a v:3 ref to plaintext
 * fails to decrypt and parks likewise.
 */
export function withBlobRef(
  entry: JournalEntry,
  slot: PayloadSlot,
  ref: BlobRef,
  transformed = false,
): JournalEntry {
  const stored: Record<string, unknown> = { ...(entry as unknown as Record<string, unknown>) };
  delete stored[slot];
  stored[REF_FIELD_OF[slot]] = ref;
  stored.v = transformed ? WIRE_VERSION_ENCODED : WIRE_VERSION_BLOBS;
  return stored as unknown as JournalEntry;
}

export interface OffloadOutcome {
  /** The entry to publish (the offloaded form, or the input entry when nothing was offloaded). */
  entry: JournalEntry;
  /** The encoded bytes of `entry`. */
  encoded: Uint8Array;
  /** Present when a blob was written. */
  ref?: BlobRef;
}

/**
 * Offload an entry's payload slot if its encoded form exceeds the threshold. Blob-before-pointer:
 * the blob is fully written BEFORE the returned entry (holding the ref) exists to be published, so
 * a ref on the stream always resolves. No-ops (returns the input) when: under threshold, the kind
 * has no payload slot, the slot is absent, or the entry already carries a ref (pass-through
 * appends). The caller re-checks the returned `encoded` against max_payload — an entry whose
 * METADATA alone is oversized cannot be saved by offload and must fail loud.
 */
export async function offloadEntry(
  runId: string,
  entry: JournalEntry,
  encoded: Uint8Array,
  codec: Codec,
  blobs: BlobStore,
  thresholdBytes: number,
): Promise<OffloadOutcome> {
  if (encoded.byteLength <= thresholdBytes) return { entry, encoded };
  if (blobRefOf(entry) !== undefined) return { entry, encoded };

  // Already through the payload codec (v:3 inline): the blob must hold the codec's output, not the
  // plaintext — so offload the ciphertext we already have rather than re-deriving it from a slot
  // that is no longer on the entry. Stays v:3, so the reader knows to untransform after fetching.
  const enc = encSlotOf(entry);
  if (enc !== undefined) {
    if (typeof enc.encoded !== "string") return { entry, encoded };
    const bytes = fromBase64Checked(runId, entry.seq, enc.encoded);
    const ref = await putBlob(runId, bytes, blobs);
    const stored: Record<string, unknown> = { ...(entry as unknown as Record<string, unknown>) };
    delete stored[ENC_FIELD_OF[enc.slot]];
    stored[REF_FIELD_OF[enc.slot]] = ref;
    const out = stored as unknown as JournalEntry;
    return { entry: out, encoded: codec.encode(out), ref };
  }

  const slot = payloadSlot(entry);
  if (slot === undefined) return { entry, encoded };
  const value = (entry as unknown as Record<string, unknown>)[slot];
  if (value === undefined) return { entry, encoded };

  // `undefined`: this entry reached here UNtransformed, so its blob is plaintext and the ref stays
  // v:2. An entry the codec did touch took the `encSlotOf` branch above, which offloads the
  // ciphertext it already holds — re-deriving it from a slot that is no longer on the entry is not
  // possible, which is why that branch exists at all.
  const { ref, transformed } = await storePayloadBlob(
    runId,
    `the ${slot} of a ${entry.kind} entry (seq ${entry.seq})`,
    value,
    codec,
    blobs,
    undefined,
  );
  const stored = withBlobRef(entry, slot, ref, transformed);
  return { entry: stored, encoded: codec.encode(stored), ref };
}

/**
 * Encode one value and write it to the blob store under its content-addressed name
 * (blob-before-pointer: the returned ref is safe to publish anywhere). Shared by journal appends,
 * work-item shrinking, and the KV status finalize — identical value + codec ⇒ identical bytes ⇒
 * identical name, so the store dedups cross-embeddings of one value under the same runId.
 *
 * **This is the ONE function that writes a user payload to the blob store**, and it is the reason
 * that is worth stating: a payload reaches durable storage by three different routes — a journal
 * append, a work item's shrink, the status hint's shrink — and each one used to decide for itself
 * whether the codec applied. One of them silently didn't, so an oversized payload was stored in the
 * clear while a small one was encrypted. Two things here make that shape unavailable:
 *
 * 1. `payloadCodec` is a REQUIRED parameter. `undefined` is a decision a caller has to write down,
 *    not something it can reach by omission. (`putBlob` is private, so there is no way around it.)
 * 2. It returns `transformed` alongside the ref. The v:2-vs-v:3 marker on the entry and the
 *    `payloadEnc` flag on a work item are then derived from what this function ACTUALLY did, rather
 *    than from a boolean the caller tracks in parallel and can get out of step — which is exactly
 *    how the second half of that bug looked.
 *
 * With a codec the BLOB holds its output and the digest covers it — the same posture
 * {@link offloadEntry} takes for a v:3 entry, and the reason the content address is over ciphertext
 * rather than over a secret's cleartext hash.
 *
 * Error classes: an encode failure is a deterministic SerializationError (fails the run); a codec
 * failure is a PayloadCodecError (parks); a blob put failure propagates raw (transient infra — the
 * caller retries).
 */
export async function storePayloadBlob(
  runId: string,
  what: string,
  value: unknown,
  codec: Codec,
  blobs: BlobStore,
  payloadCodec: PayloadCodec | undefined,
): Promise<{ ref: BlobRef; transformed: boolean }> {
  let blob: Uint8Array;
  try {
    blob = codec.encodeValue(value);
  } catch (e) {
    throw new SerializationError(runId, `run ${runId}: cannot encode ${what} as a blob`, {
      cause: e,
    });
  }
  if (payloadCodec !== undefined) {
    try {
      blob = await payloadCodec.encode(blob);
    } catch (e) {
      throw new PayloadCodecError(
        `the configured payloadCodec failed to encode ${what}`,
        { runId },
        { cause: e },
      );
    }
  }
  return { ref: await putBlob(runId, blob, blobs), transformed: payloadCodec !== undefined };
}

/**
 * Content-address a ready-made byte string and write it (blob-before-pointer). Split out of
 * {@link offloadValue} because the payload-codec path already holds the exact bytes to store —
 * re-encoding a value it has only in transformed form is not possible, and hashing the plaintext
 * would content-address a secret by its cleartext digest.
 */
async function putBlob(runId: string, blob: Uint8Array, blobs: BlobStore): Promise<BlobRef> {
  const digest = `sha256:${await sha256Hex(blob)}`;
  const ref: BlobRef = { name: `${runId}/${digest}`, digest, size: blob.byteLength };
  await blobs.put(ref.name, blob); // blob-before-pointer; throws = transient infra (retry)
  return ref;
}

/**
 * A bounded byte cache for resolved blobs, keyed by object name. Content-addressed names make
 * entries immutable — a hit can never be stale. Values are CACHED AS BYTES and decoded fresh per
 * resolution, so no two replays ever share (and could cross-mutate) one decoded object.
 */
export class BlobBytesCache {
  private readonly map = new Map<string, Uint8Array>();
  private bytes = 0;
  constructor(private readonly maxBytes: number) {}

  get(name: string): Uint8Array | undefined {
    const v = this.map.get(name);
    if (v !== undefined) {
      // refresh LRU position
      this.map.delete(name);
      this.map.set(name, v);
    }
    return v;
  }

  set(name: string, data: Uint8Array): void {
    if (data.byteLength > this.maxBytes || this.map.has(name)) return;
    this.map.set(name, data);
    this.bytes += data.byteLength;
    while (this.bytes > this.maxBytes) {
      const oldest = this.map.keys().next();
      if (oldest.done === true) break;
      const evicted = this.map.get(oldest.value);
      this.map.delete(oldest.value);
      this.bytes -= evicted?.byteLength ?? 0;
    }
  }
}

/**
 * Resolve an entry's blob ref (if any) back to an inline value. Verifies the content digest before
 * trusting the bytes. Error taxonomy (the worker parks on both — loud, recoverable, never a
 * terminal): {@link BlobMissingError} when the object is definitively absent or this reader has no
 * blob store while the journal carries refs; {@link BlobIntegrityError} on a malformed ref or a
 * digest mismatch. A transport failure inside `blobs.get` propagates as-is (transient infra).
 */
export async function resolveEntry(
  runId: string,
  entry: JournalEntry,
  codec: Codec,
  blobs: BlobStore | undefined,
  cache?: BlobBytesCache,
  payloadCodec?: PayloadCodec,
): Promise<JournalEntry> {
  // An INLINE transformed slot (v:3 without a ref) never touches the blob store.
  const enc = encSlotOf(entry);
  if (enc !== undefined) {
    if (typeof enc.encoded !== "string") {
      throw new PayloadCodecError(`malformed ${enc.slot}Enc (expected a base64 string)`, {
        runId,
        seq: entry.seq,
      });
    }
    const plain = await untransform(
      runId,
      entry.seq,
      fromBase64Checked(runId, entry.seq, enc.encoded),
      payloadCodec,
    );
    const out: Record<string, unknown> = { ...(entry as unknown as Record<string, unknown>) };
    delete out[ENC_FIELD_OF[enc.slot]];
    out[enc.slot] = decodeValueChecked(runId, entry.seq, plain, codec);
    return out as unknown as JournalEntry;
  }

  const found = blobRefOf(entry);
  if (found === undefined) {
    assertNotEncodedWithoutCodec(runId, entry, payloadCodec);
    return entry;
  }
  if (!isBlobRefShape(found.ref)) {
    throw new BlobIntegrityError(runId, entry.seq, "?", "malformed blob ref on the entry");
  }
  const ref = found.ref;
  if (blobs === undefined) {
    throw new BlobMissingError(
      runId,
      entry.seq,
      ref.name,
      "this reader has no blob store (offload disabled / DE_BLOBS not provisioned) but the journal carries blob refs",
      "no-store",
    );
  }
  let bytes = cache?.get(ref.name);
  if (bytes === undefined) {
    const fetched = await blobs.get(ref.name);
    if (fetched === null) {
      throw new BlobMissingError(
        runId,
        entry.seq,
        ref.name,
        "the object is gone from the blob store (GC'd early or bucket lost) — restore it or cancel the run",
      );
    }
    const digest = `sha256:${await sha256Hex(fetched)}`;
    if (digest !== ref.digest) {
      throw new BlobIntegrityError(
        runId,
        entry.seq,
        ref.name,
        `digest mismatch: ref has ${ref.digest}, content is ${digest}`,
      );
    }
    cache?.set(ref.name, fetched);
    bytes = fetched;
  }
  // An OFFLOADED transformed slot: the BLOB holds codec output, so untransform before decoding.
  // Digest verification above ran on the stored bytes — i.e. on the transformed form, which is
  // exactly what integrity should cover.
  const plain =
    entry.v >= WIRE_VERSION_ENCODED
      ? await untransform(runId, entry.seq, bytes, payloadCodec)
      : bytes;
  const resolved: Record<string, unknown> = { ...(entry as unknown as Record<string, unknown>) };
  delete resolved[REF_FIELD_OF[found.slot]];
  resolved[found.slot] = decodeValueChecked(runId, entry.seq, plain, codec);
  return resolved as unknown as JournalEntry;
}
