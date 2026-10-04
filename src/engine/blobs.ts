// @synadia-ai/durable — the SDK half of the chunked upload and download (durable-engine docs/API.md
// "Sizes", `put`, `get`; the engine's findings F25, this repo's F1). A value whose request would
// pass `request_max` goes up in `put` parts first and travels as the ref the engine answers; a ref
// the engine hands back because a value would not fit a reply comes down in `get` parts, its digest
// checked. Both directions run through the library's own offload code (`offloadEntry`,
// `storePayloadBlob`, `resolveEntry` of journal/blobs.ts) with the engine as its blob store, so a
// ref, its name (`{runId}/sha256:<hex>`), its `v:2` / `v:3` marking and its digest check are the
// library's bytes. The pure core never sees a ref: the worker's journal is resolved before replay.

import { BlobMissingError, PayloadTooLargeError, SerializationError } from "../errors.js";
import { type BlobRef } from "../journal/entry.js";
import { type BlobStore, sha256Hex } from "../journal/blobs.js";
import { fromBase64, toBase64 } from "../journal/payload-codec.js";
import { type DurableEventFields, type DurableEventName } from "../internal/metrics.js";
import { EngineUnavailableError, InvalidRequestError } from "./errors.js";
import { REQUEST_MARGIN_MS, type Transport } from "./transport.js";

/** What `info`'s `limits` says a request may weigh, and the raw bytes of one `put` part. */
export interface Sizes {
  requestMax: number;
  partBytes: number;
}

/** The headroom the engine keeps under max_payload for a request's envelope (API.md "Sizes"). */
const REQUEST_HEADROOM = 16 * 1024;

/** `(request_max − 4096) / 4 × 3`: one part's raw bytes, base64 of which fits a request. */
export function partBytesOf(requestMax: number): number {
  return Math.floor((requestMax - 4096) / 4) * 3;
}

/**
 * The connection's sizes, from the engine's `info` once per transport (and from the server's
 * max_payload less the engine's headroom while no engine has answered).
 */
export function sizesOf(
  request: Transport,
  maxPayload: () => number | undefined,
): () => Promise<Sizes> {
  let known: Sizes | undefined;
  return async () => {
    if (known !== undefined) return known;
    try {
      const r = await request<{ limits?: { request_max?: number } }>("info", {});
      const rm = r.limits?.request_max;
      if (typeof rm === "number" && rm > 4096) {
        known = { requestMax: rm, partBytes: partBytesOf(rm) };
        return known;
      }
    } catch {
      /* the fallback below; the request itself will say what is wrong */
    }
    const mp = maxPayload() ?? 1024 * 1024;
    const rm = Math.max(8 * 1024, mp - REQUEST_HEADROOM);
    return { requestMax: rm, partBytes: partBytesOf(rm) };
  };
}

/** The engine's object-store chunk: one acked publish each, so one dependent round trip each. */
export const STORE_CHUNK_BYTES = 128 * 1024;
/** Round trips allowed per chunk the engine stores, reads back or deletes for a `put`. */
export const PUT_RTTS_PER_CHUNK = 4;
/** The slowest store a `put` waits for: a MiB of what the engine moves per this many ms. */
export const PUT_MS_PER_MIB = 1_000;

/**
 * How long a `put` part may take to answer (this repo's F33). A part is staged in the blob store
 * before it is answered; the last one is answered once the engine has read every part back,
 * checked the digest, written the blob and deleted the parts (API.md `put`). So the wait grows
 * with the bytes the engine moves for it (`stored`: the part's own, plus the value twice on the
 * last), at a round trip per store chunk and a floor on the store's throughput, over the margin
 * every request gets. A slow store is waited for, not refused.
 */
export function putTimeoutMs(stored: number, rttMs: number): number {
  const chunks = Math.ceil(stored / STORE_CHUNK_BYTES);
  return Math.ceil(
    REQUEST_MARGIN_MS +
      chunks * PUT_RTTS_PER_CHUNK * Math.max(0, rttMs) +
      (stored / (1024 * 1024)) * PUT_MS_PER_MIB,
  );
}

/** The run a blob belongs to: its name is `{runId}/sha256:<hex>`. */
export function runOfBlob(name: string): string {
  const i = name.indexOf("/");
  return i < 0 ? "" : name.slice(0, i);
}

export interface EngineBlobsOptions {
  request: Transport;
  sizes: () => Promise<Sizes>;
  /** The item's run and lease, from an execution: sent with a part of that run's blob only. */
  lease?: { runId: string; lease: string };
  /** The connection's round trip in ms (`nc.rtt()`), measured once per upload; 0 without it. */
  rtt?: () => Promise<number>;
  emit?: (name: DurableEventName, fields: DurableEventFields) => void;
}

/**
 * The engine as the library's `BlobStore`: `put` uploads in parts and checks that the ref the
 * engine answers names the same blob; `get` downloads in parts, and answers `null` when the engine
 * says the blob is gone (`get`'s `blob_gone`), so `resolveEntry` raises the library's
 * `BlobMissingError` naming the entry and the worker parks the run. Bytes the engine found not to
 * be the digest's come up as the transport's `BlobIntegrityError`, which parks it the same way.
 */
export class EngineBlobs implements BlobStore {
  readonly #o: EngineBlobsOptions;
  constructor(o: EngineBlobsOptions) {
    this.#o = o;
  }

  #leaseFor(runId: string): Record<string, string> {
    const l = this.#o.lease;
    return l !== undefined && l.runId === runId ? { lease: l.lease } : {};
  }

  /** Upload `data` as the blob `name` (`{runId}/sha256:<hex>`, the library's content address). */
  async put(name: string, data: Uint8Array): Promise<void> {
    const ref = await this.upload(runOfBlob(name), data);
    if (ref.name !== name) {
      throw new EngineUnavailableError("put", "internal", `put: the engine stored ${ref.name}, not ${name}`); // prettier-ignore
    }
  }

  /** Upload `data` into `runId`'s namespace in parts; the ref the engine answers. */
  async upload(runId: string, data: Uint8Array): Promise<BlobRef> {
    const t0 = Date.now();
    const digest = `sha256:${await sha256Hex(data)}`;
    const name = `${runId}/${digest}`;
    try {
      const { partBytes } = await this.#o.sizes();
      const rtt = await this.#rtt();
      const upload = globalThis.crypto.randomUUID();
      const last = Math.max(0, Math.ceil(data.byteLength / partBytes) - 1);
      let ref: BlobRef | undefined;
      for (let part = 0; part <= last; part++) {
        const raw = data.subarray(part * partBytes, (part + 1) * partBytes);
        const stored = raw.byteLength + (part === last ? 2 * data.byteLength : 0);
        const r = await this.#o
          .request<{ ref?: BlobRef }>(
            "put",
            {
              runId,
              ...this.#leaseFor(runId),
              name: upload,
              part,
              bytes: toBase64(raw),
              ...(part === last ? { last: true, digest } : {}),
            },
            { timeoutMs: putTimeoutMs(stored, rtt) },
          )
          .catch((e: unknown) => {
            throw deterministicPut(runId, data.byteLength, part, e);
          });
        ref = r.ref;
      }
      if (ref === undefined) throw new EngineUnavailableError("put", "internal", "put: the last part answered no ref"); // prettier-ignore
      this.#o.emit?.("blob.put", { name, bytes: data.byteLength, ms: Date.now() - t0, outcome: "ok" }); // prettier-ignore
      return ref;
    } catch (e) {
      this.#o.emit?.("blob.put", { name, bytes: data.byteLength, ms: Date.now() - t0, outcome: "error" }); // prettier-ignore
      throw e;
    }
  }

  async #rtt(): Promise<number> {
    try {
      return (await this.#o.rtt?.()) ?? 0;
    } catch {
      return 0; // the request itself will say what is wrong with the connection
    }
  }

  /** Download the blob `name` in parts; `null` when the engine says it is gone. */
  async get(name: string): Promise<Uint8Array | null> {
    const t0 = Date.now();
    const runId = runOfBlob(name);
    const parts: Uint8Array[] = [];
    let size = 0;
    try {
      for (let part = 0; ; part++) {
        const r = await this.#o.request<{ bytes: string; last?: true; size: number }>("get", { runId, ...this.#leaseFor(runId), name, part }); // prettier-ignore
        const b = fromBase64(r.bytes);
        parts.push(b);
        size += b.byteLength;
        if (r.last === true || b.byteLength === 0 || size >= r.size) break;
      }
    } catch (e) {
      // `get` answers `blob_gone` for a blob that is gone (API.md, the engine's F30): a definitive
      // absence, which the transport made the library's BlobMissingError.
      if (e instanceof BlobMissingError) {
        this.#o.emit?.("blob.get", { name, bytes: 0, ms: Date.now() - t0, outcome: "missing" });
        return null;
      }
      this.#o.emit?.("blob.get", { name, bytes: size, ms: Date.now() - t0, outcome: "error" });
      throw e;
    }
    const out = new Uint8Array(size);
    let at = 0;
    for (const p of parts) {
      out.set(p, at);
      at += p.byteLength;
    }
    this.#o.emit?.("blob.get", { name, bytes: size, ms: Date.now() - t0, outcome: "ok" });
    return out;
  }
}

/**
 * A `put` that the engine did not answer within its size-scaled wait, or refused as malformed, is
 * deterministic for this value (this repo's F33): a PayloadTooLargeError or SerializationError,
 * which the pure core fails the run on (and a client call rejects with), rather than a transient
 * the item's redelivery would meet again with the same value, for ever. A lease that expired, a
 * closed connection, no engine at all and the engine's own `unavailable` keep their meaning.
 */
function deterministicPut(runId: string, size: number, part: number, e: unknown): unknown {
  if (e instanceof EngineUnavailableError && e.timedOut) {
    return new PayloadTooLargeError(runId, `put (run ${runId}): the engine did not store a ${size}-byte value in time (part ${part}); the blob store is too slow for a value this size: ${e.message}`, { cause: e }); // prettier-ignore
  }
  if (e instanceof InvalidRequestError) {
    return new SerializationError(runId, `put (run ${runId}): ${e.message}`, { cause: e });
  }
  return e;
}

/** The encoded size of a request body, as the transport will send it. */
export function bodyBytes(body: object): number {
  return new TextEncoder().encode(JSON.stringify(body)).byteLength;
}
