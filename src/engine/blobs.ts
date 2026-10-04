// @synadia-ai/durable — the SDK half of the chunked upload and download (durable-engine docs/API.md
// "Sizes", `put`, `get`; the engine's findings F25, this repo's F1). A value whose request would
// pass `request_max` goes up in `put` parts first and travels as the ref the engine answers; a ref
// the engine hands back because a value would not fit a reply comes down in `get` parts, its digest
// checked. Both directions run through the library's own offload code (`offloadEntry`,
// `storePayloadBlob`, `resolveEntry` of journal/blobs.ts) with the engine as its blob store, so a
// ref, its name (`{runId}/sha256:<hex>`), its `v:2` / `v:3` marking and its digest check are the
// library's bytes. The pure core never sees a ref: the worker's journal is resolved before replay.

import { type BlobRef } from "../journal/entry.js";
import { type BlobStore, sha256Hex } from "../journal/blobs.js";
import { fromBase64, toBase64 } from "../journal/payload-codec.js";
import { type DurableEventFields, type DurableEventName } from "../internal/metrics.js";
import { EngineUnavailableError } from "./errors.js";
import { type Transport } from "./transport.js";

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
  emit?: (name: DurableEventName, fields: DurableEventFields) => void;
}

/**
 * The engine as the library's `BlobStore`: `put` uploads in parts and checks that the ref the
 * engine answers names the same blob; `get` downloads in parts, and answers `null` when the engine
 * says the blob is gone (`get`'s `internal`), so `resolveEntry` raises the library's
 * `BlobMissingError` and the worker parks the run.
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
      const upload = globalThis.crypto.randomUUID();
      const last = Math.max(0, Math.ceil(data.byteLength / partBytes) - 1);
      let ref: BlobRef | undefined;
      for (let part = 0; part <= last; part++) {
        const bytes = toBase64(data.subarray(part * partBytes, (part + 1) * partBytes));
        const r = await this.#o.request<{ ref?: BlobRef }>("put", {
          runId,
          ...this.#leaseFor(runId),
          name: upload,
          part,
          bytes,
          ...(part === last ? { last: true, digest } : {}),
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
      // `get` answers `internal` for a blob that is gone (API.md): a definitive absence.
      if (e instanceof EngineUnavailableError && e.code === "internal") {
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

/** The encoded size of a request body, as the transport will send it. */
export function bodyBytes(body: object): number {
  return new TextEncoder().encode(JSON.stringify(body)).byteLength;
}
