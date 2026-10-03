// @synadia-ai/durable — the BlobStore implemented over the NATS object store (DE_BLOBS). The only
// file that imports @nats-io/obj. Blobs are chunked by the object store, so a payload may vastly
// exceed the connection max_payload; names are content-addressed by the pure engine
// (journal/blobs.ts), which is what makes `put` idempotent and overwrite-safe here.
//
// Writes go through the object-store client. READS do not: its `get` streams the chunks through an
// ordered push consumer that it stops but never deletes (LEARNINGS #59 — one consumer left behind
// per blob read, and consumers are capped per stream on a managed service). The object store's
// on-stream layout is a NATS ADR (meta at `$O.<bucket>.M.<name>`, chunks at
// `$O.<bucket>.C.<nuid>`), so the chunks are read here with one batched direct get (direct.ts),
// no consumer, and verified against the size the meta records.

import { type JetStreamClient, type JetStreamManager } from "@nats-io/jetstream";
import { Objm, type ObjectStore } from "@nats-io/obj";
import { type Metrics, SILENT_METRICS, emitSafe } from "../../../src/internal/metrics.js";
import { type BlobStore } from "../../../src/journal/blobs.js";
import { readSubject } from "./direct.js";

export class NatsBlobStore implements BlobStore {
  private osP: Promise<ObjectStore> | undefined;
  private jsmP: Promise<JetStreamManager> | undefined;

  constructor(
    private readonly js: JetStreamClient,
    private readonly bucket: string,
    private readonly metrics: Metrics = SILENT_METRICS,
  ) {}

  /**
   * Open the bucket lazily and memoize — but reset on failure so a bucket that appears later
   * (provision raced us, or an operator fixed a deployment) heals on the next call instead of
   * caching the rejection forever. Any failure here propagates raw: transient infra, the caller
   * (worker NAK / client caller) retries.
   */
  private os(): Promise<ObjectStore> {
    this.osP ??= new Objm(this.js).open(this.bucket).catch((e: unknown) => {
      this.osP = undefined;
      throw e;
    });
    return this.osP;
  }

  async put(name: string, data: Uint8Array): Promise<void> {
    const t0 = Date.now();
    try {
      const os = await this.os();
      // Content-addressed dedup: the name IS the digest, so an existing complete object already
      // holds these bytes — skip the re-upload (common: a CAN rollover or terminal re-drive
      // re-embedding a resolved value converges on the same object).
      const info = await os.info(name);
      if (info !== null && !info.deleted && info.size === data.byteLength) {
        emitSafe(this.metrics, "blob.put", {
          name,
          bytes: data.byteLength,
          ms: Date.now() - t0,
          outcome: "exists",
        });
        return;
      }
      await os.putBlob({ name }, data);
      emitSafe(this.metrics, "blob.put", {
        name,
        bytes: data.byteLength,
        ms: Date.now() - t0,
        outcome: "ok",
      });
    } catch (e) {
      emitSafe(this.metrics, "blob.put", {
        name,
        bytes: data.byteLength,
        ms: Date.now() - t0,
        outcome: "error",
      });
      throw e;
    }
  }

  async get(name: string): Promise<Uint8Array | null> {
    const t0 = Date.now();
    try {
      const os = await this.os();
      // The meta record (a last-by-subject stream get, no consumer): absent or deleted = null.
      const info = await os.info(name);
      if (info === null || info.deleted) {
        emitSafe(this.metrics, "blob.get", { name, bytes: 0, ms: Date.now() - t0, outcome: "missing" }); // prettier-ignore
        return null;
      }
      // The chunks: one batched direct get on the object's chunk subject, in stream order.
      const jsm = await (this.jsmP ??= this.js.jetstreamManager(false));
      const read = await readSubject(jsm, `OBJ_${this.bucket}`, `$O.${this.bucket}.C.${info.nuid}`); // prettier-ignore
      // Completeness: the meta records the byte size, and a direct read may be a prefix (a
      // follower may answer, a chunk may still be in flight). Short = transient, retried by the
      // caller — never handed to the digest check as "corrupt".
      if (read.bytes !== info.size || read.out.length !== info.chunks) {
        throw new Error(
          `blob ${name}: read ${read.out.length} chunks / ${read.bytes} bytes, meta says ` +
            `${info.chunks} / ${info.size} (transient short read)`,
        );
      }
      const data = new Uint8Array(read.bytes);
      let at = 0;
      for (const m of read.out) {
        data.set(m.data, at);
        at += m.data.byteLength;
      }
      emitSafe(this.metrics, "blob.get", {
        name,
        bytes: data.byteLength,
        ms: Date.now() - t0,
        outcome: "ok",
      });
      return data;
    } catch (e) {
      emitSafe(this.metrics, "blob.get", {
        name,
        bytes: 0,
        ms: Date.now() - t0,
        outcome: "error",
      });
      throw e;
    }
  }
}
