// @synadia-ai/durable — the Journal implemented over JetStream. append() = optimistic publish with
// expect.lastSubjectSequence (CAS) + msgID (dedup); a 10071 rejection becomes a FencedError. load()
// reads the run's per-run subject in order with a batched DIRECT GET — one request, no consumer —
// verified complete against the leader's per-subject count. This is the shell over the
// one-primitive core (DESIGN.md §0).

import { type JetStreamClient, type JetStreamManager } from "@nats-io/jetstream";
import {
  FencedError,
  PayloadCodecError,
  PayloadTooLargeError,
  SerializationError,
} from "../../../src/errors.js";
import { type Metrics, SILENT_METRICS, emitSafe } from "../../../src/internal/metrics.js";
import {
  type BlobBytesCache,
  type BlobStore,
  offloadEntry,
  encodePayload,
  resolveEntry,
} from "../../../src/journal/blobs.js";
import { type Codec } from "../../../src/journal/codec.js";
import { type PayloadCodec } from "../../../src/journal/payload-codec.js";
import { type JournalEntry, type LoadedEntry } from "../../../src/journal/entry.js";
import { type AppendResult, type Journal, type JournalAppend } from "../../../src/journal/store.js";
import { readSubject } from "./direct.js";
import { type Names } from "./names.js";

/** NATS error code for a rejected expect.lastSubjectSequence (StreamWrongLastSequence). */
const WRONG_LAST_SEQUENCE = 10071;

function isWrongLastSequence(e: unknown): boolean {
  return typeof e === "object" && e !== null && "code" in e && e.code === WRONG_LAST_SEQUENCE;
}

/** The nats-core CLIENT-SIDE rejection for a publish beyond the connection's max_payload. */
function isMaxPayloadExceeded(e: unknown): boolean {
  return e instanceof Error && e.message.includes("max_payload");
}

/** Large-payload offload wiring for a journal instance (absent = offload disabled). */
export interface JournalOffload {
  blobs: BlobStore;
  /** Encoded-entry size above which the payload slot is offloaded (already clamped/derived). */
  thresholdBytes: number;
  /** Bounded byte cache for resolved blobs (shared across this instance's loads). */
  cache?: BlobBytesCache;
}

export class NatsJournal implements Journal {
  constructor(
    private readonly js: JetStreamClient,
    private readonly jsm: JetStreamManager,
    private readonly n: Names,
    private readonly codec: Codec,
    /** The connection's max_payload (nc.info), enforced BEFORE publish; undefined = unknown. */
    private readonly maxPayload?: number,
    private readonly metrics: Metrics = SILENT_METRICS,
    private readonly offload?: JournalOffload,
    /** Optional payload transform (encryption at rest). Applied to payload slots only. */
    private readonly payloadCodec?: PayloadCodec,
    /** Messages per direct-get request; a longer journal is read in consecutive batches. */
    private readonly readBatch = 1024,
  ) {}

  /**
   * Resolve one entry's blob ref to its inline value (no-op for inline entries). Public for
   * observers that scan with {@link loadRaw} and inflate only the entries they need.
   */
  resolveLoaded(runId: string, entry: JournalEntry): Promise<JournalEntry> {
    return resolveEntry(
      runId,
      entry,
      this.codec,
      this.offload?.blobs,
      this.offload?.cache,
      this.payloadCodec,
    );
  }

  /** Load with blob refs resolved (the Journal seam contract — the core never sees a ref). */
  async load(runId: string): Promise<LoadedEntry[]> {
    const out = await this.loadRaw(runId);
    // Resolve blob refs AFTER the stream drain (digest-verified inflation): a multi-MB blob fetch
    // inside the fetch loop would eat the consumer iterator's expiry window and masquerade as a
    // short read. A BlobMissingError/BlobIntegrityError aborts the load — the journal is
    // unreadable without the blob and the worker parks the run; a transient blob fetch failure
    // retries like any load failure.
    for (let i = 0; i < out.length; i++) {
      const l = out[i];
      if (l !== undefined) out[i] = { entry: await this.resolveLoaded(runId, l.entry), streamSeq: l.streamSeq }; // prettier-ignore
    }
    return out;
  }

  /**
   * Load WITHOUT resolving blob refs — for observers that only scan kinds/metadata (terminal
   * polling, cancel cascades) and must not download every offloaded value to do so. Entries may
   * carry `*Ref` fields in place of their payload slot; inflate selectively via
   * {@link resolveLoaded}.
   */
  async loadRaw(runId: string): Promise<LoadedEntry[]> {
    const t0 = Date.now();
    const subject = this.n.journalSubject(runId);
    const emit = (entries: number, bytes: number, outcome: string): void =>
      emitSafe(this.metrics, "journal.load", {
        runId,
        entries,
        bytes,
        ms: Date.now() - t0,
        outcome,
      });

    // Two requests in flight together, one round trip in all: the LEADER's per-subject message
    // count (a JS API request — leader-served) and the batched direct read of the subject (which a
    // replica may answer). The count is the completeness check the protocol requires (§5.5): a
    // direct read served by a lagging follower is a consistent PREFIX, and replaying over a prefix
    // mis-places the frontier — it fences on an already-recorded command, indistinguishable from a
    // real concurrent writer. The old path did this in four sequential API calls (stream info,
    // ordered-consumer create, fetch, consumer delete — ~570 ms at a 50 ms RTT, and the create and
    // delete serialize on the stream server-side); this is one, and it creates no consumer at all,
    // so a fleet's loads no longer churn the journal stream's consumer list (LEARNINGS #58).
    const [count, firstRead] = await Promise.all([
      this.jsm.streams
        .info(this.n.journalStream, { subjects_filter: subject })
        .then((info) => info.state.subjects?.[subject] ?? 0),
      this.readSubject(subject),
    ]);
    let read = firstRead;
    if (read.out.length < count) {
      // The two requests raced an append: the count saw it, the read did not. That is the common
      // shape of a "short" read on a single server (an append landing between two concurrent
      // requests), so read once more before calling it one — the append has landed by now. A
      // genuinely lagging follower comes up short twice and fails loud below.
      read = await this.readSubject(subject);
    }
    const out = read.out;
    // A short read must FAIL LOUD, never return a truncated journal (see above; invariant #2 —
    // progress is never gated on an unverified read). Throwing here is a transient infra error:
    // the worker NAKs and the redelivery re-reads. (More than the count is fine: the count was
    // taken first and an append landed in between — the read is a longer consistent prefix.)
    if (out.length < count) {
      emit(out.length, read.bytes, "short-read");
      throw new Error(
        `run ${runId}: journal load incomplete — got ${out.length} of ${count} entries (transient short read)`,
      );
    }
    emit(out.length, read.bytes, "ok");
    return out;
  }

  /** The run's subject as a decoded journal: one batched direct get, no consumer (direct.ts). */
  private async readSubject(subject: string): Promise<{ out: LoadedEntry[]; bytes: number }> {
    const read = await readSubject(this.jsm, this.n.journalStream, subject, this.readBatch);
    return {
      out: read.out.map((m) => ({ entry: this.codec.decode(m.data), streamSeq: m.seq })),
      bytes: read.bytes,
    };
  }

  async append(runId: string, a: JournalAppend): Promise<AppendResult> {
    const t0 = Date.now();
    const emit = (outcome: string, bytes: number): void =>
      emitSafe(this.metrics, "journal.append", {
        runId,
        kind: a.entry.kind,
        seq: a.entry.seq,
        bytes,
        ms: Date.now() - t0,
        outcome,
      });
    const subject = this.n.journalSubject(runId);
    // Encode FIRST and isolate its failure: a non-serializable value is a deterministic programming
    // error (retrying can't help), distinct from a transient publish failure. Surface it as a
    // SerializationError so the core fails the run terminally instead of NAK-looping forever.
    // The payload transform runs BEFORE the structural encode: it rewrites the entry (slot out,
    // `*Enc` in, v:3) and the bytes we publish must be of that rewritten form. Its error classes are
    // distinct and must survive — a PayloadCodecError parks the run, a SerializationError fails it.
    let entry: JournalEntry;
    try {
      entry = await encodePayload(runId, a.entry, this.codec, this.payloadCodec);
    } catch (e) {
      emit(e instanceof PayloadCodecError ? "payload-codec" : "serialization", 0);
      throw e;
    }
    let payload: Uint8Array;
    try {
      payload = this.codec.encode(entry);
    } catch (e) {
      emit("serialization", 0);
      throw new SerializationError(
        runId,
        `run ${runId}: cannot encode ${entry.kind} entry (seq ${entry.seq}): ${String(e)}`,
        { cause: e },
      );
    }
    // Large-payload offload, BEFORE the max_payload gate: an over-threshold entry writes its
    // payload slot to the blob store (blob-before-pointer) and publishes the small v:2 ref form
    // instead. Error classes pass through untouched — a SerializationError from the value encode
    // is deterministic (fails the run), a blob-store put failure is transient infra (the emit in
    // the catch below never fires for it; the worker NAKs and retries).
    if (this.offload && payload.byteLength > this.offload.thresholdBytes) {
      const out = await offloadEntry(
        runId,
        entry,
        payload,
        this.codec,
        this.offload.blobs,
        this.offload.thresholdBytes,
      );
      entry = out.entry;
      payload = out.encoded;
    }
    // A payload beyond the server's max_payload is DETERMINISTIC for this value (re-publishing the
    // same bytes can never succeed) — surface it like an encode failure so the core fails the run
    // terminally instead of NAK-looping forever (with offload enabled, only oversized METADATA —
    // e.g. a pathological step name — still lands here). Checked before publish when the
    // connection knows its limit; the catch below maps the client-side rejection when it doesn't.
    if (this.maxPayload !== undefined && payload.byteLength > this.maxPayload) {
      emit("payload-too-large", payload.byteLength);
      throw new PayloadTooLargeError(
        runId,
        `run ${runId}: ${entry.kind} entry (seq ${entry.seq}) encodes to ${payload.byteLength} bytes — ` +
          `over the server max_payload of ${this.maxPayload}; keep journaled values small or offload large blobs`,
      );
    }
    try {
      const ack = await this.js.publish(subject, payload, {
        msgID: a.msgID,
        expect: { lastSubjectSequence: a.expectLastSeq },
      });
      const duplicate = ack.duplicate ?? false;
      emit(duplicate ? "duplicate" : "ok", payload.byteLength);
      return { streamSeq: ack.seq, duplicate };
    } catch (e) {
      if (isWrongLastSequence(e)) {
        emit("fenced", payload.byteLength);
        throw new FencedError(
          runId,
          `run ${runId}: journal append fenced at expectLastSeq=${a.expectLastSeq}`,
          {
            cause: e,
          },
        );
      }
      if (isMaxPayloadExceeded(e)) {
        emit("payload-too-large", payload.byteLength);
        // Fallback when maxPayload was unknown at construction: nats-core rejected client-side.
        throw new PayloadTooLargeError(
          runId,
          `run ${runId}: ${a.entry.kind} entry (seq ${a.entry.seq}) encodes to ${payload.byteLength} bytes — ` +
            `over the server max_payload; keep journaled values small or offload large blobs`,
          { cause: e },
        );
      }
      emit("error", payload.byteLength);
      throw e;
    }
  }
}
