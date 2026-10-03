// @synadia-ai/durable — the Journal seam between pure core and the NATS shell, plus an in-memory
// implementation used by unit tests. Both honor the same contract:
//   - append() is an optimistic CAS keyed by `expectLastSeq` (the run subject's last stream seq);
//     a mismatch throws FencedError (NATS code 10071).
//   - **CAS dominates dedup** (verified vs real NATS — primitives.test.ts): a stale `expectLastSeq`
//     fences even when the `msgID` is a duplicate, so a concurrent zombie cannot slip a write past the
//     fence. A duplicate `msgID` resolves as a duplicate ONLY when the CAS still matches (true no-op).
// This is the single primitive the whole design rests on (DESIGN.md §0).

import { FencedError, PayloadTooLargeError, SerializationError } from "../errors.js";
import { type BlobStore, encodePayload, offloadEntry, resolveEntry } from "./blobs.js";
import { type Codec, jsonCodec } from "./codec.js";
import { type PayloadCodec } from "./payload-codec.js";
import { type JournalEntry, type LoadedEntry } from "./entry.js";

export interface JournalAppend {
  entry: JournalEntry;
  msgID: string;
  /** Expected stream-seq of the last message on this run's journal subject (0 if none). */
  expectLastSeq: number;
}

export interface AppendResult {
  /** Stream sequence assigned to the entry (or the original seq on a duplicate). */
  streamSeq: number;
  /** True if the msgID was deduped (the entry already existed). */
  duplicate: boolean;
}

export interface Journal {
  /** Load the full ordered journal for a run (empty if none). Blob refs are resolved to inline values. */
  load(runId: string): Promise<LoadedEntry[]>;
  /** Append one entry with optimistic concurrency. Throws FencedError if the CAS fails. */
  append(runId: string, append: JournalAppend): Promise<AppendResult>;
}

/**
 * In-memory Journal with NATS-faithful semantics (per-run subject = per-run ordered log; global
 * stream-seq counter; msgID dedup; CAS via expectLastSeq). Lets the replay engine and two-phase
 * crash logic be tested without a server. Dedup is unbounded here (NATS dedup is time-bounded —
 * but correctness never relies on it; seq + CAS is the guard).
 *
 * Entries are stored ENCODED (round-tripped through the codec on append/load), exactly like the NATS
 * shell: an unencodable entry rejects with SerializationError, so the pure suite is not blind to
 * serialization (the gap that let a non-serializable value's infinite-retry bug hide; see DESIGN §7).
 *
 * With `blobs` (+ `offloadThresholdBytes`) configured it also mirrors large-payload offload
 * faithfully: an encoded entry over the threshold writes its payload slot to the blob store FIRST
 * (blob-before-pointer — so a CAS loser's blob still lands, exactly like the real flow), stores the
 * ref'd v:2 form, and load() resolves refs (digest-verified) back to inline values.
 */
export class InMemoryJournal implements Journal {
  private readonly logs = new Map<string, { data: Uint8Array; streamSeq: number }[]>();
  private readonly seen = new Map<string, number>();
  private streamSeq = 0;

  constructor(
    private readonly codec: Codec = jsonCodec,
    private readonly opts: {
      /** Optional max_payload, NATS-faithful: an oversized entry rejects with PayloadTooLargeError. */
      maxPayload?: number;
      /** Optional blob store: enables large-payload offload (with `offloadThresholdBytes`). */
      blobs?: BlobStore;
      /** Encoded-entry size above which the payload slot is offloaded (no default here — the pure
       *  double doesn't guess; the NATS shell derives its default from the connection max_payload). */
      offloadThresholdBytes?: number;
      /** Optional payload transform — the same seam the NATS shell exposes as `payloadCodec`. */
      payloadCodec?: PayloadCodec;
    } = {},
  ) {}

  async load(runId: string): Promise<LoadedEntry[]> {
    // Snapshot before the per-entry awaits below: an append landing MID-load must not tear the
    // view (the NATS shell reads a counted batch — same consistency).
    const log = [...(this.logs.get(runId) ?? [])];
    const out: LoadedEntry[] = [];
    for (const l of log) {
      const entry = await resolveEntry(
        runId,
        this.codec.decode(l.data),
        this.codec,
        this.opts.blobs,
        undefined,
        this.opts.payloadCodec,
      );
      out.push({ entry, streamSeq: l.streamSeq });
    }
    return out;
  }

  async append(runId: string, a: JournalAppend): Promise<AppendResult> {
    // Encode FIRST (faithful to the NATS shell): an unencodable entry is a deterministic failure that
    // rejects with SerializationError, never a CAS/dedup outcome.
    // Payload transform first (faithful to the shell): it rewrites the entry, so the bytes stored
    // must be of the rewritten form. Its own error classes propagate unchanged.
    const entry = await encodePayload(runId, a.entry, this.codec, this.opts.payloadCodec);
    let data: Uint8Array;
    try {
      data = this.codec.encode(entry);
    } catch (e) {
      throw new SerializationError(runId, `run ${runId}: cannot encode ${a.entry.kind} entry`, {
        cause: e,
      });
    }
    // Offload BEFORE the max_payload check (faithful: offload exists to keep oversized payloads
    // from ever reaching the publish). The blob write lands even if the CAS below fences — the
    // real blob-before-pointer ordering, which the divergent-racer tests rely on. Error classes
    // pass through untouched: encodeValue failures arrive as SerializationError (deterministic),
    // a blob-store put failure as its raw error (transient infra → the worker retries).
    if (this.opts.blobs && this.opts.offloadThresholdBytes !== undefined) {
      const out = await offloadEntry(
        runId,
        entry,
        data,
        this.codec,
        this.opts.blobs,
        this.opts.offloadThresholdBytes,
      );
      data = out.encoded;
    }
    // An entry over the server's max_payload is a deterministic rejection (faithful to the NATS
    // shell's pre-publish check): PayloadTooLargeError, never a CAS/dedup outcome or a retry.
    if (this.opts.maxPayload !== undefined && data.byteLength > this.opts.maxPayload) {
      throw new PayloadTooLargeError(
        runId,
        `run ${runId}: ${a.entry.kind} entry (seq ${a.entry.seq}) encodes to ${data.byteLength} bytes — ` +
          `over the server max_payload of ${this.opts.maxPayload}`,
      );
    }
    const log = this.logs.get(runId) ?? [];
    const lastSeq = log.at(-1)?.streamSeq ?? 0;
    // CAS dominates dedup (real-NATS faithful): evaluate the optimistic CAS FIRST, so a stale
    // expectLastSeq fences even a duplicate msgID — a concurrent zombie can't slip a write through.
    if (a.expectLastSeq !== lastSeq) {
      throw new FencedError(
        runId,
        `run ${runId}: expected last seq ${a.expectLastSeq}, have ${lastSeq}`,
      );
    }
    const dup = this.seen.get(a.msgID);
    if (dup !== undefined) return { streamSeq: dup, duplicate: true };
    const streamSeq = ++this.streamSeq;
    log.push({ data, streamSeq });
    this.logs.set(runId, log);
    this.seen.set(a.msgID, streamSeq);
    return { streamSeq, duplicate: false };
  }
}
