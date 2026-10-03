// @synadia-ai/durable — the run status index (KV). This is a cache/hint only: the journal is the
// source of truth (invariant #1), so status writes use a plain put (not a progress gate).
// v0.1 has no gating lease — single-delivery via the work-queue plus the journal CAS provides
// single-writer correctness; an explicit KV lease to cut wasted concurrent work is a documented
// next refinement (DESIGN.md §6, invariant #3).

import { type KV } from "@nats-io/kv";
import { decodeJson, encodeJson } from "../../../src/internal/json.js";
import { type BlobStore, storePayloadBlob } from "../../../src/journal/blobs.js";
import { type Codec } from "../../../src/journal/codec.js";
import { type BlobRef } from "../../../src/journal/entry.js";
import { type RunState } from "../../../src/types.js";

export interface RunRecord {
  status: RunState;
  wf: string;
  version: number;
  updatedMs: number;
  result?: unknown;
  /** Offloaded form: `result` was too large for a KV value and lives in the blob store. The hint
   *  stays a hint — `handle.result()` resolves values through the journal, never through this. */
  resultRef?: BlobRef;
  error?: { name: string; message: string };
  /** Offloaded form of `error` (see resultRef). */
  errorRef?: BlobRef;
  /** For status "continued": the next generation's runId (continue-as-new chain). */
  next?: string;
  /** For status "blocked": a human-readable reason (e.g. "waiting for charge v2"). */
  detail?: string;
  /**
   * For status "suspended": when the run's earliest pending wake is due (epoch ms), if it is parked
   * on a timer at all. Observability only — the reconciler reads `DE_PARKED`, not this.
   */
  dueMs?: number;
}

/** The terminal statuses — never downgraded by a best-effort non-terminal hint. */
function isTerminal(status: RunState): boolean {
  return (
    status === "completed" ||
    status === "failed" ||
    status === "continued" ||
    status === "cancelled"
  );
}

export class RunsStore {
  constructor(
    private readonly kv: KV,
    private readonly now: () => number = Date.now,
    /** Large-payload offload for terminal hints (a KV value rides max_payload too). */
    private readonly offload?: { blobs: BlobStore; thresholdBytes: number; codec: Codec },
    /**
     * Set when a `payloadCodec` is configured: the terminal hint then carries NO payload at all.
     *
     * Encrypting the journal while `DE_RUNS` kept the plaintext result would defeat the whole
     * point — the finished value is the most sensitive thing a run produces, and a status-KV read
     * right is the weakest one we hand out (it is the read-only observer's, and the dashboard's).
     * Redacting costs nothing: this record is a HINT, and `handle.result()` resolves values through
     * the journal, never through it. The status/wf/version stay, so the dashboard still shows the
     * shape of the run — it just cannot show data it has no key for.
     */
    private readonly redactPayloads = false,
  ) {}

  /**
   * Shrink an over-threshold record by offloading its result/error to the blob store. The terminal
   * journal append already offloaded the same value with the same codec, so this converges on the
   * SAME content-addressed object (the put dedups) — without it, finalizing a run whose result is
   * near/over max_payload would fail the KV put and NAK-loop the terminal re-drive forever.
   */
  private async shrink(runId: string, record: RunRecord): Promise<RunRecord> {
    if (this.redactPayloads) {
      const out = { ...record };
      delete out.result;
      delete out.error;
      return out;
    }
    if (!this.offload) return record;
    if (encodeJson(record).byteLength <= this.offload.thresholdBytes) return record;
    const out = { ...record };
    if (out.result !== undefined) {
      // `undefined` deliberately: with a codec configured this method returned above with the
      // payload REDACTED, so a blob written here can only ever hold a value that was not going to
      // be transformed anyway. See the redactPayloads branch.
      out.resultRef = (
        await storePayloadBlob(
          runId,
          "the result of the status record",
          out.result,
          this.offload.codec,
          this.offload.blobs,
          undefined,
        )
      ).ref;
      delete out.result;
    } else if (out.error !== undefined) {
      out.errorRef = (
        await storePayloadBlob(
          runId,
          "the error of the status record",
          out.error,
          this.offload.codec,
          this.offload.blobs,
          undefined, // as above: redaction already ran, so this value is deliberately untransformed
        )
      ).ref;
      delete out.error;
    }
    return out;
  }

  async get(runId: string): Promise<RunRecord | null> {
    const e = await this.kv.get(runId);
    return e ? decodeJson<RunRecord>(e.value) : null;
  }

  async set(runId: string, record: RunRecord): Promise<void> {
    await this.kv.put(runId, encodeJson(await this.shrink(runId, record)));
  }

  async markRunning(runId: string, wf: string, version: number): Promise<void> {
    await this.set(runId, { status: "running", wf, version, updatedMs: this.now() });
  }

  /**
   * Set a non-terminal status (`suspended`/`blocked`). Best-effort observability hint: it NEVER
   * downgrades a terminal status (`completed`/`failed`/`continued`/`cancelled`) — the journal is the
   * truth, so a worker that already finalized this run wins. (No gating lease, so a stale read can still
   * race a concurrent finalize; the common path — and a redelivered wake to a finalized run — are
   * correct, and a terminal status is never clobbered.)
   */
  private async markNonTerminal(
    runId: string,
    wf: string,
    version: number,
    status: RunState,
    extra?: { detail?: string; dueMs?: number },
  ): Promise<void> {
    const rec = await this.get(runId);
    if (rec && isTerminal(rec.status)) return;
    await this.set(runId, {
      status,
      wf,
      version,
      updatedMs: this.now(),
      ...(extra?.detail !== undefined ? { detail: extra.detail } : {}),
      ...(extra?.dueMs !== undefined ? { dueMs: extra.dueMs } : {}),
    });
  }

  /**
   * Mark a run as parked on a `sleep`/`await`/`call` (never downgrades a terminal). `dueMs` is the
   * earliest pending wake when the run is parked on a timer — a hint for `status()`; the reconciler
   * that repairs a lost wake reads `DE_PARKED` (see `parked.ts`), never this.
   */
  markSuspended(runId: string, wf: string, version: number, dueMs?: number): Promise<void> {
    return this.markNonTerminal(
      runId,
      wf,
      version,
      "suspended",
      dueMs !== undefined ? { dueMs } : undefined,
    );
  }

  /**
   * Mark a run as BLOCKED: its recorded workflow version isn't registered on any worker, so it is
   * parked (not failed) until that version is deployed. A loud, distinct status (never downgrades a
   * terminal) carrying a reason for `status()` + the UI. See docs/DESIGN-versioning.md §3.2.
   */
  markBlocked(runId: string, wf: string, version: number, detail: string): Promise<void> {
    return this.markNonTerminal(runId, wf, version, "blocked", { detail });
  }

  async finalizeCompleted(
    runId: string,
    wf: string,
    version: number,
    result: unknown,
  ): Promise<void> {
    await this.set(runId, { status: "completed", wf, version, updatedMs: this.now(), result });
  }

  async finalizeFailed(
    runId: string,
    wf: string,
    version: number,
    error: { name: string; message: string },
  ): Promise<void> {
    await this.set(runId, { status: "failed", wf, version, updatedMs: this.now(), error });
  }

  async finalizeCancelled(runId: string, wf: string, version: number): Promise<void> {
    await this.set(runId, { status: "cancelled", wf, version, updatedMs: this.now() });
  }

  /** Mark a generation as continued-as-new, pointing at the next generation (a hint for observers). */
  async markContinued(runId: string, wf: string, version: number, next: string): Promise<void> {
    await this.set(runId, { status: "continued", wf, version, updatedMs: this.now(), next });
  }
}
