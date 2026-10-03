// @synadia-ai/durable — the metrics/health seam. Injected like Logger (no deps, silent default):
// the library reports observable moments as (name, fields) events and commits to nothing beyond
// that — no counter/gauge/histogram API, no aggregation, no exporter. A sink maps events onto
// whatever metrics system the host app runs (statsd, Prometheus, OTel, a log line).

/**
 * Stable event names. The set grows over time (a new name is a semver-minor change); sinks should
 * ignore names they don't recognize.
 *
 * - `journal.append` — one CAS append attempt. Fields: `runId`, `kind` (entry kind), `seq`,
 *   `bytes`, `ms`, `outcome`: `ok` | `duplicate` | `fenced` | `payload-too-large` |
 *   `serialization` | `error`. Fences and payload rejections are counted here.
 * - `journal.load` — one full journal read (every replay starts with one). Fields: `runId`,
 *   `entries`, `bytes`, `ms`, `outcome`: `ok` | `short-read`.
 * - `work.dispatch` — a work item entered this worker's local queue. Fields: `workflow`, `runId`,
 *   `kind` (work-item kind), `inflight` (local dispatched-but-unfinished depth).
 * - `work.done` — a work item finished processing. Fields: `workflow`, `runId`, `kind`,
 *   `outcome` (`completed` | `failed` | `cancelled` | `continued` | `suspended` | `fenced` |
 *   `retry` | `parked` | `lease-held` | `terminal-redrive` | `cancel-tombstoned` |
 *   `retry-tombstone-race` | `retry-no-journal` | `dropped-no-journal` | `error` — the NAK/ack
 *   reason), `ms` (execution), `waitMs` (local queue wait), `deliveryCount`.
 * - `pump.restart` — a consumer pump failed and is recreating its consumer (supervision).
 *   Fields: `stream`, `durable`, `restarts`, `error`.
 * - `maintenance.purge` — a timer-wake hygiene pass purged consumed wakes. Fields: `workflow`,
 *   `purged`.
 * - `maintenance.headroom` — a gauge, emitted once per hygiene pass, for the one stream that
 *   cannot be `discard: new` and therefore evicts rather than rejects when full (`DE_TIMERS`; an
 *   evicted pending schedule is a run that never wakes). Fields: `stream`, `bytes`, `maxBytes`,
 *   `usedPct`. Alert on it — past ~75 % the worker also logs a warning.
 * - `maintenance.reconcile` — one sweep of the parked-timer index for runs whose wake never
 *   arrived (the repair for an evicted/lost schedule; see `parked.ts`). Fields: `overdue` (markers
 *   past their due time + grace), `redriven` (runs re-driven), `executing` (skipped: a live lease
 *   says a worker is on it). A non-zero `redriven` means wakes were genuinely lost — pair it with
 *   `maintenance.headroom` to find out why.
 * - `lease.gate` — a gating-lease arbitration before executing a work item. Fields: `runId`,
 *   `outcome`: `acquired` | `reentrant` | `takeover` | `held` (+ `retryMs`) | `fail-open` |
 *   `budget-exhausted` (the holder hit `maxWorkMs` and let the lease lapse so a peer can take over).
 * - `blob.put` — one offloaded-payload write to the blob object store. Fields: `name`, `bytes`,
 *   `ms`, `outcome`: `ok` | `exists` (content-addressed dedup skipped the upload) | `error`.
 * - `blob.get` — one blob fetch during journal resolution (cache hits don't reach the store and
 *   are not counted). Fields: `name`, `bytes`, `ms`, `outcome`: `ok` | `missing` | `error`.
 */
export type DurableEventName =
  | "journal.append"
  | "journal.load"
  | "work.dispatch"
  | "work.done"
  | "pump.restart"
  | "maintenance.purge"
  | "maintenance.headroom"
  | "maintenance.reconcile"
  | "lease.gate"
  | "blob.put"
  | "blob.get";

export type DurableEventFields = Readonly<Record<string, string | number | boolean>>;

/** The injectable sink. Implementations must be cheap and non-blocking; they are called inline. */
export interface Metrics {
  onEvent(name: DurableEventName, fields: DurableEventFields): void;
}

export const SILENT_METRICS: Metrics = Object.freeze({
  onEvent() {},
});

/** Emit through a guard: a throwing/buggy user sink must NEVER break engine progress. */
export function emitSafe(m: Metrics, name: DurableEventName, fields: DurableEventFields): void {
  try {
    m.onEvent(name, fields);
  } catch {
    /* a metrics sink is observability, never control flow */
  }
}
