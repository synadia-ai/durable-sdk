// @synadia-ai/durable — the worker: "The worker's loop" of durable-engine docs/API.md, in
// TypeScript. One `poll` per concurrency slot names every registered workflow with its versions;
// for each item the journal is paged in with `load` while `more`, reversed through the payload codec,
// and replayed by the library's `executeRun` with its three seams as requests under the item's
// lease (Journal → load/append, Scheduler → wake, Children → child), with `extend` on a third of the
// ack wait meanwhile; the `ExecOutcome` goes back as `done`. On `expired` from any request the item
// is dropped and the slot polls on. A value too large for a request or a reply goes up and comes
// down in parts (blobs.ts). Every poll names this worker, and stop() withdraws its waiting polls
// (`poll {cancelPoll}`), so a stop takes as long as the items in flight, not a poll's wait. A poll
// refused at a limit (`LimitError`) is logged at error and backs off; the first one, at start(),
// rejects start() so a process does not run without work silently. The
// worker holds no state across items: the consumers, the inbound event, the run lease, the ack
// matrix, the obligations after a terminal, the cascade, the forward, the parked index and the
// lost-wake repair are the engine's.

import { type NatsConnection } from "@nats-io/nats-core";
import { type ChildStart, type Children } from "../core/children.js";
import { type ExecOutcome, executeRun } from "../core/execute.js";
import { buildRegistry } from "../core/registry.js";
import { type Scheduler } from "../core/scheduler.js";
import {
  BlobIntegrityError,
  BlobMissingError,
  ConnectionClosedError,
  PayloadCodecError,
} from "../errors.js";
import { type Logger, SILENT_LOGGER } from "../internal/logger.js";
import {
  type DurableEventFields,
  type DurableEventName,
  type Metrics,
  SILENT_METRICS,
  emitSafe,
} from "../internal/metrics.js";
import { BlobBytesCache, DEFAULT_BLOB_CACHE_BYTES, type OffloadOptions } from "../journal/blobs.js";
import { jsonCodec } from "../journal/codec.js";
import { type LoadedEntry } from "../journal/entry.js";
import { type PayloadCodec } from "../journal/payload-codec.js";
import { type AppendResult, type Journal, type JournalAppend } from "../journal/store.js";
import { type AnyWorkflow } from "../types.js";
import { EngineBlobs, sizesOf } from "./blobs.js";
import { EngineError, InvalidRequestError, LeaseExpiredError, LimitError } from "./errors.js";
import { DEFAULT_PREFIX, type Op } from "./names.js";
import { fitEntry, fitSlot, reverseJournal, type WireJournal, wireEntry } from "./payload.js";
import { createTransport, type RequestOptions } from "./transport.js";

/** How long one `poll` asks the engine to wait (API.md's loop: 30 s; the engine caps it). */
const POLL_WAIT_MS = 30_000;

/** How long stop() waits for the items in flight before it returns anyway. */
const STOP_DRAIN_MS = 30_000;

export interface WorkerOptions {
  nc: NatsConnection;
  workflows: AnyWorkflow[];
  /** The engine's prefix (default `de`): requests go to `{prefix}.api.v1.<op>`. */
  prefix?: string;
  /** Resolved by `ctx.get(key)` in a workflow (a connection, an http client, config, …). */
  dependencies?: Record<string, unknown>;
  /**
   * Max work items this worker executes at once (default 16): one `poll` per slot. Items of one
   * run never overlap: the engine hands a run's items one at a time.
   */
  concurrency?: number;
  /** How long one `poll` waits for an item (default 30 000 ms; the engine caps it at `max_poll_wait_ms`). */
  pollWaitMs?: number;
  logger?: Logger;
  /**
   * Optional metrics sink (default: silent): `journal.append` (the size of the entry sent),
   * `journal.load` (one per item: the journal it replays from, the poll's and its pages),
   * `work.dispatch`, `work.done`, and `blob.put` / `blob.get` for a value too large for a request
   * or a reply. The Python worker emits the same.
   */
  metrics?: Metrics;
  /** Payload encryption at rest: the same codec on every worker and client of a prefix. */
  payloadCodec?: PayloadCodec;

  // The library's options that are the engine's now (durable-engine serve's flags). Accepted so a
  // library worker's configuration still compiles; ignored here.
  /** The engine's: `serve --ack-wait`. Ignored. */
  ackWaitMs?: number;
  /** The engine's: `serve --max-work`. Ignored. */
  maxWorkMs?: number;
  /** The engine's: `serve --no-lease`. Ignored. */
  lease?: boolean;
  /** The engine's: `serve --lease-ttl`. Ignored. */
  leaseTtlMs?: number;
  /** The engine's: `serve --stream-max-bytes`. Ignored. */
  streamMaxBytes?: number;
  /** The engine's (a version no poll names parks `blocked`, 5 s). Ignored. */
  versionWaitMs?: number;
  /** The engine's: `serve --maintenance-interval`. Ignored. */
  maintenanceIntervalMs?: number;
  /** The engine's (the lost-wake repair is always on). Ignored. */
  reconcileTimers?: boolean;
  /** The engine's: `serve --reconcile-grace`. Ignored. */
  reconcileGraceMs?: number;
  /**
   * Its tuning is the engine's (`info`'s `offload_threshold`, `serve --offload-threshold`) and
   * ignored; `false` keeps the library's meaning: no value goes up in `put` parts, so one too
   * large for a request fails the run loud (`PayloadTooLargeError`).
   */
  offload?: OffloadOptions;
}

/** A point-in-time worker health snapshot (see `Worker.health()`). */
export interface WorkerHealth {
  /** start() called, stop() not yet, and the connection not closed for good. */
  running: boolean;
  /** The caller's NATS connection has CLOSED for good: this worker will never receive work again. */
  connectionClosed: boolean;
  /** Work items held and not yet finished. */
  inflight: number;
  /** Distinct runs among them. */
  queuedRuns: number;
  /**
   * One entry per poll slot (the library's consumer pumps are the engine's now). `alive` means the
   * slot's last poll was answered and it is polling again; a slot that is backing off after a
   * failed poll is not alive, and `restarts` counts those failures.
   */
  pumps: Array<{
    stream: string;
    durable: string;
    alive: boolean;
    restarts: number;
    lastError?: string;
  }>;
}

export interface Worker {
  start(): Promise<void>;
  stop(): Promise<void>;
  /** Cheap, synchronous, side-effect-free health snapshot (for readiness/liveness endpoints). */
  health(): WorkerHealth;
}

/** A `poll` reply: `{none: true}`, or an item under a lease. */
interface PollReply {
  none?: true;
  lease: string;
  runId: string;
  workflow: string;
  version: number;
  item?: { kind?: string };
  attempt: number;
  journal: WireJournal;
  more?: true;
  ackWaitMs: number;
  workDeadlineMs: number;
}

/** The back-off of a slot whose poll failed: 250 ms doubling to 5 s. */
function pollBackoffMs(failures: number): number {
  return Math.min(5_000, 250 * 2 ** Math.min(Math.max(0, failures - 1), 5));
}

/**
 * The back-off of a slot whose poll was refused at a limit (API.md "Errors"): `rate` clears within
 * a second; the others when runs end, a worker goes away or an operator acts, so 30 s.
 */
export function limitBackoffMs(e: LimitError): number {
  return e.limit === "rate" ? 1_000 : 30_000;
}

/** How often the worker says that its polls are failing (one line, with the count). */
export const POLL_WARN_EVERY_MS = 60_000;

/**
 * One log line per `everyMs` for an event that repeats: `hit()` counts one and answers how many
 * the line about to be logged stands for (this one and those since the last line), or 0 to stay
 * quiet. A long idle over a slow link, every poll failing, then fills no log (F41).
 */
export class EveryLog {
  #last = -Infinity;
  #count = 0;
  constructor(
    readonly everyMs: number,
    readonly now: () => number = Date.now,
  ) {}
  hit(): number {
    this.#count += 1;
    const t = this.now();
    if (t - this.#last < this.everyMs) return 0;
    this.#last = t;
    const n = this.#count;
    this.#count = 0;
    return n;
  }
}

export function createWorker(opts: WorkerOptions): Worker {
  const prefix = opts.prefix ?? DEFAULT_PREFIX;
  const logger = opts.logger ?? SILENT_LOGGER;
  const metrics = opts.metrics ?? SILENT_METRICS;
  const emit = (name: DurableEventName, fields: DurableEventFields): void =>
    emitSafe(metrics, name, fields);
  const request = createTransport(opts.nc, prefix, logger);
  const codec = opts.payloadCodec;
  const sizes = sizesOf(request, () => opts.nc.info?.max_payload);
  /** `offload: false` keeps the library's meaning: nothing goes up in parts, a value too large fails loud. */
  const fit = opts.offload === false ? undefined : sizes;
  /** Resolved blobs, by name (content-addressed, so never stale), as the library's worker kept. */
  const blobCache = new BlobBytesCache(DEFAULT_BLOB_CACHE_BYTES);
  // name → version → workflow. Throws on a duplicate (name, version): fail loud at construction.
  const registry = buildRegistry(opts.workflows);
  const workflows = registry.names().map((name) => ({
    name,
    versions: opts.workflows
      .filter((w) => w.name === name)
      .map((w) => w.version)
      .sort((a, b) => a - b),
  }));
  const concurrency = Math.max(1, Math.floor(opts.concurrency ?? 16));
  const pollWaitMs = Math.max(0, opts.pollWaitMs ?? POLL_WAIT_MS);

  let running = false;
  let closedReported = false;
  /** The slots backing off: stop() wakes them at once. */
  const napping = new Set<() => void>();
  /** A sleep that stop() cuts short. */
  const nap = (ms: number): Promise<void> =>
    new Promise((r) => {
      const wake = (): void => {
        clearTimeout(t);
        napping.delete(wake);
        r();
      };
      const t = setTimeout(wake, ms);
      napping.add(wake);
    });
  /** The worker's "poll failed; retrying": one line a minute, however many slots fail. */
  const pollWarn = new EveryLog(POLL_WARN_EVERY_MS);
  /** A refusal at a limit: one error line per limit per back-off, however many slots it refuses. */
  const limitLogs = new Map<string, EveryLog>();
  function logLimit(e: LimitError, where: string, fields: Record<string, unknown> = {}): void {
    const key = `${where}|${e.code}|${e.limit}`;
    let l = limitLogs.get(key);
    if (l === undefined) limitLogs.set(key, (l = new EveryLog(limitBackoffMs(e))));
    const n = l.hit();
    const then = where === "poll" ? `polling again in ${String(limitBackoffMs(e) / 1000)} s` : "the item goes back until it clears"; // prettier-ignore
    if (n > 0) logger.error(`durable: the engine refused this worker's ${where} at a limit (${e.code} ${e.limit}); ${then}`, { ...fields, code: e.code, limit: e.limit, refused: n, error: e.message }); // prettier-ignore
  }
  /** This worker's name on its polls, so that stop() withdraws its polls and no one else's. */
  const workerId = globalThis.crypto.randomUUID();
  const slots: WorkerHealth["pumps"] = [];
  /** runId of every item held right now, by item. */
  const held = new Map<symbol, string>();
  const inflight = new Set<Promise<void>>();
  /** The tail of each run's items executing on this worker (removed when it settles). */
  const runChains = new Map<string, Promise<void>>();

  function reportConnectionClosed(): void {
    if (closedReported) return;
    closedReported = true;
    logger.error(
      "durable: the NATS connection is CLOSED for good — this worker will never receive work again " +
        "and cannot recover on its own (the connection is the caller's). Restart it on a new " +
        "connection, and connect with maxReconnectAttempts: -1 (withReconnectDefaults) so an " +
        "outage is ridden out instead of given up on. Runs are unaffected: other workers, or this " +
        "one restarted, resume them from the journal.",
    );
  }

  /**
   * One item, from its journal to its `done`. Never throws. `before` is the run's previous item
   * on this worker, still executing (a redelivery after its lease ended): this one waits for it,
   * its lease kept live meanwhile, so one process never executes a run twice at once.
   */
  async function processItem(it: PollReply, before: Promise<void> | undefined): Promise<void> {
    const { runId, lease } = it;
    const kind = it.item?.kind ?? "";
    const startedAt = Date.now();
    emit("work.dispatch", { workflow: it.workflow, runId, kind, inflight: held.size });
    let dropped: LeaseExpiredError | undefined;
    const drop = (e: LeaseExpiredError): void => {
      if (dropped !== undefined) return;
      dropped = e;
      logger.warn("durable: the item's lease expired; dropping it (it redelivers to a poll)", {
        runId,
        error: e.message,
      });
    };
    // Every request for this item carries its lease; after an `expired` nothing more is sent.
    const leased = async <R>(
      op: Op,
      body: Record<string, unknown>,
      o?: RequestOptions,
    ): Promise<R> => {
      if (dropped !== undefined) throw new LeaseExpiredError(op, `run ${runId}: the item's lease expired; not sent`); // prettier-ignore
      try {
        return await request<R>(op, { ...body, lease }, o);
      } catch (e) {
        if (e instanceof LeaseExpiredError) drop(e);
        throw e;
      }
    };

    // A part of this run's blob carries the lease; another run's (a child's input) does not.
    const blobs = new EngineBlobs({
      request: (op, body, o) => ("lease" in body ? leased(op, body as Record<string, unknown>, o) : request(op, body, o)), // prettier-ignore
      sizes,
      lease: { runId, lease },
      rtt: () => opts.nc.rtt(),
      emit,
    });

    /**
     * The run's journal: `first` (the poll's), then `load` pages while `more`, every ref fetched
     * and every `<slot>Enc` reversed. One `journal.load` per call: every replay starts with one,
     * as the library's, and `bytes` is the journal as the engine handed it.
     */
    async function loadAll(first: WireJournal, more: boolean): Promise<LoadedEntry[]> {
      const t0 = Date.now();
      const wire = [...first];
      while (more) {
        const after = wire.at(-1)?.streamSeq ?? 0;
        const r = await leased<{ journal: WireJournal; more?: true }>("load", { runId, after });
        wire.push(...r.journal);
        more = r.more === true && r.journal.length > 0;
      }
      const out = await reverseJournal(runId, wire, codec, blobs, blobCache);
      const bytes = wire.reduce((n, l) => n + jsonCodec.encode(l.entry).byteLength, 0);
      emit("journal.load", { runId, entries: out.length, bytes, ms: Date.now() - t0, outcome: "ok" }); // prettier-ignore
      return out;
    }

    const journal: Journal = {
      async load(_id: string): Promise<LoadedEntry[]> {
        return loadAll([], true);
      },
      async append(id: string, a: JournalAppend): Promise<AppendResult> {
        const t0 = Date.now();
        let bytes = 0;
        const done = (outcome: string): void =>
          emit("journal.append", { runId: id, kind: a.entry.kind, seq: a.entry.seq, bytes, ms: Date.now() - t0, outcome }); // prettier-ignore
        try {
          const envelope = { runId: id, msgID: a.msgID, expectLastSeq: a.expectLastSeq, lease };
          const entry = await fitEntry(id, await wireEntry(id, a.entry, codec), envelope, fit, blobs); // prettier-ignore
          bytes = jsonCodec.encode(entry).byteLength;
          const r = await leased<AppendResult>("append", { runId: id, entry, msgID: a.msgID, expectLastSeq: a.expectLastSeq }); // prettier-ignore
          done(r.duplicate ? "duplicate" : "ok");
          return { streamSeq: r.streamSeq, duplicate: r.duplicate === true };
        } catch (e) {
          done(outcomeOfAppendError(e));
          throw e;
        }
      },
    };
    const scheduler: Scheduler = {
      async scheduleWake(id: string, wf: string, seq: number, dueMs: number): Promise<void> {
        await leased("wake", { runId: id, wf, seq, dueMs });
      },
    };
    const children: Children = {
      async start(c: ChildStart): Promise<void> {
        const head = { childRunId: c.childRunId, workflow: c.workflow, version: c.version, parent: c.parent, lease }; // prettier-ignore
        await leased("child", {
          childRunId: c.childRunId,
          workflow: c.workflow,
          version: c.version,
          // A child's input too large for the request is `put` under the CHILD's run.
          ...(await fitSlot("input", c.input, codec, c.childRunId, head, fit, blobs)),
          parent: c.parent,
        });
      },
    };

    let outcome: ExecOutcome | { kind: "retry"; error: unknown } = {
      kind: "retry",
      error: undefined,
    };
    /** The library's `work.done` outcome when it is not the ExecOutcome's kind. */
    let label: string | undefined;
    // Extend from dispatch: the wait behind `before` and the journal's pages count too.
    const extender = setInterval(
      () => {
        leased("extend", { runId }).catch((e: unknown) => {
          if (!(e instanceof LeaseExpiredError)) logger.debug("durable: extend failed", { runId, error: String(e) }); // prettier-ignore
        });
      },
      Math.max(100, Math.floor(it.ackWaitMs / 3)),
    );
    extender.unref?.();
    try {
      if (before !== undefined) await before;
      const loaded = await loadAll(it.journal, it.more === true);
      const wf = registry.resolve(it.workflow, it.version);
      if (wf === undefined) {
        // The engine hands an item only to a poll that names its version; reaching this is a bug.
        logger.error("durable: an item for a version this worker does not run; handing it back", { runId, workflow: it.workflow, version: it.version }); // prettier-ignore
        outcome = { kind: "retry", error: new Error(`${it.workflow} v${it.version} not registered`) }; // prettier-ignore
      } else {
        outcome = await executeRun(runId, {
          workflow: wf,
          journal,
          loaded,
          scheduler,
          children,
          dependencies: opts.dependencies ?? {},
          attempt: it.attempt,
        });
      }
    } catch (e) {
      if (e instanceof BlobMissingError || e instanceof BlobIntegrityError) {
        label = parkOnBlob(runId, e);
      } else if (e instanceof PayloadCodecError) {
        label = "payload-codec";
        // The journal is readable but its payloads are not, and every cause (no codec, the wrong
        // key, altered bytes) is operational: hand it back (the engine naks with backoff), loud.
        logger.error(
          "durable: payload codec could not decode this run's payloads; parking run — check that " +
            "this worker has the same payloadCodec (and key) as the writer",
          { runId, error: String(e) },
        );
      } else if (e instanceof LimitError) {
        logLimit(e, "request", { runId });
      } else if (!(e instanceof LeaseExpiredError)) {
        logger.warn("durable: work item failed; will redeliver", { runId, error: String(e) });
      }
      outcome = { kind: "retry", error: e };
    } finally {
      clearInterval(extender);
    }

    // A blob the engine said is gone (`blob_gone`) under a step-end `load` or a ref's `get` inside
    // the execution: the pure core halted with `retry` on it, as on any infrastructure error; park
    // it as the library did, with its label and its loud line.
    if (
      label === undefined &&
      outcome.kind === "retry" &&
      (outcome.error instanceof BlobMissingError || outcome.error instanceof BlobIntegrityError)
    ) {
      label = parkOnBlob(runId, outcome.error);
    }
    let reported: string = label ?? outcome.kind;
    if (
      outcome.kind === "retry" &&
      label === undefined &&
      !(outcome.error instanceof LeaseExpiredError) &&
      dropped === undefined
    ) {
      // Refused at a limit inside the execution (a child at `runs`, an append at `bytes`): the
      // item goes back and the run waits until it clears (API.md "Errors"), loud.
      if (outcome.error instanceof LimitError) {
        label = "limit";
        logLimit(outcome.error, "request", { runId });
      } else logger.warn("durable: infra error during run; will retry", { runId, error: String(outcome.error) }); // prettier-ignore
    }
    if (label === "limit") reported = "limit";
    if (dropped !== undefined) {
      reported = "expired";
    } else {
      try {
        await request("done", { runId, lease, outcome: wireOutcome(outcome) });
      } catch (e) {
        if (e instanceof LeaseExpiredError) drop(e);
        else logger.warn("durable: done failed; the item redelivers at its ack wait", { runId, error: String(e) }); // prettier-ignore
        reported = dropped !== undefined ? "expired" : "error";
      }
    }
    emit("work.done", { workflow: it.workflow, runId, kind, outcome: reported, ms: Date.now() - startedAt, waitMs: 0, deliveryCount: it.attempt }); // prettier-ignore
  }

  /**
   * An offloaded payload that cannot be resolved (gone, or its digest wrong) makes the journal
   * unreadable: operational, so hand the item back (the engine naks with backoff and parks the run
   * too), loud, and never a terminal. It heals when the blob is restored, or the run is cancelled.
   */
  function parkOnBlob(runId: string, e: BlobMissingError | BlobIntegrityError): string {
    logger.error("durable: offloaded payload unresolvable; parking run — restore the blob or cancel the run", { runId, blob: e.blobName, error: String(e) }); // prettier-ignore
    return "blob-unresolvable";
  }

  /** Hand an item back unexecuted (a poll answered after stop()): `done retry`, the engine naks it. */
  async function release(it: PollReply): Promise<void> {
    try {
      await request("done", { runId: it.runId, lease: it.lease, outcome: { kind: "retry" } });
    } catch {
      /* the lease lapses at its ack wait and the item redelivers all the same */
    }
  }

  async function pollLoop(i: number, first: PollReply | undefined): Promise<void> {
    const state = slots[i]!;
    let failures = 0;
    while (running) {
      if (opts.nc.isClosed()) {
        if (running) reportConnectionClosed();
        break;
      }
      let r: PollReply;
      try {
        if (first !== undefined) {
          // The item start()'s first poll was handed.
          r = first;
          first = undefined;
        } else {
          r = await request<PollReply>("poll", { workflows, waitMs: pollWaitMs, worker: workerId }, { waitMs: pollWaitMs }); // prettier-ignore
        }
        failures = 0;
        state.alive = running;
      } catch (e) {
        if (e instanceof ConnectionClosedError || opts.nc.isClosed()) {
          if (running) reportConnectionClosed();
          break;
        }
        if (e instanceof BlobMissingError || e instanceof BlobIntegrityError) {
          // The engine parked the run whose item this poll was to be handed (its journal met a gone
          // blob at the hand-off, API.md `poll`): the run's, not this worker's. Loud, and poll again.
          logger.error("durable: the engine parked a run whose blob is gone — restore the blob or cancel the run", { runId: e.runId, blob: e.blobName, error: e.message }); // prettier-ignore
          continue;
        }
        failures += 1;
        state.alive = false;
        state.restarts += 1;
        state.lastError = String(e);
        if (e instanceof LimitError) {
          // Refused at a limit (the engine's F46): never a broker blip. Loud, and polled again
          // only after the limit could have cleared.
          logLimit(e, "poll", { workflows: workflows.map((w) => w.name) });
          if (running) await nap(limitBackoffMs(e));
          continue;
        }
        if (e instanceof InvalidRequestError) {
          logger.error("durable: the engine refused this worker's poll", { error: e.message });
        } else {
          const n = pollWarn.hit();
          if (n > 0) logger.warn("durable: poll failed; retrying", { error: String(e), failures: n, everyMs: POLL_WARN_EVERY_MS }); // prettier-ignore
        }
        if (running) await nap(pollBackoffMs(failures));
        continue;
      }
      if (r.none === true) continue;
      if (!running) {
        await release(r);
        break;
      }
      const key = Symbol(r.runId);
      held.set(key, r.runId);
      const p = processItem(r, runChains.get(r.runId));
      runChains.set(r.runId, p);
      inflight.add(p);
      try {
        await p;
      } finally {
        inflight.delete(p);
        held.delete(key);
        if (runChains.get(r.runId) === p) runChains.delete(r.runId);
      }
    }
    state.alive = false;
  }

  return {
    async start(): Promise<void> {
      if (running) return;
      if (opts.nc.isClosed()) throw new ConnectionClosedError("createWorker: the NATS connection is closed"); // prettier-ignore
      // A refusal at a limit (but `rate`, which clears within a second) rejects start(): a
      // process must not run without work silently (the engine's F46).
      const refused = (e: unknown, where: string): e is LimitError => {
        if (!(e instanceof LimitError) || e.limit === "rate") return false;
        logger.error(`durable: the engine refused this worker's ${where} at a limit (${e.code} ${e.limit}); start() rejects`, { code: e.code, limit: e.limit, workflows: workflows.map((w) => w.name), error: e.message }); // prettier-ignore
        return true;
      };
      let reachable = true;
      try {
        await request("info", {});
      } catch (e) {
        if (e instanceof ConnectionClosedError || refused(e, "info")) throw e;
        reachable = false;
        // Not fatal: the polls retry until an engine answers, as a library worker rides out an outage.
        if (e instanceof EngineError) logger.warn("durable: no engine answered info; polling until one does", { prefix, error: e.message }); // prettier-ignore
      }
      // The first poll checks once (`waitMs: 0`) whether the engine serves these workflows to this
      // worker; an item it is handed goes to the first slot.
      let first: PollReply | undefined;
      if (reachable) {
        try {
          const r = await request<PollReply>("poll", { workflows, waitMs: 0, worker: workerId }, { waitMs: 0 }); // prettier-ignore
          if (r.none !== true) first = r;
        } catch (e) {
          if (e instanceof ConnectionClosedError || refused(e, "first poll")) throw e;
          // Anything else the slots meet again and retry, as before.
        }
      }
      running = true;
      slots.length = 0;
      for (let i = 0; i < concurrency; i++) {
        slots.push({ stream: `${prefix}.api.v1.poll`, durable: `slot-${i}`, alive: true, restarts: 0 }); // prettier-ignore
        void pollLoop(i, i === 0 ? first : undefined);
      }
    },
    async stop(): Promise<void> {
      const wasRunning = running;
      running = false;
      for (const wake of [...napping]) wake();
      for (const s of slots) s.alive = false;
      // Withdraw this worker's polls still waiting in the engine (API.md "Withdrawing polls"):
      // every instance answers them `none` at once and never hands them an item. An item handed in
      // the instant before goes straight back (`done retry`). Without an engine to answer, the
      // polls end at their own wait, as before.
      if (wasRunning && !opts.nc.isClosed()) {
        const withdraw = { workflows: [], worker: workerId, cancelPoll: true };
        await request("poll", withdraw).catch((e: unknown) => {
          logger.debug("durable: withdrawing the polls failed; they end at their wait", { error: String(e) }); // prettier-ignore
        });
      }
      // The items in flight finish (bounded).
      let t: ReturnType<typeof setTimeout> | undefined;
      const bound = new Promise<"bound">((r) => {
        t = setTimeout(() => r("bound"), STOP_DRAIN_MS);
        t.unref?.();
      });
      const r = await Promise.race([Promise.allSettled([...inflight]), bound]);
      clearTimeout(t);
      if (r === "bound") logger.warn("durable: stop() returned with items still in flight", { inflight: inflight.size }); // prettier-ignore
    },
    health(): WorkerHealth {
      const connectionClosed = opts.nc.isClosed();
      return {
        running: running && !connectionClosed,
        connectionClosed,
        inflight: held.size,
        queuedRuns: new Set(held.values()).size,
        pumps: slots.map((p) => ({ ...p, alive: p.alive && running && !connectionClosed })),
      };
    },
  };
}

/** The `done` outcome: its kind (the engine acts on the journal's terminal, never on a value). */
function wireOutcome(o: ExecOutcome | { kind: "retry"; error: unknown }): Record<string, unknown> {
  if (o.kind === "retry") {
    const e = o.error;
    return e instanceof Error ? { kind: "retry", error: { name: e.name, message: e.message } } : { kind: "retry" }; // prettier-ignore
  }
  return { kind: o.kind };
}

function outcomeOfAppendError(e: unknown): string {
  if (!(e instanceof Error)) return "error";
  if (e.name === "FencedError") return "fenced";
  if (e.name === "PayloadTooLargeError") return "payload-too-large";
  if (e.name === "SerializationError") return "serialization";
  if (e.name === "PayloadCodecError") return "payload-codec";
  return "error";
}
