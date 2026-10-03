// @synadia-ai/durable — the worker: "The worker's loop" of durable-engine docs/API.md, in
// TypeScript. One `poll` per concurrency slot names every registered workflow with its versions;
// for each item the journal is paged in with `load` while `more`, reversed through the payload codec,
// and replayed by the library's `executeRun` with its three seams as requests under the item's
// lease (Journal → load/append, Scheduler → wake, Children → child), with `extend` on a third of the
// ack wait meanwhile; the `ExecOutcome` goes back as `done`. On `expired` from any request the item
// is dropped and the slot polls on. The worker holds no state across items: the consumers, the
// inbound event, the run lease, the ack matrix, the obligations after a terminal, the cascade, the
// forward, the parked index and the lost-wake repair are the engine's.

import { type NatsConnection } from "@nats-io/nats-core";
import { type ChildStart, type Children } from "../core/children.js";
import { type ExecOutcome, executeRun } from "../core/execute.js";
import { buildRegistry } from "../core/registry.js";
import { type Scheduler } from "../core/scheduler.js";
import { ConnectionClosedError, PayloadCodecError } from "../errors.js";
import { type Logger, SILENT_LOGGER } from "../internal/logger.js";
import {
  type DurableEventFields,
  type DurableEventName,
  type Metrics,
  SILENT_METRICS,
  emitSafe,
} from "../internal/metrics.js";
import { type OffloadOptions } from "../journal/blobs.js";
import { type LoadedEntry } from "../journal/entry.js";
import { type PayloadCodec } from "../journal/payload-codec.js";
import { type AppendResult, type Journal, type JournalAppend } from "../journal/store.js";
import { type AnyWorkflow } from "../types.js";
import { EngineError, InvalidRequestError, LeaseExpiredError } from "./errors.js";
import { DEFAULT_PREFIX, type Op } from "./names.js";
import { reverseJournal, slot, type WireJournal, wireEntry } from "./payload.js";
import { createTransport } from "./transport.js";

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
  /** Optional metrics sink (default: silent): `journal.append`, `journal.load`, `work.dispatch`, `work.done`. */
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
  /** The engine's (offload is `info`'s `offload_threshold`). Ignored. */
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

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** The back-off of a slot whose poll failed: 250 ms doubling to 5 s. */
function pollBackoffMs(failures: number): number {
  return Math.min(5_000, 250 * 2 ** Math.min(Math.max(0, failures - 1), 5));
}

export function createWorker(opts: WorkerOptions): Worker {
  const prefix = opts.prefix ?? DEFAULT_PREFIX;
  const logger = opts.logger ?? SILENT_LOGGER;
  const metrics = opts.metrics ?? SILENT_METRICS;
  const emit = (name: DurableEventName, fields: DurableEventFields): void =>
    emitSafe(metrics, name, fields);
  const request = createTransport(opts.nc, prefix, logger);
  const codec = opts.payloadCodec;
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
  const slots: WorkerHealth["pumps"] = [];
  /** runId of every item held right now, by item. */
  const held = new Map<symbol, string>();
  const inflight = new Set<Promise<void>>();

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

  /** One item, from its journal to its `done`. Never throws. */
  async function processItem(it: PollReply): Promise<void> {
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
    const leased = async <R>(op: Op, body: Record<string, unknown>): Promise<R> => {
      if (dropped !== undefined) throw new LeaseExpiredError(op, `run ${runId}: the item's lease expired; not sent`); // prettier-ignore
      try {
        return await request<R>(op, { ...body, lease });
      } catch (e) {
        if (e instanceof LeaseExpiredError) drop(e);
        throw e;
      }
    };

    async function loadAll(first: WireJournal, more: boolean): Promise<LoadedEntry[]> {
      const wire = [...first];
      while (more) {
        const after = wire.at(-1)?.streamSeq ?? 0;
        const r = await leased<{ journal: WireJournal; more?: true }>("load", { runId, after });
        wire.push(...r.journal);
        more = r.more === true && r.journal.length > 0;
      }
      return reverseJournal(runId, wire, codec);
    }

    const journal: Journal = {
      async load(id: string): Promise<LoadedEntry[]> {
        const t0 = Date.now();
        const out = await loadAll([], true);
        emit("journal.load", { runId: id, entries: out.length, bytes: 0, ms: Date.now() - t0, outcome: "ok" }); // prettier-ignore
        return out;
      },
      async append(id: string, a: JournalAppend): Promise<AppendResult> {
        const t0 = Date.now();
        const done = (outcome: string): void =>
          emit("journal.append", { runId: id, kind: a.entry.kind, seq: a.entry.seq, bytes: 0, ms: Date.now() - t0, outcome }); // prettier-ignore
        try {
          const entry = await wireEntry(id, a.entry, codec);
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
        await leased("child", {
          childRunId: c.childRunId,
          workflow: c.workflow,
          version: c.version,
          ...(await slot("input", c.input, codec, c.childRunId)),
          parent: c.parent,
        });
      },
    };

    let outcome: ExecOutcome | { kind: "retry"; error: unknown } = { kind: "retry", error: undefined };
    let extender: ReturnType<typeof setInterval> | undefined;
    try {
      const loaded = await loadAll(it.journal, it.more === true);
      const wf = registry.resolve(it.workflow, it.version);
      if (wf === undefined) {
        // The engine hands an item only to a poll that names its version; reaching this is a bug.
        logger.error("durable: an item for a version this worker does not run; handing it back", { runId, workflow: it.workflow, version: it.version }); // prettier-ignore
        outcome = { kind: "retry", error: new Error(`${it.workflow} v${it.version} not registered`) }; // prettier-ignore
      } else {
        extender = setInterval(() => {
          leased("extend", { runId }).catch((e: unknown) => {
            if (!(e instanceof LeaseExpiredError)) logger.debug("durable: extend failed", { runId, error: String(e) }); // prettier-ignore
          });
        }, Math.max(100, Math.floor(it.ackWaitMs / 3)));
        extender.unref?.();
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
      if (e instanceof PayloadCodecError) {
        // The journal is readable but its payloads are not, and every cause (no codec, the wrong
        // key, altered bytes) is operational: hand it back (the engine naks with backoff), loud.
        logger.error(
          "durable: payload codec could not decode this run's payloads; parking run — check that " +
            "this worker has the same payloadCodec (and key) as the writer",
          { runId, error: String(e) },
        );
      } else if (!(e instanceof LeaseExpiredError)) {
        logger.warn("durable: work item failed; will redeliver", { runId, error: String(e) });
      }
      outcome = { kind: "retry", error: e };
    } finally {
      if (extender !== undefined) clearInterval(extender);
    }

    let reported: string = outcome.kind;
    if (outcome.kind === "retry" && !(outcome.error instanceof LeaseExpiredError) && dropped === undefined) {
      logger.warn("durable: infra error during run; will retry", { runId, error: String(outcome.error) }); // prettier-ignore
    }
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

  /** Hand an item back unexecuted (a poll answered after stop()): `done retry`, the engine naks it. */
  async function release(it: PollReply): Promise<void> {
    try {
      await request("done", { runId: it.runId, lease: it.lease, outcome: { kind: "retry" } });
    } catch {
      /* the lease lapses at its ack wait and the item redelivers all the same */
    }
  }

  async function pollLoop(i: number): Promise<void> {
    const state = slots[i]!;
    let failures = 0;
    while (running) {
      if (opts.nc.isClosed()) {
        if (running) reportConnectionClosed();
        break;
      }
      let r: PollReply;
      try {
        r = await request<PollReply>("poll", { workflows, waitMs: pollWaitMs }, { waitMs: pollWaitMs }); // prettier-ignore
        failures = 0;
        state.alive = running;
      } catch (e) {
        if (e instanceof ConnectionClosedError || opts.nc.isClosed()) {
          if (running) reportConnectionClosed();
          break;
        }
        failures += 1;
        state.alive = false;
        state.restarts += 1;
        state.lastError = String(e);
        if (e instanceof InvalidRequestError) logger.error("durable: the engine refused this worker's poll", { error: e.message }); // prettier-ignore
        else if (failures === 1 || failures % 20 === 0) logger.warn("durable: poll failed; retrying", { error: String(e) }); // prettier-ignore
        if (running) await delay(pollBackoffMs(failures));
        continue;
      }
      if (r.none === true) continue;
      if (!running) {
        await release(r);
        break;
      }
      const key = Symbol(r.runId);
      held.set(key, r.runId);
      const p = processItem(r);
      inflight.add(p);
      try {
        await p;
      } finally {
        inflight.delete(p);
        held.delete(key);
      }
    }
    state.alive = false;
  }

  return {
    async start(): Promise<void> {
      if (running) return;
      if (opts.nc.isClosed()) throw new ConnectionClosedError("createWorker: the NATS connection is closed"); // prettier-ignore
      try {
        await request("info", {});
      } catch (e) {
        if (e instanceof ConnectionClosedError) throw e;
        // Not fatal: the polls retry until an engine answers, as a library worker rides out an outage.
        if (e instanceof EngineError) logger.warn("durable: no engine answered info; polling until one does", { prefix, error: e.message }); // prettier-ignore
      }
      running = true;
      slots.length = 0;
      for (let i = 0; i < concurrency; i++) {
        slots.push({ stream: `${prefix}.api.v1.poll`, durable: `slot-${i}`, alive: true, restarts: 0 }); // prettier-ignore
        void pollLoop(i);
      }
    },
    async stop(): Promise<void> {
      running = false;
      for (const s of slots) s.alive = false;
      // The items in flight finish (bounded). A poll still waiting in the engine is not
      // abandoned: its loop stays subscribed until the engine answers, and an item it is handed
      // then goes straight back (`done retry`) instead of waiting out its ack wait.
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
