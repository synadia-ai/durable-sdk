// The simulated world: ONE in-memory journal, a work queue, timers, a client, and several workers
// whose every interaction with any of them passes through a GATE. The world releases one gate at a
// time, chosen by a seeded PRNG, and lets the microtask queue run dry in between — so the whole
// interleaving of several concurrent `executeRun`s is a pure function of the seed, and a failing
// seed replays exactly.
//
// What is REAL here: `executeRun` + `CtxImpl` (the replay engine), `recordInbound` (the inbound-event
// recording, PROTOCOL §6.4), `InMemoryJournal` (CAS dominates dedup). What is a MODEL: the queue, the
// timers, the client, and the worker's outcome handling (`processOne` below mirrors
// `src/nats/worker.ts` minus lease, registry, blobs and KV hints — none of which may gate
// correctness, which is exactly why the simulation runs without them).
//
// Faults the world injects, each against a budget: an infrastructure error before an operation
// lands; a LOST ACK (the append lands, the caller sees an error); a crash (an incarnation is
// abandoned mid-flight, every gate it holds is dropped, its work item is redelivered); a duplicate
// delivery (an item redelivered while its first incarnation is still running — a zombie).

import { type ChildStart, type Children } from "../../../src/core/children.js";
import { type ExecOutcome, executeRun } from "../../../src/core/execute.js";
import { hasResolve, recordInbound } from "../../../src/core/inbound.js";
import { type Scheduler } from "../../../src/core/scheduler.js";
import { type WorkItem } from "../../../src/core/work-item.js";
import { FencedError } from "../../../src/errors.js";
import {
  type ContinuedEntry,
  type JournalEntry,
  type LoadedEntry,
  type StartEntry,
  msgId,
  stamp,
} from "../../../src/journal/entry.js";
import { InMemoryJournal, type Journal } from "../../../src/journal/store.js";
import { type AnyWorkflow } from "../../../src/types.js";
import {
  type Program,
  type SimHooks,
  type SimInput,
  type SimResult,
  SimStepError,
  buildWorkflows,
  programIndex,
  promisedSignals,
  workflowName,
} from "./program.js";
import { Rng } from "./rng.js";

export interface Faults {
  crashes: number;
  redeliveries: number;
  infraErrors: number;
  lostAcks: number;
}

export interface SimCase {
  programs: Program[];
  /** Seeds the interleaving and the faults — independent of the seed that drew the programs. */
  schedSeed: number;
  workers: number;
  faults: Faults;
  /** msgID dedup on the journal. OFF models an expired duplicate window: the CAS must suffice. */
  journalDedup: boolean;
  /** msgID dedup on the work queue and the timers. OFF → duplicates arrive; content dedup must hold. */
  queueDedup: boolean;
  /** The client cancels the root run at some point. */
  cancelRoot: boolean;
}

/** The pieces of the engine a control may swap for a deliberately broken one. */
export interface SimOptions {
  recordInbound?: typeof recordInbound;
  /**
   * BREAKS the Journal contract on purpose: a load may return a stale prefix — what a
   * follower-served read would look like without the shell's completeness guard.
   */
  staleReads?: boolean;
  /**
   * A CANCEL journals its request AHEAD of its run's chain on the worker driving that run
   * (`dispatch` in src/nats/worker.ts; PROTOCOL §11.7 step 2). Default on; `false` is the model of
   * the worker before the fabric test repo's F23, where the request waited behind the execution.
   */
  cancelAhead?: boolean;
  maxSteps?: number;
}

export interface SimStats {
  steps: number;
  runs: number;
  incarnations: number;
  fences: number;
  crashes: number;
  redeliveries: number;
  infraErrors: number;
  lostAcks: number;
  effects: number;
  /** Firings of a (run, seq) effect beyond the first — the at-least-once window, observed. */
  repeatedEffects: number;
  /** Executions that ended by ADOPTING another incarnation's terminal. */
  terminalRaces: number;
  eventRetries: number;
  /** A cancel journaled AHEAD of its run's chain while that worker was still executing the run. */
  cancelsAhead: number;
}

export interface SimReport {
  violations: string[];
  /**
   * Hits on a KNOWN, OPEN finding — tolerated so that everything else stays strict, and counted so
   * that the tolerance cannot outlive the finding unnoticed. Delete the tolerance with the fix.
   */
  known: string[];
  trace: string[];
  stats: SimStats;
}

const SHORT_DEDUP_WINDOW = 150;
const ROOT = "root";
const TERMINAL_KINDS = new Set(["result", "fail", "continued", "cancelled"]);
/** The only failures a generated program can legitimately end in. */
const EXPECTED_FAILURES = new Set(["SimStepError", "WorkflowFailure", "DurableTimeoutError"]);

const isTerminal = (e: JournalEntry): boolean => TERMINAL_KINDS.has(e.kind);
const same = (a: unknown, b: unknown): boolean => JSON.stringify(a) === JSON.stringify(b);
/** A choice that moves the world forward by itself (a fault only ever perturbs one that does). */
const progresses = (ch: Choice): boolean => ch.weight > 0 && !ch.label.startsWith("fault");
/** Let every microtask run; the code under test has no other way to make progress between gates. */
const settle = (): Promise<void> => new Promise((r) => setImmediate(r));

interface QueueItem {
  id: number;
  wf: string;
  work: WorkItem;
  deliveries: number;
  inflight: number;
  gone: boolean;
  /** A cancel item's ahead-of-the-chain recording has been started (it is attempted once). */
  ahead?: boolean;
}

interface Incarnation {
  id: number;
  worker: number;
  item: QueueItem;
  dead: boolean;
  done: boolean;
  /** Not a delivery: the ahead-of-the-chain recording of a cancel item (no ack, no inflight). */
  ahead?: boolean;
  /** Per run: the highest stream-seq any of this incarnation's loads returned. */
  seen: Map<string, number>;
}

interface Gate {
  inc: Incarnation;
  label: string;
  fault: "infra" | "lostAck" | undefined;
  resolve: () => void;
  reject: (e: Error) => void;
}

interface Timer {
  runId: string;
  wf: string;
  seq: number;
  dueMs: number;
  order: number;
}

interface ClientAction {
  label: string;
  notBefore: number;
  /** The run must have started (a signal for a run with no journal is dropped by design). */
  needsRun: string;
  act: () => void;
}

interface Landed {
  runId: string;
  streamSeq: number;
  entry: JournalEntry;
  inc: number;
}

interface Choice {
  weight: number;
  label: string;
  act: () => void;
}

export async function runCase(c: SimCase, opts: SimOptions = {}): Promise<SimReport> {
  const world = new World(c, opts);
  return world.run();
}

class World {
  private readonly inner = new InMemoryJournal();
  private readonly rng: Rng;
  private readonly workflows: Map<string, AnyWorkflow>;
  private readonly budget: Faults;
  private now = 1_700_000_000_000;
  private step = 0;
  private serial = 0;

  private readonly pending: Gate[] = [];
  private readonly queue: QueueItem[] = [];
  private readonly published = new Map<string, number>();
  private timers: Timer[] = [];
  private readonly armed = new Set<string>();
  private readonly incs: Incarnation[] = [];
  private readonly runs = new Map<string, { wf: string }>();
  private readonly actions: ClientAction[] = [];
  private readonly landed: Landed[] = [];
  private readonly fired = new Map<string, number>();
  private readonly cancelsSent = new Set<string>();

  private readonly violations: string[] = [];
  private readonly known: string[] = [];
  private readonly trace: string[] = [];
  private readonly stats: SimStats = {
    steps: 0,
    runs: 0,
    incarnations: 0,
    fences: 0,
    crashes: 0,
    redeliveries: 0,
    infraErrors: 0,
    lostAcks: 0,
    effects: 0,
    repeatedEffects: 0,
    terminalRaces: 0,
    eventRetries: 0,
    cancelsAhead: 0,
  };

  constructor(
    private readonly c: SimCase,
    private readonly opts: SimOptions,
  ) {
    this.rng = new Rng(c.schedSeed);
    this.workflows = buildWorkflows(c.programs);
    this.budget = { ...c.faults };
  }

  private readonly clock = (): number => this.now;

  private violate(message: string): void {
    this.violations.push(message);
    this.trace.push(`!! ${message}`);
  }

  // ---------------------------------------------------------------- the run loop

  async run(): Promise<SimReport> {
    await this.clientStart();
    const maxSteps = this.opts.maxSteps ?? 3_000;
    for (;;) {
      await settle();
      if (this.violations.length > 0) break; // the first violation is the finding; stop early
      let choices = this.choices();
      if (!choices.some(progresses)) {
        // Nothing can move — except a client that is merely not due yet. Time passes.
        const waiting = this.actions.filter((a) => this.runs.has(a.needsRun));
        if (waiting.length === 0) break; // quiescent
        for (const a of waiting) a.notBefore = 0;
        choices = this.choices();
        if (!choices.some(progresses)) break;
      }
      if (++this.step > maxSteps) {
        this.violate(`did not converge within ${maxSteps} steps (a livelock, or a redelivery loop)`); // prettier-ignore
        break;
      }
      this.now += 1;
      const choice = choices[this.rng.weighted(choices.map((ch) => ch.weight))];
      if (choice === undefined) break;
      this.trace.push(`${this.step} ${choice.label}`);
      choice.act();
    }
    await settle();
    if (this.violations.length === 0) await this.checkFinal();
    this.stats.steps = this.step;
    this.stats.runs = this.runs.size;
    this.stats.incarnations = this.incs.length;
    return { violations: this.violations, known: this.known, trace: this.trace, stats: this.stats }; // prettier-ignore
  }

  private choices(): Choice[] {
    const out: Choice[] = [];
    this.pending.forEach((g, i) => {
      out.push({ weight: 6, label: `inc${g.inc.id} ${g.label}`, act: () => this.release(i) });
    });
    for (const item of this.queue) {
      if (item.gone) continue;
      if (item.work.kind === "cancel" && item.ahead !== true && this.opts.cancelAhead !== false) {
        // The worker already executing the run pulls the cancel and records it ahead of its chain.
        // (A worker NOT driving the run records it on delivery, which is ahead of nothing.)
        for (const w of this.workersDriving(item.work.runId)) {
          out.push({
            weight: 4,
            label: `worker ${w} records item${item.id} (cancel ${item.work.runId}) ahead of its chain`,
            act: () => this.recordAhead(item, w),
          });
        }
      }
      const workers = this.eligibleWorkers(item);
      if (workers.length === 0) continue;
      if (item.inflight === 0) {
        out.push({
          weight: 4,
          label: `deliver item${item.id} (${item.work.kind} ${item.work.runId})`,
          act: () => this.deliver(item, this.rng.pick(workers)),
        });
      } else if (this.budget.redeliveries > 0) {
        out.push({
          weight: 0.4,
          label: `fault: redeliver item${item.id} (${item.work.kind} ${item.work.runId}) while in flight`,
          act: () => {
            this.budget.redeliveries -= 1;
            this.stats.redeliveries += 1;
            this.deliver(item, this.rng.pick(workers));
          },
        });
      }
    }
    if (this.timers.length > 0) {
      out.push({ weight: 2, label: "fire the next timer", act: () => this.fireTimer() });
    }
    this.actions.forEach((a, i) => {
      if (this.step < a.notBefore || !this.runs.has(a.needsRun)) return;
      out.push({
        weight: 2,
        label: `client: ${a.label}`,
        act: () => {
          this.actions.splice(i, 1);
          a.act();
        },
      });
    });
    if (this.budget.crashes > 0) {
      for (const inc of this.incs) {
        if (inc.dead) continue;
        // A finished incarnation is still worth crashing while it holds gates: that is work its
        // executeRun set in motion and did not wait for.
        if (inc.done && !this.pending.some((g) => g.inc === inc)) continue;
        out.push({ weight: 0.3, label: `fault: crash inc${inc.id}`, act: () => this.crash(inc) });
      }
    }
    return out;
  }

  private eligibleWorkers(item: QueueItem): number[] {
    // One worker never drives the same run twice at once (the real worker chains them in-process).
    const busy = new Set(
      this.incs
        .filter((i) => !i.dead && !i.done && i.item.work.runId === item.work.runId)
        .map((i) => i.worker),
    );
    return Array.from({ length: this.c.workers }, (_, w) => w).filter((w) => !busy.has(w));
  }

  private workersDriving(runId: string): number[] {
    const live = this.incs.filter(
      (i) => !i.dead && !i.done && i.ahead !== true && i.item.work.runId === runId,
    );
    return [...new Set(live.map((i) => i.worker))];
  }

  /** Mirrors `recordCancelAhead` in src/nats/worker.ts: best-effort, never acks, never throws. */
  private recordAhead(item: QueueItem, worker: number): void {
    item.ahead = true;
    const inc: Incarnation = {
      id: this.incs.length,
      worker,
      item,
      dead: false,
      done: false,
      ahead: true,
      seen: new Map(),
    };
    this.incs.push(inc);
    this.trace.push(`   → inc${inc.id} on worker ${worker} (cancel ahead of the chain)`);
    void (async () => {
      const journal = this.journalFor(inc);
      try {
        const loaded = await journal.load(item.work.runId);
        if (!loaded.some((l) => l.entry.kind === "start")) return "no journal";
        if (loaded.some((l) => isTerminal(l.entry))) return "terminal";
        await (this.opts.recordInbound ?? recordInbound)(journal, item.work, loaded, this.clock);
        return "recorded";
      } catch (e) {
        return `not recorded (${String(e)})`;
      }
    })().then((decision) => {
      if (inc.dead) return;
      inc.done = true;
      this.trace.push(`   inc${inc.id} cancel ahead: ${decision}`);
    });
  }

  // ---------------------------------------------------------------- gates and faults

  private gate(inc: Incarnation, label: string, fault?: "infra" | "lostAck"): Promise<void> {
    if (inc.dead) return new Promise<void>(() => {}); // fresh: a shared one would pin every dead frame
    return new Promise<void>((resolve, reject) => {
      this.pending.push({ inc, label, fault, resolve, reject });
    });
  }

  private release(index: number): void {
    const [g] = this.pending.splice(index, 1);
    if (g === undefined) return;
    if (g.fault === "infra" && this.budget.infraErrors > 0 && this.rng.chance(0.06)) {
      this.budget.infraErrors -= 1;
      this.stats.infraErrors += 1;
      this.trace.push(`   fault: infrastructure error at ${g.label}`);
      g.reject(new Error(`sim: infrastructure failure (${g.label})`));
    } else if (g.fault === "lostAck" && this.budget.lostAcks > 0 && this.rng.chance(0.06)) {
      this.budget.lostAcks -= 1;
      this.stats.lostAcks += 1;
      this.trace.push(`   fault: the ack of ${g.label} is lost — it LANDED, the caller sees an error`); // prettier-ignore
      g.reject(new Error(`sim: ack lost (${g.label})`));
    } else {
      g.resolve();
    }
  }

  private crash(inc: Incarnation): void {
    this.budget.crashes -= 1;
    this.stats.crashes += 1;
    inc.dead = true;
    for (let i = this.pending.length - 1; i >= 0; i--) {
      if (this.pending[i]?.inc === inc) this.pending.splice(i, 1);
    }
    if (!inc.done && inc.ahead !== true) inc.item.inflight -= 1; // unacked → the queue redelivers it
  }

  // ---------------------------------------------------------------- the seams, per incarnation

  private journalFor(inc: Incarnation): Journal {
    return {
      load: async (runId) => {
        await this.gate(inc, `load ${runId}`, "infra");
        let out = await this.inner.load(runId);
        if (this.opts.staleReads === true && out.length > 1 && this.rng.chance(0.25)) {
          out = out.slice(0, 1 + this.rng.int(out.length - 1));
          this.trace.push(
            `   stale read: inc${inc.id} is served ${out.length} entries of ${runId}`,
          );
        }
        const last = out.at(-1)?.streamSeq ?? 0;
        if (last > (inc.seen.get(runId) ?? 0)) inc.seen.set(runId, last);
        return out;
      },
      append: async (runId, a) => {
        const what = `${runId} ${a.entry.kind}${a.entry.seq >= 0 ? `#${a.entry.seq}` : ""}`;
        await this.gate(inc, `append ${what}`, "infra");
        const msgID = this.c.journalDedup ? a.msgID : `${a.msgID}~${++this.serial}`;
        let r;
        try {
          r = await this.inner.append(runId, { ...a, msgID });
        } catch (e) {
          if (e instanceof FencedError) {
            this.stats.fences += 1;
            this.trace.push(`   inc${inc.id} FENCED appending ${what}`);
          }
          throw e;
        }
        if (!r.duplicate) this.onLanded(inc, runId, a.entry, r.streamSeq);
        await this.gate(inc, `ack of append ${what}`, "lostAck");
        return r;
      },
    };
  }

  private onLanded(inc: Incarnation, runId: string, entry: JournalEntry, streamSeq: number): void {
    this.landed.push({ runId, streamSeq, entry, inc: inc.id });
    if (inc.ahead === true && entry.kind === "cancel-requested") {
      if (this.workersDriving(runId).includes(inc.worker)) this.stats.cancelsAhead += 1;
    }
    if (entry.kind === "start") this.registerRun(runId, entry.wf);
  }

  private schedulerFor(inc: Incarnation): Scheduler {
    return {
      scheduleWake: async (runId, wf, seq, dueMs) => {
        await this.gate(inc, `schedule wake ${runId}#${seq}`, "infra");
        // One schedule per (run, seq), as in DE_TIMERS (the schedule's subject IS that pair, so a
        // re-arm while one is pending replaces it). Re-arming one that already FIRED is a new
        // schedule only once the duplicate window has passed.
        const key = `${runId}:${seq}`;
        if (this.timers.some((t) => t.runId === runId && t.seq === seq)) return;
        if (this.c.queueDedup && this.armed.has(key)) return;
        this.armed.add(key);
        this.timers.push({ runId, wf, seq, dueMs, order: ++this.serial });
      },
    };
  }

  /** Mirrors `NatsChildren.start`: append the start (a fence = already started), ALWAYS enqueue. */
  private childrenFor(inc: Incarnation): Children {
    return {
      start: async (spec: ChildStart) => {
        try {
          await this.journalFor(inc).append(spec.childRunId, {
            entry: stamp(
              {
                kind: "start",
                seq: 0,
                wf: spec.workflow,
                wfVersion: spec.version,
                input: spec.input,
                parent: spec.parent,
              },
              this.now,
            ),
            msgID: msgId.start(spec.childRunId),
            expectLastSeq: 0,
          });
        } catch (e) {
          if (!(e instanceof FencedError)) throw e;
        }
        await this.publish(
          inc,
          spec.workflow,
          { runId: spec.childRunId, kind: "start" },
          `${spec.childRunId}:work:start`,
        );
      },
    };
  }

  private hooksFor(inc: Incarnation): SimHooks {
    return {
      incId: inc.id,
      effect: async (key, name, throws) => {
        this.onEffect(inc, key);
        await this.gate(inc, `effect ${key} (${name})`);
        if (throws) throw new SimStepError(`step ${name} failed`);
        return `${name}@inc${inc.id}`;
      },
    };
  }

  /**
   * The effect invariants, checked at the moment of firing:
   *  - an incarnation fires a step only if it OWNS the step-begin (its append landed) or had SEEN
   *    it in a load (the resume path) — anything else is a fenced zombie firing a side effect;
   *  - and never for a step whose step-end it had already seen.
   */
  private onEffect(inc: Incarnation, key: string): void {
    const at = key.lastIndexOf(":");
    const runId = key.slice(0, at);
    const seq = Number(key.slice(at + 1));
    this.stats.effects += 1;
    const times = (this.fired.get(key) ?? 0) + 1;
    this.fired.set(key, times);
    if (times > 1) this.stats.repeatedEffects += 1;
    this.trace.push(
      `   EFFECT ${key} fired by inc${inc.id}${times > 1 ? ` (firing ${times})` : ""}`,
    );

    const seen = inc.seen.get(runId) ?? 0;
    const entry = (kind: string): Landed | undefined =>
      this.landed.find((l) => l.runId === runId && l.entry.kind === kind && l.entry.seq === seq);
    const begin = entry("step-begin");
    if (begin === undefined) {
      this.violate(`inc${inc.id} fired effect ${key} with no step-begin in the journal`);
    } else if (begin.inc !== inc.id && begin.streamSeq > seen) {
      this.violate(
        `inc${inc.id} fired effect ${key}, but the step-begin is inc${begin.inc}'s and this ` +
          `incarnation never saw it — a fenced zombie fired a side effect`,
      );
    }
    const end = entry("step-end");
    if (end !== undefined && (end.inc === inc.id || end.streamSeq <= seen)) {
      this.violate(`inc${inc.id} re-fired effect ${key} after seeing its step-end`);
    }
  }

  // ---------------------------------------------------------------- queue, timers, client

  private enqueue(wf: string, work: WorkItem, msgID?: string): void {
    if (work.kind === "cancel") this.cancelsSent.add(work.runId);
    if (msgID !== undefined) {
      // The duplicate window, in scheduler steps. It is never ABSENT: a cancelled parent and its
      // finished child re-notify each other on every terminal re-drive (cascade → child-done →
      // cascade → …), and the window is what ends that after one round. `queueDedup: false` is a
      // window short enough that a retried publish gets through, not a queue without one.
      const window = this.c.queueDedup ? Infinity : SHORT_DEDUP_WINDOW;
      const last = this.published.get(msgID);
      if (last !== undefined && this.step - last <= window) return;
      this.published.set(msgID, this.step);
    }
    this.queue.push({ id: ++this.serial, wf, work, deliveries: 0, inflight: 0, gone: false });
  }

  private async publish(
    inc: Incarnation,
    wf: string,
    work: WorkItem,
    msgID: string,
  ): Promise<void> {
    // prettier-ignore
    await this.gate(inc, `publish ${work.kind} → ${work.runId}`, "infra");
    this.enqueue(wf, work, msgID);
  }

  private fireTimer(): void {
    this.timers.sort((a, b) => a.dueMs - b.dueMs || a.order - b.order);
    const t = this.timers.shift();
    if (t === undefined) return;
    if (t.dueMs > this.now) this.now = t.dueMs;
    this.enqueue(t.wf, { runId: t.runId, kind: "timer", resolvesSeq: t.seq });
  }

  private async clientStart(): Promise<void> {
    const wf = workflowName(0);
    const input: SimInput = { gen: 0 };
    const r = await this.inner.append(ROOT, {
      entry: stamp({ kind: "start", seq: 0, wf, wfVersion: 1, input }, this.now),
      msgID: msgId.start(ROOT),
      expectLastSeq: 0,
    });
    this.landed.push({
      runId: ROOT,
      streamSeq: r.streamSeq,
      entry: stamp({ kind: "start", seq: 0, wf, wfVersion: 1, input }, this.now),
      inc: -1,
    });
    this.registerRun(ROOT, wf);
    this.enqueue(wf, { runId: ROOT, kind: "start" }, `${ROOT}:work:start`);
    // A client that retried its publish: without queue dedup, two start items race on two workers.
    if (this.rng.chance(0.3)) this.enqueue(wf, { runId: ROOT, kind: "start" }, `${ROOT}:work:start`); // prettier-ignore
    if (this.c.cancelRoot) {
      this.actions.push({
        label: `cancel ${ROOT}`,
        notBefore: this.rng.int(120),
        needsRun: ROOT,
        act: () => this.enqueue(wf, { runId: ROOT, kind: "cancel", reason: "sim" }, `${ROOT}:cancel`), // prettier-ignore
      });
    }
  }

  private registerRun(runId: string, wf: string): void {
    if (this.runs.has(runId)) return;
    this.runs.set(runId, { wf });
    const program = this.c.programs[programIndex(wf)];
    if (program === undefined) return;
    for (const name of promisedSignals(program)) {
      const id = `${runId}.${name}`;
      const send = (): void =>
        this.enqueue(
          wf,
          { runId, kind: "signal", signal: { name, data: { from: name }, id } },
          `${runId}:sig:${id}`,
        );
      const notBefore = this.step + this.rng.int(60);
      this.actions.push({
        label: `signal ${runId} ${name}`,
        notBefore,
        needsRun: runId,
        act: send,
      });
      if (this.rng.chance(0.2)) {
        this.actions.push({ label: `signal ${runId} ${name} (a retry)`, notBefore, needsRun: runId, act: send }); // prettier-ignore
      }
    }
  }

  // ---------------------------------------------------------------- the worker (a model)

  private deliver(item: QueueItem, worker: number): void {
    item.deliveries += 1;
    item.inflight += 1;
    const inc: Incarnation = {
      id: this.incs.length,
      worker,
      item,
      dead: false,
      done: false,
      seen: new Map(),
    };
    this.incs.push(inc);
    this.trace.push(`   → inc${inc.id} on worker ${worker} (delivery ${item.deliveries})`);
    void this.processOne(inc).then((decision) => {
      if (inc.dead) return;
      inc.done = true;
      item.inflight -= 1;
      this.trace.push(`   inc${inc.id} ${decision}`);
      if (!decision.startsWith("nak")) item.gone = true;
    });
  }

  /** Mirrors `processOne` in `src/nats/worker.ts` — see this file's header for what is left out. */
  private async processOne(inc: Incarnation): Promise<string> {
    const { work, wf: name } = inc.item;
    const journal = this.journalFor(inc);
    try {
      let loaded = await journal.load(work.runId);
      const start = loaded.find((l) => l.entry.kind === "start")?.entry as StartEntry | undefined;
      if (!start) return inc.item.deliveries >= 5 ? "term (no journal)" : "nak (no journal)";
      const parent = start.parent;

      const terminal = loaded.find((l) => isTerminal(l.entry))?.entry;
      if (terminal) {
        if (terminal.kind === "continued") {
          await this.startNextGeneration(inc, terminal, parent, name);
          if (work.kind === "cancel") await this.forwardCancel(inc, name, terminal.nextRunId);
        } else {
          if (terminal.kind === "cancelled") await this.cascadeCancel(inc, work.runId);
          if (parent) await this.notifyParent(inc, parent, work.runId, terminal);
        }
        return "ack (terminal re-drive)";
      }

      const before = this.stats.fences;
      loaded = await (this.opts.recordInbound ?? recordInbound)(journal, work, loaded, this.clock);
      if (this.stats.fences > before) this.stats.eventRetries += 1;

      const wf = this.workflows.get(name);
      if (wf === undefined) throw new Error(`sim: no workflow ${name}`);
      const outcome = await executeRun(work.runId, {
        workflow: wf,
        journal,
        loaded,
        scheduler: this.schedulerFor(inc),
        children: this.childrenFor(inc),
        dependencies: { sim: this.hooksFor(inc) },
        clock: this.clock,
        attempt: inc.item.deliveries,
      });
      const canonical = await this.checkOutcome(inc, work.runId, outcome);
      switch (outcome.kind) {
        case "completed":
        case "failed":
          if (parent && canonical) await this.notifyParent(inc, parent, work.runId, canonical);
          return `ack (${outcome.kind})`;
        case "cancelled":
          await this.cascadeCancel(inc, work.runId);
          if (parent && canonical) await this.notifyParent(inc, parent, work.runId, canonical);
          return "ack (cancelled)";
        case "continue":
          if (canonical?.kind === "continued") {
            await this.startNextGeneration(inc, canonical, parent, name);
            if (work.kind === "cancel") await this.forwardCancel(inc, name, canonical.nextRunId);
          }
          return "ack (continued)";
        case "suspended":
          return "ack (suspended)";
        case "fenced":
          return "nak (fenced)";
        case "retry":
          return "nak (retry)";
      }
    } catch (e) {
      return `nak (error: ${e instanceof Error ? e.message : String(e)})`;
    }
  }

  /**
   * THE JOURNAL IS THE SOURCE OF TRUTH: whatever an execution reports as the run's outcome must be
   * what the journal records — the same kind, the same value — or a CAS loser is telling the
   * worker (and through it a parent, the status index, the next generation) something untrue.
   */
  private async checkOutcome(
    inc: Incarnation,
    runId: string,
    outcome: ExecOutcome,
  ): Promise<JournalEntry | undefined> {
    if (outcome.kind === "suspended" || outcome.kind === "fenced" || outcome.kind === "retry") {
      return undefined;
    }
    const terminals = (await this.inner.load(runId)).filter((l) => isTerminal(l.entry));
    const t = terminals[0]?.entry;
    const mine = this.landed.some((l) => l.runId === runId && l.inc === inc.id && isTerminal(l.entry)); // prettier-ignore
    if (!mine) this.stats.terminalRaces += 1;
    const say = (what: string): void =>
      this.violate(
        `inc${inc.id} reported ${runId} as ${what}, but the journal's terminal is ` +
          `${t === undefined ? "MISSING" : JSON.stringify(t)}`,
      );
    if (t === undefined) say(outcome.kind);
    else if (outcome.kind === "completed") {
      if (t.kind !== "result" || !same(t.value, outcome.value)) say(`completed with ${JSON.stringify(outcome.value)}`); // prettier-ignore
    } else if (outcome.kind === "failed") {
      if (t.kind !== "fail" || !same(t.error, outcome.error)) say(`failed with ${JSON.stringify(outcome.error)}`); // prettier-ignore
    } else if (outcome.kind === "cancelled") {
      if (t.kind !== "cancelled") say("cancelled");
    } else {
      const n = outcome.next;
      if (t.kind !== "continued" || t.nextRunId !== n.nextRunId || !same(t.input, n.input) || (t.version ?? 1) !== n.version) say(`continued with ${JSON.stringify(n)}`); // prettier-ignore
    }
    return t;
  }

  private forwardCancel(inc: Incarnation, wf: string, nextRunId: string): Promise<void> {
    return this.publish(inc, wf, { runId: nextRunId, kind: "cancel", reason: "sim" }, `${nextRunId}:cancel`); // prettier-ignore
  }

  private async notifyParent(
    inc: Incarnation,
    parent: { runId: string; seq: number; wf: string },
    childRunId: string,
    terminal: JournalEntry,
  ): Promise<void> {
    const child: WorkItem["child"] =
      terminal.kind === "result"
        ? { id: childRunId, ok: true, value: terminal.value }
        : terminal.kind === "fail"
          ? { id: childRunId, ok: false, error: terminal.error }
          : {
              id: childRunId,
              ok: false,
              error: { name: "CancelledError", message: `run ${childRunId}: cancelled` },
            };
    await this.publish(
      inc,
      parent.wf,
      { runId: parent.runId, kind: "child-done", resolvesSeq: parent.seq, child },
      `${childRunId}:done`,
    );
  }

  private async cascadeCancel(inc: Incarnation, runId: string): Promise<void> {
    const loaded = await this.journalFor(inc).load(runId);
    for (const l of loaded) {
      if (l.entry.kind !== "call" || hasResolve(loaded, l.entry.seq)) continue;
      await this.publish(
        inc,
        l.entry.wf,
        { runId: l.entry.childRunId, kind: "cancel", reason: `parent ${runId} cancelled` },
        `${l.entry.childRunId}:cancel`,
      );
    }
  }

  private async startNextGeneration(
    inc: Incarnation,
    ce: ContinuedEntry,
    parent: StartEntry["parent"],
    name: string,
  ): Promise<void> {
    const body = {
      kind: "start" as const,
      seq: 0,
      wf: name,
      wfVersion: ce.version ?? 1,
      input: ce.input,
      generation: ce.generation,
      base: ce.base,
    };
    try {
      await this.journalFor(inc).append(ce.nextRunId, {
        entry: stamp(parent ? { ...body, parent } : body, this.now),
        msgID: msgId.start(ce.nextRunId),
        expectLastSeq: 0,
      });
    } catch (e) {
      if (!(e instanceof FencedError)) throw e;
    }
    await this.publish(inc, name, { runId: ce.nextRunId, kind: "start" }, `${ce.nextRunId}:work:start`); // prettier-ignore
  }

  // ---------------------------------------------------------------- the end state

  private async checkFinal(): Promise<void> {
    for (const inc of this.incs) {
      if (!inc.dead && !inc.done) {
        this.violate(
          `inc${inc.id} (${inc.item.work.kind} ${inc.item.work.runId}) never returned and holds no gate — executeRun hung`,
        );
      }
    }
    for (const [runId, { wf }] of this.runs) {
      const entries = await this.inner.load(runId);
      this.checkJournalShape(runId, entries);
      const terminals = entries.filter((l) => isTerminal(l.entry));
      // CANCELLATION FOLLOWS THE CHAIN: a cancel journaled in a generation that continued must have
      // reached the next one — whichever of the two the worker saw first.
      const rolled = terminals[0]?.entry;
      if (
        rolled?.kind === "continued" &&
        entries.some((l) => l.entry.kind === "cancel-requested")
      ) {
        if (!this.cancelsSent.has(rolled.nextRunId)) {
          this.violate(`${runId} was cancelled and continued as ${rolled.nextRunId}, which was never sent the cancel`); // prettier-ignore
        }
      }
      if (terminals.length === 0) {
        this.violate(
          `${runId} is STRANDED: nothing is queued, armed or in flight, and its journal has no terminal ` +
            `(last entries: ${entries
              .slice(-4)
              .map((l) => `${l.entry.kind}${l.entry.seq >= 0 ? `#${l.entry.seq}` : ""}`)
              .join(", ")})`,
        );
      } else if (terminals.length === 1 && terminals[0] !== undefined) {
        await this.checkPureReplay(runId, wf, entries, terminals[0].entry);
      }
    }
  }

  private checkJournalShape(runId: string, entries: LoadedEntry[]): void {
    const count = new Map<string, number>();
    const bump = (k: string): void => void count.set(k, (count.get(k) ?? 0) + 1);
    let ended = false;
    for (const [i, l] of entries.entries()) {
      const e = l.entry;
      if ((e.kind === "start") !== (i === 0)) this.violate(`${runId}: entry ${i} is a ${e.kind}`);
      // Events may still arrive after the end (a late signal, a cancel that lost the race) — but a
      // COMMAND or a step-end after the terminal is somebody executing a run that is over.
      if (ended && !["signal", "resolve", "cancel-requested"].includes(e.kind)) {
        this.violate(`${runId}: a ${e.kind}#${e.seq} was journaled AFTER the run's terminal — a finished run was executed`); // prettier-ignore
      }
      if (isTerminal(e)) ended = true;
      if (isTerminal(e)) bump("terminal");
      else if (e.kind === "step-end") bump(`step-end#${e.seq}`);
      else if (e.kind === "resolve") bump(`resolve→${e.resolvesSeq}`);
      else if (e.kind === "signal") bump(`signal ${e.signalId}`);
      else if (e.kind === "cancel-requested") bump("cancel-requested");
      else if (e.kind !== "start") bump(`command#${e.seq}`);
      if (
        e.kind === "step-end" &&
        !entries.slice(0, i).some((p) => p.entry.kind === "step-begin" && p.entry.seq === e.seq)
      ) {
        // prettier-ignore
        this.violate(`${runId}: step-end#${e.seq} has no step-begin before it`);
      }
      if (e.kind === "fail" && !EXPECTED_FAILURES.has(e.error.name)) {
        this.violate(`${runId} failed with ${e.error.name}: ${e.error.message} — no generated program can`); // prettier-ignore
      }
    }
    for (const [what, n] of count) {
      if (n > 1) this.violate(`${runId}: ${n} × ${what} in one journal`);
    }
    // NOT asserted: that command seqs are contiguous or in stream order. Neither holds, by design —
    // a fresh command that a journaled cancel stops consumes its seq without journaling anything
    // (onCancel cleanup steps then land at later seqs), and a fan-out branch whose append hit an
    // infrastructure error is re-journaled on the retry, AFTER its younger siblings. The engine
    // looks commands up by seq, never by position, so neither matters.
  }

  /**
   * Replaying a finished journal must reproduce its terminal WITHOUT firing an effect or journaling
   * anything: the recorded outcome is a function of the recorded history, or replay is not replay.
   */
  private async checkPureReplay(
    runId: string,
    wfName: string,
    entries: LoadedEntry[],
    terminal: JournalEntry,
  ): Promise<void> {
    const wf = this.workflows.get(wfName);
    if (wf === undefined) return;
    const last = entries.at(-1)?.streamSeq ?? 0;
    // The history the terminal was computed FROM: everything before it. (executeRun refuses to run
    // a journal that holds a terminal, and what lands after one must not matter.)
    const history = entries.slice(
      0,
      entries.findIndex((l) => isTerminal(l.entry)),
    );
    const outcome = await executeRun(runId, {
      workflow: wf,
      loaded: history,
      clock: this.clock,
      journal: {
        load: () => this.inner.load(runId),
        append: (_run, a) => {
          if (!isTerminal(a.entry)) this.violate(`pure replay of ${runId} journaled a ${a.entry.kind}#${a.entry.seq}`); // prettier-ignore
          return Promise.resolve({ streamSeq: last, duplicate: true });
        },
      },
      scheduler: { scheduleWake: () => Promise.resolve() },
      children: { start: () => Promise.resolve() },
      dependencies: {
        sim: {
          incId: -2,
          effect: (key: string, name: string, throws: boolean) => {
            if (throws) {
              // OPEN FINDING (step-failure-not-journaled): a step whose fn THROWS leaves a
              // step-begin with no step-end, so every replay runs it again.
              this.known.push(`step-failure-not-journaled: pure replay of ${runId} re-fired ${key}`); // prettier-ignore
              return Promise.reject(new SimStepError(`step ${name} failed`));
            }
            this.violate(`pure replay of ${runId} fired effect ${key}`);
            return Promise.resolve("replayed");
          },
        } satisfies SimHooks,
      },
    });
    const ok =
      (terminal.kind === "result" &&
        outcome.kind === "completed" &&
        same((terminal.value as SimResult).digest, (outcome.value as SimResult).digest)) ||
      (terminal.kind === "fail" && outcome.kind === "failed" && same(terminal.error, outcome.error)) || // prettier-ignore
      (terminal.kind === "cancelled" && outcome.kind === "cancelled") ||
      (terminal.kind === "continued" && outcome.kind === "continue" && outcome.next.nextRunId === terminal.nextRunId); // prettier-ignore
    // OPEN FINDING (premature-suspend): with a native Promise.all over a pending branch and a
    // rejected one, whether the executor sees the suspend or the throw first is decided by
    // microtask and I/O timing, not by the journal — the same history can yield either.
    const program = this.c.programs[programIndex(wfName)];
    const racy = program?.ops.some((op) => op.op === "fanout" && op.join === "all") === true;
    // (Under a journaled cancel the pending branch's halt is `cancelled` rather than `suspend`.)
    if (!ok && racy && (outcome.kind === "suspended" || outcome.kind === "cancelled")) {
      this.known.push(`premature-suspend: pure replay of ${runId} is ${outcome.kind}, its journal records a ${terminal.kind}`); // prettier-ignore
      return;
    }
    if (!ok) {
      this.violate(
        `pure replay of ${runId} yields ${JSON.stringify(outcome)}, but its journal records ${JSON.stringify(terminal)}`,
      );
    }
  }
}
