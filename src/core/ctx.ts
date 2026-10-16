// @synadia-ai/durable — the deterministic context + two-phase replay engine (pure; depends only on
// the Journal interface). See PROTOCOL §11.2.
//
// Suspension and fencing are surfaced OUT OF BAND via a halt promise the executor races against —
// they are never thrown through user workflow code, so a user `try/catch` cannot swallow them.

import { AsyncLocalStorage } from "node:async_hooks";
import {
  DurableError,
  DurableTimeoutError,
  FencedError,
  NonDeterminismError,
  SerializationError,
  StepNestingError,
  WorkflowFailure,
} from "../errors.js";
import { invariant } from "../internal/assert.js";
import {
  type AnyWorkflow,
  type CallOptions,
  type Ctx,
  type DurableFuture,
  type RetryPolicy,
  type StepOptions,
  type Workflow,
} from "../types.js";
import {
  type AwaitEntry,
  type CallEntry,
  type CancelRequestedEntry,
  type EntryKind,
  type JournalEntry,
  type LoadedEntry,
  type ResolveEntry,
  type SleepEntry,
  type StepBeginEntry,
  type StepEndEntry,
  type ValueEntry,
  msgId,
  NON_COMMAND_SEQ,
  stamp,
} from "../journal/entry.js";
import { type Journal } from "../journal/store.js";
import { type Children } from "./children.js";
import { type Scheduler } from "./scheduler.js";

/** Out-of-band reason the executor stops driving the workflow. */
export type Halt =
  | { tag: "suspend" }
  | { tag: "fenced" }
  | { tag: "retry"; error: unknown }
  | { tag: "continue" }
  | { tag: "fail"; error: { name: string; message: string } }
  | { tag: "cancelled" };

/** Result of an optimistic append: success, lost-CAS (fenced), transient (retry), or unencodable (fail). */
type AppendStatus = "ok" | "fenced" | "retry" | "fail";

/** The settled (or pending) state of a durable op + the stream-seq of its resolving entry (race ordering). */
type Resolution =
  | { state: "value"; streamSeq: number; value: unknown }
  | { state: "error"; streamSeq: number; error: { name: string; message: string } }
  | { state: "timeout"; streamSeq: number }
  | { state: "pending" };

/** The next generation a continue-as-new starts (deterministic from the current run's base+generation). */
export interface ContinueSpec {
  nextRunId: string;
  input: unknown;
  generation: number;
  base: string;
  /** The next generation's workflow version (defaults to the current generation's version). */
  version: number;
}

/**
 * A promise that never settles — returned from ctx.* after a halt so user code suspends forever.
 *
 * A FRESH one per call, never a shared constant: `await p` registers a reaction ON `p` that holds
 * the awaiting function's whole frame — the workflow's locals, this ctx, the loaded journal. On a
 * module-level promise those reactions are reachable for the life of the process, so every
 * suspension of every run leaked its entire continuation (20,000 suspensions of a workflow holding
 * 64 KiB: 1.3 GB retained after a full GC). A promise nobody else references is collected together
 * with whatever awaits it.
 */
function never<T = never>(): Promise<T> {
  return new Promise<T>(() => {});
}

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** Kinds that consume a command position (seq). start/step-end/signal/resolve/result/fail do not. */
const COMMAND_KINDS = new Set<EntryKind>([
  "step-begin",
  "now",
  "random",
  "uuid",
  "sleep",
  "await",
  "call",
]);

/** Kinds that end a run. At most one per journal, and nothing is executed past it. */
const TERMINAL_KINDS = new Set<EntryKind>(["result", "fail", "continued", "cancelled"]);

/**
 * The step executing on THIS async call chain, for every ctx in the process — ONE store, never one
 * per ctx. On Node 20/22 AsyncLocalStorage rides async_hooks, and every instance that has ever
 * `run()` is visited on EVERY promise the process creates, for the life of the process (it is only
 * ever removed by `disable()`). A store per execution therefore made a worker slower with every
 * run it had ever driven — 30× on a few hundred executions, unboundedly on a long-lived worker —
 * and slowed the application's own promises with it. Node 24's AsyncContextFrame hides the cost,
 * which is how it went unseen. The frame names its ctx, so another ctx's step is not "inside" ours.
 */
const STEP_SCOPE = new AsyncLocalStorage<{ ctx: CtxImpl; stepSeq: number }>();

export interface CtxInit {
  runId: string;
  workflow: AnyWorkflow;
  journal: Journal;
  scheduler: Scheduler;
  children: Children;
  loaded: LoadedEntry[];
  dependencies: Record<string, unknown>;
  clock: () => number;
  attempt: number;
  /** This run's continue-as-new generation (0 for the original). */
  generation: number;
  /** The chain's logical (generation-0) runId. */
  base: string;
}

export class CtxImpl implements Ctx {
  readonly runId: string;
  readonly workflow: string;
  readonly version: number;
  readonly attempt: number;

  /** continue-as-new chain identity. */
  private readonly generation: number;
  private readonly base: string;
  /** Set once ctx.continueAsNew is called: the next generation the executor will start. */
  private continueSpec: ContinueSpec | undefined;

  private readonly journal: Journal;
  private readonly scheduler: Scheduler;
  private readonly children: Children;
  private readonly deps: Record<string, unknown>;
  private readonly clock: () => number;
  /** Stream-seq of the `cancel-requested` entry if cancellation was requested (freezes later resolves). */
  private readonly cancelAt: number | undefined;
  /** Reason from the `cancel-requested` entry, carried onto the terminal `cancelled`. */
  private readonly cancelReason: string | undefined;

  /** Mutable view of the journal (loaded entries + appends made this session). */
  private entries: LoadedEntry[];
  /** Stream-seq of the last entry on this run's subject (for CAS chaining). */
  private lastSeq: number;
  /** Next command position. */
  private seq = 0;
  /** Set once we halt (any reason); makes halt() idempotent. */
  private halted = false;
  /**
   * Set once a real fence is observed (a lost CAS — a zombie worker). Only a fence short-circuits the
   * append queue; a fan-out SUSPEND must NOT, or a concurrent branch's legitimate command-append would
   * be turned into a spurious fence.
   */
  private fenced = false;
  /**
   * Serializes journal appends so concurrent ctx.* ops (fan-out) chain their CAS on `lastSeq` instead
   * of racing it into a self-fence. A no-op for sequential single-op workflows (the chain is settled).
   */
  private appendChain: Promise<unknown> = Promise.resolve();
  /**
   * Every branch's arm promise (`ctx.start*` journals the command, then fires its side effect). Only
   * the append half rides {@link appendChain}, so these are tracked separately for {@link flushAppends}
   * to drain — otherwise a discarded future's child-start could still be in flight when the worker acks.
   */
  private readonly arms: Promise<AppendStatus>[] = [];
  /** Stashes the infrastructure error that triggered a retry-halt, for reporting. */
  private retryError: unknown = undefined;
  /** Stashes a deterministic serialization failure that fails the run (never retried), for reporting. */
  private fatal: { name: string; message: string } | undefined = undefined;
  private resolveHalt!: (h: Halt) => void;
  private readonly haltP: Promise<Halt>;
  /** Opt-in compensation handlers (ctx.onCancel), run LIFO before the `cancelled` terminal. */
  private readonly cancelHandlers: Array<() => void | Promise<unknown>> = [];
  /** True while onCancel handlers run — suspending ctx.* ops are rejected (v0.2: local cleanup only). */
  private inCleanup = false;
  /** Ends the wait on the cleanup handler now running, when one of its steps cannot go on (see {@link runCleanup}). */
  private abortCleanupHandler: ((h: Halt) => void) | undefined;

  constructor(init: CtxInit) {
    this.runId = init.runId;
    this.workflow = init.workflow.name;
    this.version = init.workflow.version;
    this.attempt = init.attempt;
    this.generation = init.generation;
    this.base = init.base;
    this.journal = init.journal;
    this.scheduler = init.scheduler;
    this.children = init.children;
    this.deps = init.dependencies;
    this.clock = init.clock;
    this.entries = init.loaded.map((l) => ({ ...l }));
    this.lastSeq = this.entries.at(-1)?.streamSeq ?? 0;
    const cr = init.loaded.find((l) => l.entry.kind === "cancel-requested");
    this.cancelAt = cr?.streamSeq;
    this.cancelReason = cr ? (cr.entry as CancelRequestedEntry).reason : undefined;
    this.haltP = new Promise<Halt>((res) => {
      this.resolveHalt = res;
    });
  }

  /** The executor races this against the workflow promise. */
  haltPromise(): Promise<Halt> {
    return this.haltP;
  }

  /**
   * Await everything this run set in motion (looping in case a still-running concurrent branch adds
   * more while we drain). The executor calls this before returning ANY outcome: the worker acks or
   * NAKs the moment executeRun returns, and anything still in flight past that point could be lost
   * to a crash right after the ack — an acked item is never redelivered, so nothing would re-drive
   * the missing intent (e.g. a parent suspended forever on a child whose start was never enqueued).
   *
   * Two things drain, and BOTH are needed, because arming a fan-out branch is two operations:
   *
   * - the **append chain** — every journaled command; and
   * - the **arms** — each branch's side effect too (the child start, the schedule), which is not on
   *   the chain. Only a discarded (never-awaited) future can still be arming at this point, since
   *   `awaitFuture` awaits its own arm; draining them is what makes "the command is journaled and
   *   the child is started" true at the ack rather than merely likely. See LEARNINGS #48.
   *
   * Neither can throw here: the chain swallows append outcomes onto itself, and the arms are
   * drained with allSettled (an arm that rejects still rejects for whoever awaits the future).
   */
  async flushAppends(): Promise<void> {
    for (;;) {
      const armCount = this.arms.length;
      const tail = this.appendChain;
      // Arms first: an arm enqueues appends, never the other way round.
      await Promise.allSettled(this.arms);
      await tail;
      if (this.arms.length === armCount && this.appendChain === tail) return;
    }
  }

  // ---------------------------------------------------------------- core: ctx.run (two-phase)

  async run<T>(name: string, fn: () => Promise<T> | T, opts?: StepOptions): Promise<T> {
    this.assertNotInStep("ctx.run");
    const seq = this.seq++;
    const begin = this.findAt(seq, "step-begin") as StepBeginEntry | undefined;
    const end = this.findAt(seq, "step-end") as StepEndEntry | undefined;

    if (end) {
      // REPLAY (completed): return the recorded value; fn is NOT executed.
      this.assertName(seq, begin?.name, name);
      return end.value as T;
    }
    if (begin) {
      // RESUME (crashed after begin, before end): re-run fn (at-least-once; idempotency key) —
      // unless a cancel is journaled: re-running would fire the side effect for a run being killed
      // (whether it already fired once is the same uncertainty as any crash; `ctx.onCancel`
      // compensation with `ctx.key()` is the tool for that).
      this.assertName(seq, begin.name, name);
      if (this.cancelPending()) return this.haltCancelled<T>();
      return this.runStepEffect(seq, name, fn, opts);
    }
    // Neither begin nor end here → a fresh step. A journaled cancel hard-stops BEFORE any new
    // intent is appended: no new effects fire once cancellation is requested (cleanup exempt).
    if (this.cancelPending()) return this.haltCancelled<T>();
    // Fail loud if a DIFFERENT command kind was recorded at this position (nondeterministic
    // divergence — e.g. the code changed ctx.now() into ctx.run()).
    this.assertKind(seq, "step-begin");
    // FRONTIER (fresh): CAS the intent BEFORE the effect. A zombie loses here and never fires fn.
    const key = `${this.runId}:${seq}`;
    const r = await this.appendCAS(
      stamp({ kind: "step-begin", seq, name, key }, this.clock()),
      msgId.stepBegin(this.runId, seq),
    );
    if (r === "retry") return this.haltRetry<T>();
    if (r === "fenced") return this.haltFenced<T>();
    return this.runStepEffect(seq, name, fn, opts);
  }

  private async runStepEffect<T>(
    seq: number,
    name: string,
    fn: () => Promise<T> | T,
    opts: StepOptions | undefined,
  ): Promise<T> {
    // Run fn inside an async-local step context: ctx.key() and the nesting guard read it, and it scopes
    // per-fn even when sibling steps run concurrently (fan-out) — a global counter would conflate a
    // nested ctx.* call with a concurrent sibling.
    const value = await STEP_SCOPE.run({ ctx: this, stepSeq: seq }, () =>
      this.runFn(fn, opts?.retry ?? { kind: "none" }),
    );
    const r = await this.appendCAS(
      stamp({ kind: "step-end", seq, name, value }, this.clock()),
      msgId.stepEnd(this.runId, seq),
    );
    if (r === "retry") return this.haltRetry<T>();
    if (r === "fail") return this.haltFail<T>(); // step result not serializable — fail, don't retry
    if (r === "fenced") {
      // Reconcile: another worker may have completed this step. Adopt the canonical value if present.
      // The reload is INFRA — a transient read failure here must retry the run, not throw through
      // user code as a workflow failure (the same infra-vs-logic split as appendCASRaw).
      let reloaded: LoadedEntry[];
      try {
        reloaded = await this.journal.load(this.runId);
      } catch (e) {
        this.retryError = e;
        return this.haltRetry<T>();
      }
      const existing = reloaded.find((l) => l.entry.kind === "step-end" && l.entry.seq === seq);
      if (existing) {
        this.adopt(reloaded);
        return (existing.entry as StepEndEntry).value as T;
      }
      return this.haltFenced<T>();
    }
    return value;
  }

  private async runFn<T>(fn: () => Promise<T> | T, retry: RetryPolicy): Promise<T> {
    const max = retry.kind === "none" ? 1 : retry.attempts;
    let attempt = 0;
    for (;;) {
      try {
        return await fn();
      } catch (e) {
        attempt += 1;
        if (attempt >= max) throw e;
        await delay(retryDelay(retry, attempt));
      }
    }
  }

  // ---------------------------------------------------------------- deterministic builtins

  now(): Promise<number> {
    return this.valueStep("now", () => this.clock());
  }
  random(): Promise<number> {
    return this.valueStep("random", () => Math.random());
  }
  uuid(): Promise<string> {
    return this.valueStep("uuid", () => crypto.randomUUID());
  }

  private async valueStep<T>(kind: "now" | "random" | "uuid", gen: () => T): Promise<T> {
    this.assertNotInStep(`ctx.${kind}`);
    const seq = this.seq++;
    const existing = this.findAt(seq, kind) as ValueEntry | undefined;
    if (existing) return existing.value as T;
    // A fresh command under a journaled cancel hard-stops (no new commands for a run being killed).
    if (this.cancelPending()) return this.haltCancelled<T>();
    this.assertKind(seq, kind);
    const value = gen();
    const r = await this.appendCAS(
      stamp({ kind, seq, value }, this.clock()),
      msgId.value(this.runId, seq),
    );
    if (r === "retry") return this.haltRetry<T>();
    if (r === "fenced") return this.haltFenced<T>();
    return value;
  }

  // ---------------------------------------------------------------- durable timers & signals

  sleep(name: string, ms: number): Promise<void> {
    return this.awaitFuture(this.startSleepImpl("ctx.sleep", name, ms));
  }

  startSleep(name: string, ms: number): DurableFuture<void> {
    return this.startSleepImpl("ctx.startSleep", name, ms);
  }

  private startSleepImpl(op: string, name: string, ms: number): DurableFutureImpl<void> {
    this.assertNotInStep(op);
    const seq = this.seq++;
    return this.issue<void>(seq, "sleep", name, this.armSleep(seq, name, ms));
  }

  /** Journal the sleep command (fresh, computing the deadline) or validate it (replay), then schedule the wake. */
  private async armSleep(seq: number, name: string, ms: number): Promise<AppendStatus> {
    const cmd = this.findAt(seq, "sleep") as SleepEntry | undefined;
    let dueMs: number;
    if (!cmd) {
      // Fresh command under a journaled cancel: hard-stop before journaling intent or arming a
      // timer. Report "ok" (nothing to arm) — the await path then reads "pending" and converges on
      // the already-resolved cancelled halt (halt() is idempotent).
      if (this.cancelPending()) {
        this.halt({ tag: "cancelled" });
        return "ok";
      }
      this.assertKind(seq, "sleep");
      dueMs = this.clock() + ms;
      const r = await this.appendCAS(
        stamp({ kind: "sleep", seq, name, dueMs }, this.clock()),
        msgId.command(this.runId, seq),
      );
      if (r !== "ok") return r;
    } else {
      if (cmd.name !== name) {
        throw new NonDeterminismError(
          this.runId,
          seq,
          `expected sleep "${cmd.name}", got "${name}"`,
        );
      }
      dueMs = cmd.dueMs;
    }
    if (this.hasResolve(seq)) return "ok"; // timer already fired — no need to (re-)schedule
    return this.ensureWake(seq, dueMs);
  }

  /** (Re-)schedule a wake at `dueMs` (ctx.sleep + ctx.await's timeout); an infra failure → retry. */
  private async ensureWake(seq: number, dueMs: number): Promise<AppendStatus> {
    try {
      await this.scheduler.scheduleWake(this.runId, this.workflow, seq, dueMs);
      return "ok";
    } catch (e) {
      this.retryError = e; // scheduling is infrastructure — retry, don't fail the run
      return "retry";
    }
  }

  await<T>(name: string, opts?: { timeoutMs?: number }): Promise<T> {
    return this.awaitFuture(this.startAwaitImpl<T>("ctx.await", name, opts));
  }

  startAwait<T>(name: string, opts?: { timeoutMs?: number }): DurableFuture<T> {
    return this.startAwaitImpl<T>("ctx.startAwait", name, opts);
  }

  private startAwaitImpl<T>(
    op: string,
    name: string,
    opts: { timeoutMs?: number } | undefined,
  ): DurableFutureImpl<T> {
    this.assertNotInStep(op);
    const seq = this.seq++;
    return this.issue<T>(seq, "await", name, this.armAwait(seq, name, opts));
  }

  /** Journal the await command (fresh, with optional deadline) or validate it (replay); (re-)arm a timeout timer while pending. */
  private async armAwait(
    seq: number,
    name: string,
    opts: { timeoutMs?: number } | undefined,
  ): Promise<AppendStatus> {
    const cmd = this.findAt(seq, "await") as AwaitEntry | undefined;
    let dueMs: number | undefined;
    if (!cmd) {
      // Fresh command under a journaled cancel: hard-stop (see armSleep).
      if (this.cancelPending()) {
        this.halt({ tag: "cancelled" });
        return "ok";
      }
      this.assertKind(seq, "await");
      dueMs = opts?.timeoutMs !== undefined ? this.clock() + opts.timeoutMs : undefined;
      const body =
        dueMs !== undefined
          ? { kind: "await" as const, seq, name, dueMs }
          : { kind: "await" as const, seq, name };
      const r = await this.appendCAS(stamp(body, this.clock()), msgId.command(this.runId, seq));
      if (r !== "ok") return r;
    } else {
      if (cmd.name !== name) {
        throw new NonDeterminismError(
          this.runId,
          seq,
          `expected await "${cmd.name}", got "${name}"`,
        );
      }
      dueMs = cmd.dueMs;
    }
    // Pending with a timeout → (re-)ensure the timer; a resolved or pure-signal await arms nothing.
    if (dueMs !== undefined && this.resolveAwait(seq, name).kind === "pending") {
      return this.ensureWake(seq, dueMs);
    }
    return "ok";
  }

  /**
   * Resolve the await at `seq` (name `name`) deterministically. Signals of this name are assigned to
   * same-named awaits in command order; an await consumes a signal only when that signal's stream-seq
   * precedes the await's timeout (so a timed-out await consumes nothing and never shifts the match for
   * a later same-name await). Returns the resolution for the await AT `seq`: a signal's data, a
   * timeout, or pending (neither has arrived yet).
   */
  private resolveAwait(
    seq: number,
    name: string,
  ):
    | { kind: "signal"; data: unknown; streamSeq: number }
    | { kind: "timeout"; streamSeq: number }
    | { kind: "pending" } {
    const signals: { data: unknown; streamSeq: number }[] = [];
    for (const l of this.entries) {
      if (l.entry.kind === "signal" && l.entry.name === name && this.beforeCancel(l.streamSeq)) {
        signals.push({ data: l.entry.data, streamSeq: l.streamSeq });
      }
    }
    const awaits: { seq: number; dueMs: number | undefined }[] = [];
    for (const l of this.entries) {
      if (l.entry.kind === "await" && l.entry.name === name && l.entry.seq <= seq) {
        awaits.push({ seq: l.entry.seq, dueMs: l.entry.dueMs });
      }
    }
    awaits.sort((a, b) => a.seq - b.seq);

    let sigIdx = 0;
    for (const aw of awaits) {
      let timerSeq = aw.dueMs !== undefined ? this.resolveStreamSeq(aw.seq) : undefined;
      if (timerSeq !== undefined && !this.beforeCancel(timerSeq)) timerSeq = undefined; // fired after cancel
      const sig = signals[sigIdx];
      // The signal wins this await iff one is available and it landed no later than the timeout.
      if (sig !== undefined && (timerSeq === undefined || sig.streamSeq <= timerSeq)) {
        if (aw.seq === seq) return { kind: "signal", data: sig.data, streamSeq: sig.streamSeq };
        sigIdx += 1; // an earlier await consumed this signal — advance the FIFO cursor
      } else if (aw.seq === seq) {
        return timerSeq !== undefined
          ? { kind: "timeout", streamSeq: timerSeq }
          : { kind: "pending" };
      }
      // else: an earlier await that consumed nothing (timed out / still pending) — cursor unchanged
    }
    return { kind: "pending" };
  }

  /** Stream-seq of the `resolve` (timeout timer) for the command at `seq`, if it has fired. */
  private resolveStreamSeq(seq: number): number | undefined {
    return this.entries.find((l) => l.entry.kind === "resolve" && l.entry.resolvesSeq === seq)
      ?.streamSeq;
  }
  startCall<I, O>(workflow: Workflow<I, O>, input: I, opts?: CallOptions): DurableFuture<O> {
    return this.startCallImpl("ctx.startCall", workflow, input, opts);
  }

  /** ctx.call = start a child + await it (the blocking sugar over a single durable future). */
  call<I, O>(workflow: Workflow<I, O>, input: I, opts?: CallOptions): Promise<O> {
    return this.awaitFuture(this.startCallImpl("ctx.call", workflow, input, opts));
  }

  private startCallImpl<I, O>(
    op: string,
    workflow: Workflow<I, O>,
    input: I,
    opts: CallOptions | undefined,
  ): DurableFutureImpl<O> {
    this.assertNotInStep(op);
    const seq = this.seq++;
    return this.issue<O>(seq, "call", workflow.name, this.armCall(seq, workflow, input, opts));
  }

  /** Journal the call command (fresh) or validate it (replay), then idempotently (re-)start the child. */
  private async armCall<I, O>(
    seq: number,
    workflow: Workflow<I, O>,
    input: I,
    opts: CallOptions | undefined,
  ): Promise<AppendStatus> {
    const cmd = this.findAt(seq, "call") as CallEntry | undefined;
    if (!cmd) {
      // Fresh command under a journaled cancel: hard-stop before journaling intent or starting a
      // NEW child — a cancelled run must not spawn work (see armSleep for the "ok" convention).
      if (this.cancelPending()) {
        this.halt({ tag: "cancelled" });
        return "ok";
      }
      this.assertKind(seq, "call");
      const childRunId = opts?.runId ?? `${this.runId}_${seq}`;
      const r = await this.appendCAS(
        stamp(
          { kind: "call", seq, name: workflow.name, childRunId, wf: workflow.name, input },
          this.clock(),
        ),
        msgId.command(this.runId, seq),
      );
      if (r !== "ok") return r; // fenced / retry / fail (child input not serializable)
      return this.ensureChild(childRunId, workflow.name, workflow.version, input, seq);
    }
    if (cmd.wf !== workflow.name) {
      throw new NonDeterminismError(
        this.runId,
        seq,
        `expected call to "${cmd.wf}", got "${workflow.name}"`,
      );
    }
    if (this.findResolve(seq)) return "ok"; // already finished — no need to (re-)start the child
    return this.ensureChild(cmd.childRunId, cmd.wf, workflow.version, cmd.input, seq);
  }

  /** Idempotently (re-)start a child run; an infra failure → retry (never fails the parent). */
  private async ensureChild(
    childRunId: string,
    wf: string,
    version: number,
    input: unknown,
    seq: number,
  ): Promise<AppendStatus> {
    try {
      await this.children.start({
        childRunId,
        workflow: wf,
        version,
        input,
        parent: { runId: this.runId, seq, wf: this.workflow },
      });
      return "ok";
    } catch (e) {
      this.retryError = e; // starting a child is infrastructure — retry, don't fail the run
      return "retry";
    }
  }

  /**
   * Track a branch's arm, then hand back its future. The arm is already running (a promise is eager);
   * what tracking buys is {@link flushAppends} waiting for the branch's SIDE EFFECT — the child start,
   * the schedule — and not just for the command append that rides the chain.
   */
  private issue<T>(
    seq: number,
    kind: "call" | "sleep" | "await",
    name: string,
    armed: Promise<AppendStatus>,
  ): DurableFutureImpl<T> {
    this.arms.push(armed);
    // A future nobody ever awaits must not take the process down: an arm that throws (a divergence at
    // a recorded position) would otherwise be an unhandled rejection. This only marks it handled — the
    // promise still rejects for awaitFuture/race.
    void armed.catch(() => undefined);
    return new DurableFutureImpl<T>(this, seq, kind, name, armed);
  }

  /**
   * Resolve a durable future: wait for its command to be journaled + side-effect armed, then return the
   * recorded result, throw a failed child's error, or suspend if still pending. The thenable
   * (`await f` / `Promise.all` / `Promise.allSettled`) routes here; ctx.call is start + this.
   */
  async awaitFuture<T>(f: DurableFutureImpl<T>): Promise<T> {
    const status = await f.armed;
    if (status === "fenced") return this.haltFenced<T>();
    if (status === "retry") return this.haltRetry<T>();
    if (status === "fail") return this.haltFail<T>();
    return this.settle<T>(this.resolutionOf(f), f);
  }

  /**
   * Race durable futures — the safe alternative to native Promise.race (which would suspend on a pending
   * branch ordered before a ready one). Resolves with the JOURNAL-EARLIEST settled branch (by stream-seq
   * → deterministic across replays), throwing if that branch failed/timed out; suspends only while every
   * branch is pending. Losing branches keep running (their late results are journaled and ignored).
   */
  async race<F extends DurableFuture<unknown>>(futures: F[]): Promise<Awaited<F>> {
    if (futures.length === 0) {
      throw new WorkflowFailure(this.runId, "ctx.race requires at least one future");
    }
    const fs = futures as unknown as DurableFutureImpl<unknown>[];
    const statuses = await Promise.all(fs.map((f) => f.armed));
    if (statuses.includes("fenced")) return this.haltFenced<Awaited<F>>();
    if (statuses.includes("retry")) return this.haltRetry<Awaited<F>>();
    if (statuses.includes("fail")) return this.haltFail<Awaited<F>>();
    let winner: { at: number; res: Resolution; f: DurableFutureImpl<unknown> } | undefined;
    for (const f of fs) {
      const res = this.resolutionOf(f);
      if (res.state === "pending") continue;
      if (winner === undefined || res.streamSeq < winner.at) winner = { at: res.streamSeq, res, f };
    }
    if (winner === undefined) return this.haltSuspend<Awaited<F>>(); // all pending — wakes armed
    return this.settle<Awaited<F>>(winner.res, winner.f);
  }

  /** Turn a settled Resolution into a value, or throw the branch's failure/timeout (await + race share this). */
  private settle<T>(
    res: Resolution,
    f: { name: string; seq: number; kind: string },
  ): Promise<T> | T {
    if (res.state === "pending") return this.haltSuspend<T>();
    if (res.state === "value") return res.value as T;
    if (res.state === "timeout") {
      throw new DurableTimeoutError(
        `run ${this.runId}: ${f.kind === "await" ? `await "${f.name}"` : `"${f.name}"`} timed out (command ${f.seq})`,
      );
    }
    throw new WorkflowFailure(
      this.runId,
      `sub-workflow "${f.name}" failed: ${res.error.name}: ${res.error.message}`,
    );
  }

  /** The settled state of a future's command + the resolving entry's stream-seq (drives race ordering). */
  private resolutionOf<T>(f: DurableFutureImpl<T>): Resolution {
    if (f.kind === "call" || f.kind === "sleep") {
      const l = this.entries.find(
        (e) =>
          e.entry.kind === "resolve" &&
          e.entry.resolvesSeq === f.seq &&
          this.beforeCancel(e.streamSeq),
      );
      if (!l) return { state: "pending" };
      const re = l.entry as ResolveEntry;
      if (re.error) return { state: "error", streamSeq: l.streamSeq, error: re.error };
      return {
        state: "value",
        streamSeq: l.streamSeq,
        value: f.kind === "sleep" ? undefined : re.value,
      };
    }
    const r = this.resolveAwait(f.seq, f.name);
    if (r.kind === "pending") return { state: "pending" };
    if (r.kind === "signal") return { state: "value", streamSeq: r.streamSeq, value: r.data };
    return { state: "timeout", streamSeq: r.streamSeq };
  }

  // ---------------------------------------------------------------- continue-as-new

  continueAsNew(input: unknown, opts?: { version?: number }): Promise<never> {
    this.assertNotInStep("ctx.continueAsNew");
    if (this.inCleanup) {
      // Same rule as suspending ops: cleanup is LOCAL. Halting from inside a handler would return a
      // never-settling promise INTO runCleanup and hang the cancel forever — throw instead (caught
      // best-effort by runCleanup, like an illegal suspend).
      throw new DurableError(
        `run ${this.runId}: an onCancel handler may not call continueAsNew (use local ctx.run steps only)`,
      );
    }
    if (opts?.version !== undefined && (!Number.isInteger(opts.version) || opts.version < 1)) {
      throw new Error("ctx.continueAsNew: version must be a positive integer");
    }
    if (this.cancelAt !== undefined) {
      // Cancel requested before this generation rolled over → cancel in place; the next generation is
      // never started. (A cancel arriving AFTER `continued` is forwarded to the next gen by the worker.)
      this.halt({ tag: "cancelled" });
      return never();
    }
    // Deterministic next-generation id: identical for any worker replaying this run, so racing
    // workers agree without coordination. The executor writes the `continued` terminal + starts it.
    // The next generation runs `opts.version` if given (the in-flight migration lever), else this
    // generation's version (so a cron-style CAN loop never silently jumps versions on redeploy).
    const generation = this.generation + 1;
    this.continueSpec = {
      nextRunId: `${this.base}_g${generation}`,
      input,
      generation,
      base: this.base,
      version: opts?.version ?? this.version,
    };
    this.halt({ tag: "continue" });
    return never();
  }

  /** The next generation to start (set by continueAsNew; read by the executor on a continue outcome). */
  nextGenerationSpec(): ContinueSpec {
    invariant(
      this.continueSpec !== undefined,
      `run ${this.runId}: continue outcome without a spec`,
    );
    return this.continueSpec;
  }

  /** Append the terminal `continued` entry (executor-driven, after the fully-consumed guard). */
  async appendTerminalContinue(): Promise<"ok" | "fenced" | "retry" | "fail" | "adopted"> {
    invariant(this.continueSpec !== undefined, `run ${this.runId}: continue without a spec`);
    const s = this.continueSpec;
    const r = await this.appendTerminalCAS(
      stamp(
        {
          kind: "continued",
          seq: NON_COMMAND_SEQ,
          nextRunId: s.nextRunId,
          input: s.input,
          generation: s.generation,
          base: s.base,
          version: s.version,
        },
        this.clock(),
      ),
    );
    if (r === "retry") return "retry";
    if (r === "fail") return "fail"; // continue-as-new input not serializable
    // Confirm the terminal is actually a `continued`. The terminal msgID is shared with result/fail,
    // so a deduped append (or a lost CAS) can report "ok" without our entry landing; only continue if
    // the journal truly records a `continued`.
    const reloaded = await this.journal.load(this.runId);
    if (!reloaded.some((l) => l.entry.kind === "continued")) return "fenced";
    // A `continued` exists but WE lost the CAS: adopt it, exactly as the result/fail/cancelled paths
    // do. `nextRunId` is deterministic, but the rollover INPUT and the target VERSION are not — they
    // come from this incarnation, and two incarnations racing the boundary can compute different
    // ones. Reporting our own would let the worker start the next generation from a value the
    // journal does not record, which is the journal quietly ceasing to be the source of truth.
    // ("ok" here cannot be a dedup of somebody else's terminal: a `continued` is the last entry, so
    // if one exists the tail moved and our CAS could not have matched.)
    return r === "ok" ? "ok" : "adopted";
  }

  // ---------------------------------------------------------------- misc surface

  key(): string {
    return `${this.runId}:${this.currentStep() ?? 0}`;
  }

  get<T>(key: string): T {
    const v = this.deps[key];
    if (v === undefined) throw new Error(`ctx.get: no dependency registered for "${key}"`);
    return v as T;
  }

  assert(condition: boolean, message?: string): void {
    if (!condition) throw new WorkflowFailure(this.runId, message ?? "ctx.assert failed");
  }

  // ---------------------------------------------------------------- terminal (executor-driven)

  async appendTerminalResult(
    value: unknown,
  ): Promise<"ok" | "fenced" | "retry" | "fail" | "adopted"> {
    const r = await this.appendTerminalCAS(
      stamp({ kind: "result", seq: NON_COMMAND_SEQ, value }, this.clock()),
    );
    if (r === "retry") return "retry";
    if (r === "fail") return "fail"; // workflow return value not serializable
    if (r === "ok") return "ok";
    return this.reconcileTerminal(); // CAS lost — adopt the canonical terminal, or genuinely fence
  }
  async appendTerminalFail(error: {
    name: string;
    message: string;
  }): Promise<"ok" | "fenced" | "retry" | "adopted"> {
    const r = await this.appendTerminalCAS(
      stamp({ kind: "fail", seq: NON_COMMAND_SEQ, error }, this.clock()),
    );
    if (r === "retry") return "retry";
    if (r === "ok") return "ok";
    return this.reconcileTerminal();
  }

  /** Append the terminal `cancelled` entry (executor-driven, after a cancel hard-stop at a frontier). */
  async appendTerminalCancelled(): Promise<"ok" | "fenced" | "retry" | "adopted"> {
    const body =
      this.cancelReason !== undefined
        ? { kind: "cancelled" as const, seq: NON_COMMAND_SEQ, reason: this.cancelReason }
        : { kind: "cancelled" as const, seq: NON_COMMAND_SEQ };
    const r = await this.appendTerminalCAS(stamp(body, this.clock()));
    if (r === "retry") return "retry";
    if (r === "ok") return "ok";
    return this.reconcileTerminal();
  }

  /**
   * After a terminal CAS-loss: "adopted" if a canonical terminal now exists (the executor reads the
   * journaled value/kind via {@link canonicalTerminal} — never the loser's own), else "fenced".
   * Mirrors the step-end reconcile: the journal is the truth, so the loser must adopt, not self-report.
   */
  private async reconcileTerminal(): Promise<"adopted" | "fenced"> {
    return (await this.canonicalTerminal()) !== undefined ? "adopted" : "fenced";
  }

  /** The journal's terminal entry (result/fail/continued/cancelled) on a fresh load — the canonical outcome. */
  async canonicalTerminal(): Promise<JournalEntry | undefined> {
    const reloaded = await this.journal.load(this.runId);
    return reloaded.find(
      (l) =>
        l.entry.kind === "result" ||
        l.entry.kind === "fail" ||
        l.entry.kind === "continued" ||
        l.entry.kind === "cancelled",
    )?.entry;
  }

  // ---------------------------------------------------------------- internals

  private appendCAS(entry: JournalEntry, mid: string): Promise<AppendStatus> {
    // Short-circuit ONLY after a real fence (a zombie that lost the CAS): the journal advanced under
    // us so every further append would fence anyway. A fan-out SUSPEND must fall through — a concurrent
    // branch's command-append is legitimate intent we still want journaled.
    return this.enqueueAppend(async () => (this.fenced ? "fenced" : this.appendCASRaw(entry, mid)));
  }

  /**
   * Append a terminal entry (result/fail/continued). Bypasses the halt guard because continue-as-new
   * halts the ctx BEFORE the executor writes the `continued` terminal — the write is executor-driven
   * and intentional, never a stray append from suspended user code.
   */
  private appendTerminalCAS(entry: JournalEntry): Promise<AppendStatus> {
    return this.enqueueAppend(async () => {
      // ONE terminal per run is the CAS's job, and the CAS can only do it if we never aim it past a
      // terminal we already know about. Our view can hold one: the step-end reconcile adopts a
      // fresh journal, and the incarnation that beat us to that step may have gone on to finish
      // the run. Appending anyway would pass the CAS (the terminal is at or before our tail),
      // leaving only the shared terminal msgID between the run and a second terminal — and a
      // "duplicate" ack reads as "mine landed", so we would report our OWN outcome. Report a lost
      // race instead; the caller adopts the journal's terminal.
      if (this.entries.some((l) => TERMINAL_KINDS.has(l.entry.kind))) return "fenced";
      return this.appendCASRaw(entry, msgId.terminal(this.runId));
    });
  }

  /**
   * Serialize an append behind any in-flight appends for this run. seqs are assigned synchronously at
   * the call site, but each CAS (read `lastSeq` → append → advance `lastSeq`) must run one-at-a-time:
   * two concurrent fan-out ops would otherwise read the same `lastSeq` and the loser would self-fence.
   * Outcomes are swallowed onto the chain so one append never rejects the next.
   */
  private enqueueAppend(run: () => Promise<AppendStatus>): Promise<AppendStatus> {
    const result = this.appendChain.then(run, run);
    this.appendChain = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private async appendCASRaw(entry: JournalEntry, mid: string): Promise<AppendStatus> {
    try {
      const r = await this.journal.append(this.runId, {
        entry,
        msgID: mid,
        expectLastSeq: this.lastSeq,
      });
      if (!this.entries.some((l) => l.streamSeq === r.streamSeq)) {
        this.entries.push({ entry, streamSeq: r.streamSeq });
      }
      if (r.streamSeq > this.lastSeq) this.lastSeq = r.streamSeq;
      return "ok";
    } catch (e) {
      if (e instanceof FencedError) {
        this.fenced = true; // a real lost-CAS: short-circuit the rest of the append queue
        return "fenced";
      }
      if (e instanceof SerializationError) {
        // Deterministic: re-encoding the same value always fails, so retrying is futile — fail loud.
        this.fatal = { name: e.name, message: e.message };
        return "fail";
      }
      // Any other error is infrastructure (e.g. a transient publish failure): retry, don't fail the run.
      this.retryError = e;
      return "retry";
    }
  }

  private haltFenced<T>(): Promise<T> {
    this.halt({ tag: "fenced" });
    return never();
  }

  /**
   * True when a journaled `cancel-requested` must hard-stop a FRESH (or resumed) command: the cancel
   * point is the next command boundary, so no new intent is appended and no step fn executes once
   * cancellation is journaled. `ctx.onCancel` cleanup steps are exempt (they run AFTER the halt, in
   * the non-cancellable cleanup context). Recorded commands are unaffected — they replay, and an op
   * resolved before the cancel (stream-seq < cancelAt) still delivers its value.
   */
  private cancelPending(): boolean {
    return this.cancelAt !== undefined && !this.inCleanup;
  }

  private haltCancelled<T>(): Promise<T> {
    this.halt({ tag: "cancelled" });
    return never();
  }

  private haltRetry<T>(): Promise<T> {
    this.halt({ tag: "retry", error: this.retryError });
    return never();
  }

  private haltFail<T>(): Promise<T> {
    invariant(this.fatal !== undefined, `run ${this.runId}: fail-halt without a fatal error`);
    this.halt({ tag: "fail", error: this.fatal });
    return never();
  }

  /** The infra error that triggered a retry-halt (for the executor to report). */
  lastError(): unknown {
    return this.retryError;
  }

  /** The deterministic serialization error that failed a terminal append (for the executor to report). */
  fatalError(): { name: string; message: string } {
    invariant(this.fatal !== undefined, `run ${this.runId}: fatalError() without a fatal error`);
    return this.fatal;
  }

  /** Register a compensation handler, run (LIFO) if this run is cancelled, before the terminal. */
  onCancel(fn: () => void | Promise<unknown>): void {
    this.cancelHandlers.push(fn);
  }

  /**
   * Run the registered onCancel handlers (LIFO) in a non-cancellable cleanup context, before the executor
   * writes the `cancelled` terminal. Handler errors are best-effort (swallowed) — the run still cancels.
   * v0.2 cleanup is LOCAL-ONLY: a handler that attempts to suspend throws (and is caught here).
   *
   * Returns the halt that ABORTED the cleanup, if one did. A cleanup step journals like any other,
   * so its append can lose the CAS or hit an infrastructure error — and the run is already halted
   * (`cancelled` is how we got here), so that second halt cannot reach the executor through
   * {@link haltPromise}. Without this the step's never-settling promise was returned INTO the
   * handler this method awaits: one transient publish error during a cancel and `executeRun` never
   * returned — the worker held the item, the lease and a concurrency slot for good. A `fenced` or
   * `retry` abort ends the cleanup (the redelivery replays it from the journal); an unencodable
   * step result only ends its own handler, like any other handler error.
   */
  async runCleanup(): Promise<Halt | undefined> {
    if (this.cancelHandlers.length === 0) return undefined;
    this.inCleanup = true;
    try {
      for (let i = this.cancelHandlers.length - 1; i >= 0; i--) {
        const handler = this.cancelHandlers[i];
        const ran = Promise.resolve()
          .then(() => handler?.())
          .then(
            () => undefined,
            // best-effort: a handler error (incl. an illegal suspend) never blocks the cancel
            () => undefined,
          );
        const aborted = await new Promise<Halt | undefined>((resolve) => {
          this.abortCleanupHandler = resolve;
          void ran.then(() => resolve(undefined));
        });
        if (aborted !== undefined && aborted.tag !== "fail") return aborted;
      }
      return undefined;
    } finally {
      this.inCleanup = false;
      this.abortCleanupHandler = undefined;
    }
  }

  private haltSuspend<T>(): Promise<T> {
    if (this.inCleanup) {
      // v0.2: onCancel handlers run a non-cancellable, LOCAL cleanup — suspending is rejected.
      throw new DurableError(
        `run ${this.runId}: an onCancel handler may not suspend (use local ctx.run steps only)`,
      );
    }
    // A pending op while cancellation is requested IS the cancel point — an uncatchable hard-stop.
    this.halt(this.cancelAt !== undefined ? { tag: "cancelled" } : { tag: "suspend" });
    return never();
  }

  private findResolve(seq: number): ResolveEntry | undefined {
    const l = this.entries.find((e) => e.entry.kind === "resolve" && e.entry.resolvesSeq === seq);
    return l?.entry as ResolveEntry | undefined;
  }

  private hasResolve(seq: number): boolean {
    return this.findResolve(seq) !== undefined;
  }

  /** A journaled entry counts toward resolution only if it landed before a cancel froze the run. */
  private beforeCancel(streamSeq: number): boolean {
    return this.cancelAt === undefined || streamSeq < this.cancelAt;
  }

  private halt(h: Halt): void {
    // Cleanup runs AFTER the `cancelled` halt, so a halt raised by one of its steps finds the run
    // already halted; it still has to stop the cleanup (see runCleanup).
    if (this.inCleanup) this.abortCleanupHandler?.(h);
    if (this.halted) return;
    this.halted = true;
    this.resolveHalt(h);
  }

  private adopt(reloaded: LoadedEntry[]): void {
    this.entries = reloaded.map((l) => ({ ...l }));
    this.lastSeq = this.entries.at(-1)?.streamSeq ?? this.lastSeq;
  }

  private findAt(seq: number, kind: EntryKind): JournalEntry | undefined {
    return this.entries.find((l) => l.entry.seq === seq && l.entry.kind === kind)?.entry;
  }

  /** The command kind recorded at this position, if any (ignores start/step-end/resolve/etc.). */
  private commandKindAt(seq: number): EntryKind | undefined {
    for (const l of this.entries) {
      if (l.entry.seq === seq && COMMAND_KINDS.has(l.entry.kind)) return l.entry.kind;
    }
    return undefined;
  }

  /** Fail loud if a different command kind was recorded at a fresh position (replay divergence). */
  private assertKind(seq: number, expected: EntryKind): void {
    const found = this.commandKindAt(seq);
    if (found !== undefined && found !== expected) {
      throw new NonDeterminismError(
        this.runId,
        seq,
        `expected a "${expected}" but seq ${seq} recorded a "${found}"`,
      );
    }
  }

  /** After the workflow returns, ensure it consumed every recorded command (no shorter replay path). */
  assertFullyConsumed(): void {
    let maxCmd = -1;
    for (const l of this.entries) {
      if (COMMAND_KINDS.has(l.entry.kind) && l.entry.seq > maxCmd) maxCmd = l.entry.seq;
    }
    if (maxCmd >= this.seq) {
      throw new NonDeterminismError(
        this.runId,
        this.seq,
        `replay consumed ${this.seq} command(s) but the journal recorded through seq ${maxCmd} (workflow took a shorter path)`,
      );
    }
  }

  /**
   * The seq of OUR step executing on this async call chain (it scopes per-fn even when sibling steps
   * run concurrently — a plain counter would conflate a nested ctx.* call with a concurrent sibling).
   * Drives ctx.key() and the nesting guard; undefined = not inside one of this ctx's steps.
   */
  private currentStep(): number | undefined {
    const scope = STEP_SCOPE.getStore();
    return scope?.ctx === this ? scope.stepSeq : undefined;
  }

  /** Reject a ctx command issued inside a ctx.run step fn (it would interleave command seqs). */
  private assertNotInStep(op: string): void {
    if (this.currentStep() !== undefined) {
      throw new StepNestingError(
        this.runId,
        `run ${this.runId}: ${op} cannot be called inside a ctx.run step function`,
      );
    }
  }

  private assertName(seq: number, recorded: string | undefined, actual: string): void {
    invariant(recorded !== undefined, `run ${this.runId}: missing step-begin at seq ${seq}`);
    if (recorded !== actual) {
      throw new NonDeterminismError(
        this.runId,
        seq,
        `expected step "${recorded}", got "${actual}"`,
      );
    }
  }
}

/**
 * The concrete durable future returned by ctx.start*. A thenable: awaiting it (directly, or via
 * Promise.all / Promise.allSettled) routes through ctx.awaitFuture, which resolves it from the journal
 * or suspends the run. Carries the command seq/kind/name so combinators (ctx.race) can inspect it.
 */
class DurableFutureImpl<T> implements DurableFuture<T> {
  readonly __durable = true as const;
  constructor(
    private readonly ctx: CtxImpl,
    readonly seq: number,
    readonly kind: "call" | "sleep" | "await",
    readonly name: string,
    /** Resolves once the command is journaled + side-effect (re-)armed: "ok" | "fenced" | "retry" | "fail". */
    readonly armed: Promise<AppendStatus>,
  ) {}

  then<R1 = T, R2 = never>(
    onFulfilled?: ((value: T) => R1 | PromiseLike<R1>) | null,
    onRejected?: ((reason: unknown) => R2 | PromiseLike<R2>) | null,
  ): Promise<R1 | R2> {
    return this.ctx.awaitFuture(this).then(onFulfilled, onRejected);
  }
}

function retryDelay(retry: RetryPolicy, attempt: number): number {
  if (retry.kind === "fixed") return retry.delayMs;
  if (retry.kind === "exponential") {
    const factor = retry.factor ?? 2;
    const d = retry.baseMs * Math.pow(factor, attempt - 1);
    return retry.maxMs ? Math.min(d, retry.maxMs) : d;
  }
  return 0;
}
