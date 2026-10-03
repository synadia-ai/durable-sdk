// @synadia-ai/durable — public types (pure; no runtime imports).
// See DESIGN.md §2 for the rationale behind each surface.

/** A workflow is a normal async function over a deterministic context. */
export type WorkflowFn<I, O> = (ctx: Ctx, input: I) => Promise<O>;

/** A registered workflow definition. Used for registration AND typed start/call. */
export interface Workflow<I, O> {
  readonly name: string;
  readonly version: number;
  /** @internal The implementation; not part of the documented API. */
  readonly __fn: WorkflowFn<I, O>;
}

/**
 * The any-free "top" type for a heterogeneous workflow collection (registry, worker list). A concrete
 * `Workflow<I, O>` is assignable to this: `never` in the contravariant input position accepts any
 * input type, `unknown` in the covariant output position accepts any result.
 */
export type AnyWorkflow = Workflow<never, unknown>;

export type RetryPolicy =
  | { kind: "none" }
  | { kind: "fixed"; attempts: number; delayMs: number }
  | { kind: "exponential"; attempts: number; baseMs: number; factor?: number; maxMs?: number };

export interface StepOptions {
  /** Inline retry of the step fn before the failure is journaled. Default: no retry. */
  retry?: RetryPolicy;
}

export interface CallOptions {
  /** Override the child runId (default is deterministic: `${parentRunId}_${seq}`). */
  runId?: string;
}

/**
 * A handle to a durable op started but not yet awaited (`ctx.startCall` / `startSleep` / `startAwait`).
 * It is a thenable, so `await f`, `Promise.all([...])`, and `Promise.allSettled([...])` all work. For
 * racing use `ctx.race([...])` — native `Promise.race` over futures is unsafe (a pending branch ordered
 * before a ready one would suspend the run before the ready branch can win).
 *
 * Always CONSUME a future — `await` it, combine it (`Promise.all` / `Promise.allSettled`), or `ctx.race`
 * it. Discarding one is unsupported: it does not block the run and its result is ignored, so the
 * workflow proceeds as if the branch had never happened. (Its command IS journaled and its child or
 * timer IS armed before the run reports an outcome — the executor drains the arm — but nothing ever
 * reads what it produced.) `ctx.call`/`sleep`/`await` are `await ctx.start*`, so the blocking forms
 * are always safe.
 */
export interface DurableFuture<T> extends PromiseLike<T> {
  /** @internal brand; durable futures are created only by `ctx.start*`. */
  readonly __durable: true;
}

/**
 * The deterministic execution context. Every nondeterministic or side-effecting operation goes
 * through a `ctx.*` call and is journaled. Anything through `ctx` is `await`ed.
 */
export interface Ctx {
  readonly runId: string;
  readonly workflow: string;
  readonly version: number;
  /**
   * Redelivery attempt of the current frontier (1 on first delivery). Inherently NONDETERMINISTIC:
   * it is not journaled, so a replay sees a different value than the execution it replays. Fine
   * inside a step fn (logging, attempt-aware alerting); branching workflow control flow on it is a
   * replay-divergence footgun.
   */
  readonly attempt: number;

  /** Journal a durable step (two-phase: intent-first, then result). The core primitive. */
  run<T>(name: string, fn: () => Promise<T> | T, opts?: StepOptions): Promise<T>;

  /** Deterministic builtins — each a single journaled value-capture. */
  now(): Promise<number>;
  random(): Promise<number>;
  uuid(): Promise<string>;

  /** Durable timer — suspend until `ms` have elapsed (native JetStream scheduling). See DESIGN.md §7. */
  sleep(name: string, ms: number): Promise<void>;

  /**
   * Block until an external signal of this name arrives (`client.signal`). With `opts.timeoutMs`, a
   * durable timer races the signal: if the deadline is reached first, the await throws
   * `DurableTimeoutError` (catchable in the workflow to take a fallback path).
   */
  await<T>(name: string, opts?: { timeoutMs?: number }): Promise<T>;

  /** Invoke a sub-workflow and resume with its result (a child failure fails the parent). */
  call<I, O>(workflow: Workflow<I, O>, input: I, opts?: CallOptions): Promise<O>;

  /**
   * Start a sub-workflow WITHOUT blocking, returning a durable future. Await it — or combine several
   * with `Promise.all` / `ctx.race` — to fan out children concurrently. `ctx.call` is `await ctx.startCall`.
   */
  startCall<I, O>(workflow: Workflow<I, O>, input: I, opts?: CallOptions): DurableFuture<O>;

  /** Start a durable timer without blocking; await the future, or race it for a timeout pattern. */
  startSleep(name: string, ms: number): DurableFuture<void>;

  /** Start waiting for a signal without blocking; await the future, or race it against others. */
  startAwait<T>(name: string, opts?: { timeoutMs?: number }): DurableFuture<T>;

  /**
   * Race durable futures — resolves with the first (journal-earliest) to settle, throwing if that branch
   * failed or timed out. The safe alternative to native `Promise.race` over futures. Losing branches keep
   * running (their results are journaled and ignored).
   */
  race<F extends DurableFuture<unknown>>(futures: F[]): Promise<Awaited<F>>;

  /**
   * End this generation and atomically start a fresh one with `input`, keeping the journal bounded for
   * long-running / looping workflows. Code after this call never runs (it suspends like a terminal op);
   * the new generation begins with an empty journal. The original `Handle.result()` transparently
   * follows the chain to the generation that finally returns.
   *
   * `opts.version` migrates the chain onto a different registered version of this workflow — the
   * in-flight migration lever for versioned deploys. It defaults to the current generation's version,
   * so a looping continue-as-new never silently jumps versions on redeploy. See DESIGN-versioning.md.
   */
  continueAsNew(input: unknown, opts?: { version?: number }): Promise<never>;

  /** The current step's idempotency key (`${runId}:${stepSeq}`) for user side effects. */
  key(): string;

  /** Resolve an injected dependency (NATS connection, http client, config, …). */
  get<T>(key: string): T;

  /** Invariant guard — fail the run loudly if the condition is false. Do not catch. */
  assert(condition: boolean, message?: string): void;

  /**
   * Register a compensation handler that runs (LIFO) if this run is cancelled, before it reaches the
   * `cancelled` terminal — the opt-in cleanup hook for the layered cancellation model. Handlers run in a
   * non-cancellable context; in v0.2 they may only do local `ctx.run` steps (no suspending ctx.* ops).
   */
  onCancel(fn: () => void | Promise<unknown>): void;
}

export type RunState =
  | "pending"
  | "running"
  | "suspended"
  | "blocked"
  | "completed"
  | "failed"
  | "continued"
  | "cancelled";

export interface RunStatus {
  state: RunState;
  workflow: string;
  version: number;
  updatedMs: number;
  /** For state "continued": the next generation's runId (continue-as-new chain). */
  next?: string;
  /** For state "blocked": a human-readable reason (e.g. "waiting for charge v2"). */
  detail?: string;
  /**
   * For state "suspended": when the run's earliest pending wake is due (epoch ms) — present only
   * when it is parked on a timer (`ctx.sleep`, or an `await` with `timeoutMs`), absent when it waits
   * on a signal or a child. A hint like the rest of this record: the journal holds the deadline.
   */
  dueMs?: number;
}

export interface StartOptions {
  /** The runId is the idempotency key for start. Default: a generated uuid. */
  runId?: string;
}

export interface Handle<O> {
  readonly runId: string;
  /** Resolve when the run reaches a terminal state; reject (WorkflowFailure) if it failed. */
  result(timeoutMs?: number): Promise<O>;
  status(): Promise<RunStatus>;
}
