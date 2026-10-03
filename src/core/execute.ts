// @synadia-ai/durable — the replay executor. Drives a workflow function to its next halt point
// (completion, failure, suspension, or fencing) against a pre-loaded journal. Pure: depends only on
// the Journal interface, so the whole crash/replay/concurrency story is unit-testable without NATS.

import { invariant } from "../internal/assert.js";
import { type AnyWorkflow, type WorkflowFn } from "../types.js";
import { type JournalEntry, type LoadedEntry, type StartEntry } from "../journal/entry.js";
import { type Journal } from "../journal/store.js";
import { type Children, UNAVAILABLE_CHILDREN } from "./children.js";
import { type ContinueSpec, CtxImpl } from "./ctx.js";
import { type Scheduler, UNAVAILABLE_SCHEDULER } from "./scheduler.js";

export type ExecOutcome =
  | { kind: "completed"; value: unknown }
  | { kind: "failed"; error: { name: string; message: string } }
  | { kind: "suspended" }
  | { kind: "fenced" }
  | { kind: "retry"; error: unknown }
  | { kind: "continue"; next: ContinueSpec }
  | { kind: "cancelled" };

export interface ExecuteOptions {
  workflow: AnyWorkflow;
  journal: Journal;
  /** The full ordered journal for this run (must include the `start` entry). */
  loaded: LoadedEntry[];
  dependencies?: Record<string, unknown>;
  /** Injectable clock for the frontier value-captures (default Date.now). */
  clock?: () => number;
  attempt?: number;
  /** Wake scheduler for ctx.sleep (worker-provided; defaults to one that errors if sleep is used). */
  scheduler?: Scheduler;
  /** Child starter for ctx.call (worker-provided; defaults to one that errors if call is used). */
  children?: Children;
}

export async function executeRun(runId: string, o: ExecuteOptions): Promise<ExecOutcome> {
  const startL = o.loaded.find((l) => l.entry.kind === "start");
  invariant(startL, `run ${runId}: cannot execute — missing start entry`);
  const start = startL.entry as StartEntry;

  // A journal that already records this run's outcome is NEVER executed: report what it says. The
  // worker checks for a terminal when it first loads, but the journal it hands over here is a LATER
  // load — after recording an inbound event, after taking the lease — and another worker may have
  // finished the run in between. Replaying it anyway is not harmless: the terminal append would
  // pass its CAS (the terminal is at or before our tail) and only the shared terminal msgID would
  // stand between the run and a SECOND terminal — for as long as the duplicate window lasts — and
  // a "duplicate" ack reads as "mine landed", so this execution would report its OWN outcome over
  // the journal's. With a `cancel-requested` recorded after a `continued`, that is a run reported
  // (and cascaded, and delivered to its parent) as cancelled while its next generation runs on,
  // with its onCancel compensation fired for good measure.
  const recorded = o.loaded.find((l) => isTerminalEntry(l.entry));
  if (recorded) return outcomeOfTerminal(recorded.entry, o.workflow.version);

  const ctx = new CtxImpl({
    runId,
    workflow: o.workflow,
    journal: o.journal,
    loaded: o.loaded,
    dependencies: o.dependencies ?? {},
    clock: o.clock ?? Date.now,
    attempt: o.attempt ?? 1,
    scheduler: o.scheduler ?? UNAVAILABLE_SCHEDULER,
    children: o.children ?? UNAVAILABLE_CHILDREN,
    generation: start.generation ?? 0,
    base: start.base ?? runId,
  });

  // The registry erases the concrete input type; bridge it once here.
  const fn = o.workflow.__fn as unknown as WorkflowFn<unknown, unknown>;
  const runP = Promise.resolve()
    .then(() => fn(ctx, start.input))
    .then(
      (value) => ({ tag: "return" as const, value }),
      (error: unknown) => ({ tag: "throw" as const, error }),
    );

  const race = await Promise.race([runP, ctx.haltPromise()]);

  // Drain the append queue BEFORE reporting any outcome: the worker acks/NAKs the instant we
  // return, and a fan-out append still in flight past that point could be lost to a crash after
  // the ack — with nothing left to redeliver it (a `suspended` ack is final). Terminal paths
  // already order behind the chain (they append through it); the early-exit outcomes are the ones
  // that would otherwise race it. On a fence the chain short-circuits, so this stays cheap.
  await ctx.flushAppends();

  if (race.tag === "suspend") return { kind: "suspended" };
  if (race.tag === "fenced") return { kind: "fenced" };
  if (race.tag === "cancelled") {
    // Run opt-in onCancel compensation before the terminal. A cleanup step that lost its CAS or
    // hit an infrastructure error ends this execution like any other step would: no terminal, the
    // item redelivers, and the replay picks the cleanup up from the journal.
    const aborted = await ctx.runCleanup();
    if (aborted !== undefined) {
      await ctx.flushAppends();
      return aborted.tag === "retry" ? { kind: "retry", error: aborted.error } : { kind: "fenced" };
    }
    return cancelTerminal(ctx);
  }
  if (race.tag === "retry") return { kind: "retry", error: race.error };
  // An in-band serialization failure (a step result or child input that cannot be encoded): the value
  // is deterministically unencodable, so fail the run terminally rather than retry it forever.
  if (race.tag === "fail") return failTerminal(ctx, race.error);

  if (race.tag === "continue") {
    // Fail loud (unswallowable — outside user code) if continue-as-new was reached via a shorter
    // path than the journal recorded. The `continued` terminal is written here, never in user code.
    try {
      ctx.assertFullyConsumed();
    } catch (e) {
      return failTerminal(ctx, toErrorShape(e));
    }
    const cr = await ctx.appendTerminalContinue();
    if (cr === "retry") return { kind: "retry", error: ctx.lastError() };
    if (cr === "fail") return failTerminal(ctx, ctx.fatalError()); // continue input not serializable
    if (cr === "fenced") return { kind: "fenced" };
    if (cr === "adopted") return adoptCanonical(ctx); // the winner's rollover, never our own
    return { kind: "continue", next: ctx.nextGenerationSpec() };
  }

  if (race.tag === "throw") {
    return failTerminal(ctx, toErrorShape(race.error));
  }

  // tag === "return": fail loud if the workflow consumed fewer commands than recorded (shorter path).
  try {
    ctx.assertFullyConsumed();
  } catch (e) {
    return failTerminal(ctx, toErrorShape(e));
  }
  const r = await ctx.appendTerminalResult(race.value);
  if (r === "retry") return { kind: "retry", error: ctx.lastError() };
  if (r === "fail") return failTerminal(ctx, ctx.fatalError()); // return value not serializable
  if (r === "fenced") return { kind: "fenced" };
  if (r === "adopted") return adoptCanonical(ctx);
  return { kind: "completed", value: race.value }; // r === "ok"
}

/** Write a terminal `fail` entry and map the append result to an ExecOutcome (the shared failure path). */
async function failTerminal(
  ctx: CtxImpl,
  shape: { name: string; message: string },
): Promise<ExecOutcome> {
  const r = await ctx.appendTerminalFail(shape);
  if (r === "retry") return { kind: "retry", error: ctx.lastError() };
  if (r === "adopted") return adoptCanonical(ctx);
  return r === "fenced" ? { kind: "fenced" } : { kind: "failed", error: shape };
}

/** Write a terminal `cancelled` entry and map the append result to an ExecOutcome (the cancel hard-stop). */
async function cancelTerminal(ctx: CtxImpl): Promise<ExecOutcome> {
  const r = await ctx.appendTerminalCancelled();
  if (r === "retry") return { kind: "retry", error: ctx.lastError() };
  if (r === "adopted") return adoptCanonical(ctx);
  return r === "fenced" ? { kind: "fenced" } : { kind: "cancelled" };
}

/**
 * A terminal CAS-loss where a canonical terminal already exists: adopt the JOURNALED outcome (the
 * winner's value/kind), not this incarnation's own — the journal is the source of truth. Matches the
 * step-end adopt, and keeps the worker's KV finalize consistent with what result() reads.
 */
async function adoptCanonical(ctx: CtxImpl): Promise<ExecOutcome> {
  const t = await ctx.canonicalTerminal();
  if (t === undefined) return { kind: "fenced" }; // the terminal vanished — treat as a plain fence
  return outcomeOfTerminal(t, ctx.version);
}

export function isTerminalEntry(e: JournalEntry): boolean {
  return (
    e.kind === "result" || e.kind === "fail" || e.kind === "continued" || e.kind === "cancelled"
  );
}

/** The outcome a journaled terminal entry stands for — the only outcome a finished run has. */
function outcomeOfTerminal(t: JournalEntry, currentVersion: number): ExecOutcome {
  if (t.kind === "result") return { kind: "completed", value: t.value };
  if (t.kind === "fail") return { kind: "failed", error: t.error };
  if (t.kind === "continued") {
    return {
      kind: "continue",
      next: {
        nextRunId: t.nextRunId,
        input: t.input,
        generation: t.generation,
        base: t.base,
        // The recorded next-gen version; pre-versioning entries → this run's version.
        version: t.version ?? currentVersion,
      },
    };
  }
  if (t.kind === "cancelled") return { kind: "cancelled" };
  return { kind: "fenced" }; // not a terminal kind (unreachable)
}

export function toErrorShape(e: unknown): { name: string; message: string } {
  if (e instanceof Error) return { name: e.name, message: e.message };
  return { name: "Error", message: String(e) };
}
