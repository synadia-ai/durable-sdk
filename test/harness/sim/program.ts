// Generated workflows for the simulation: a tiny AST over the ctx surface, a generator that draws
// one from a seed, and an interpreter that turns it into a real `defineWorkflow` function.
//
// A program's COMMAND SEQUENCE is a function of the program and the journal — never of wall-clock,
// the incarnation, or an effect's return value — so the engine must never report a divergence for
// one; if it does, that is a finding. Its VALUES are deliberately incarnation-specific (an effect
// returns who ran it; the workflow's result and its continue-as-new input carry who computed them):
// two incarnations that race a step or a terminal then disagree about the value, which is what
// makes "the loser adopts the journal's value" an assertion rather than a coincidence.

import { defineWorkflow } from "../../../src/core/workflow.js";
import { type AnyWorkflow, type Ctx, type DurableFuture } from "../../../src/types.js";
import { type Rng } from "./rng.js";

export type Branch =
  | { kind: "call"; child: number }
  | { kind: "sleep"; ms: number }
  | { kind: "await"; timeoutMs?: number; signalled: boolean };

export type Op =
  | { op: "run"; throws: boolean; caught: boolean }
  | { op: "value"; kind: "now" | "random" | "uuid" }
  | { op: "sleep"; ms: number }
  | { op: "await"; timeoutMs?: number; signalled: boolean; caught: boolean }
  | { op: "call"; child: number; caught: boolean }
  | { op: "fanout"; branches: Branch[]; join: "all" | "allSettled" | "race"; caught: boolean }
  | { op: "onCancel" };

export interface Program {
  ops: Op[];
  /** How many times the run continues-as-new after its ops (0 = it returns). */
  generations: number;
}

/** What an interpreted workflow reaches the simulation through (`ctx.get("sim")`). */
export interface SimHooks {
  /** Who is executing: goes into every value this incarnation computes. */
  readonly incId: number;
  /** The body of every `ctx.run` step: records the firing, then waits on the world. */
  effect(key: string, name: string, throws: boolean): Promise<string>;
}

export interface SimInput {
  gen: number;
  /** Who computed this rollover (an incarnation id) — deliberately not journal-derived. */
  carriedBy?: number;
}

export interface SimResult {
  /** Everything the workflow observed, in order. A pure function of the journal. */
  digest: unknown[];
  /** Who returned it — NOT journal-derived, so a terminal CAS loser visibly differs from the winner. */
  by: number;
}

export const workflowName = (index: number): string => `p${index}`;
export const programIndex = (wf: string): number => Number(wf.slice(1));

/** The signals a run of `program` is promised (names are unique per op, so FIFO order is moot). */
export function promisedSignals(program: Program): string[] {
  const out: string[] = [];
  program.ops.forEach((op, i) => {
    if (op.op === "await" && op.signalled) out.push(`sig${i}`);
    if (op.op === "fanout") {
      op.branches.forEach((b, k) => {
        if (b.kind === "await" && b.signalled) out.push(`sig${i}_${k}`);
      });
    }
  });
  return out;
}

export class SimStepError extends Error {
  override name = "SimStepError";
}

export function buildWorkflows(programs: readonly Program[]): Map<string, AnyWorkflow> {
  const out = new Map<string, AnyWorkflow>();
  // Children first: a `call` op needs the child's workflow token.
  for (let i = programs.length - 1; i >= 0; i--) {
    const program = programs[i];
    if (program === undefined) continue;
    const wf = defineWorkflow(workflowName(i), (ctx: Ctx, input: SimInput) =>
      interpret(ctx, input, program, out),
    );
    out.set(workflowName(i), wf);
  }
  return out;
}

async function interpret(
  ctx: Ctx,
  input: SimInput,
  program: Program,
  workflows: Map<string, AnyWorkflow>,
): Promise<SimResult> {
  const sim = ctx.get<SimHooks>("sim");
  const digest: unknown[] = [];
  const child = (index: number): AnyWorkflow => {
    const wf = workflows.get(workflowName(index));
    if (wf === undefined) throw new Error(`sim: no workflow for program ${index}`);
    return wf;
  };
  const childInput: SimInput = { gen: 0 };
  // A failure is digested by NAME only: the message embeds run ids and incarnation-made text.
  const guarded = async (caught: boolean, body: () => Promise<void>): Promise<void> => {
    if (!caught) return body();
    try {
      await body();
    } catch (e) {
      digest.push(`caught:${e instanceof Error ? e.name : "unknown"}`);
    }
  };
  // A child's result is `{digest, by}`; only the digest is journal-derived on the child's side too,
  // but the parent reads the JOURNALED value, so all of it is fair game here.
  for (const [i, op] of program.ops.entries()) {
    switch (op.op) {
      case "run":
        await guarded(op.caught, async () => {
          digest.push(await ctx.run(`s${i}`, () => sim.effect(ctx.key(), `s${i}`, op.throws)));
        });
        break;
      case "value":
        digest.push(
          op.kind === "now"
            ? await ctx.now()
            : op.kind === "random"
              ? await ctx.random()
              : await ctx.uuid(),
        );
        break;
      case "sleep":
        await ctx.sleep(`z${i}`, op.ms);
        digest.push("slept");
        break;
      case "await":
        await guarded(op.caught, async () => {
          digest.push(
            await ctx.await(
              `sig${i}`,
              op.timeoutMs !== undefined ? { timeoutMs: op.timeoutMs } : undefined,
            ),
          );
        });
        break;
      case "call":
        await guarded(op.caught, async () => {
          digest.push(await ctx.call(child(op.child), childInput as never));
        });
        break;
      case "fanout":
        await guarded(op.caught, async () => {
          const futures: DurableFuture<unknown>[] = op.branches.map((b, k) =>
            b.kind === "call"
              ? ctx.startCall(child(b.child), childInput as never)
              : b.kind === "sleep"
                ? ctx.startSleep(`z${i}_${k}`, b.ms)
                : ctx.startAwait(
                    `sig${i}_${k}`,
                    b.timeoutMs !== undefined ? { timeoutMs: b.timeoutMs } : undefined,
                  ),
          );
          if (op.join === "all") digest.push(await Promise.all(futures));
          else if (op.join === "race") digest.push(await ctx.race(futures));
          else {
            const settled = await Promise.allSettled(futures);
            digest.push(
              settled.map((s) =>
                s.status === "fulfilled"
                  ? s.value
                  : `rejected:${s.reason instanceof Error ? s.reason.name : "unknown"}`,
              ),
            );
          }
        });
        break;
      case "onCancel":
        ctx.onCancel(() => ctx.run(`c${i}`, () => sim.effect(ctx.key(), `c${i}`, false)));
        break;
    }
  }
  if (input.gen < program.generations) {
    return ctx.continueAsNew({ gen: input.gen + 1, carriedBy: sim.incId } satisfies SimInput);
  }
  return { digest, by: sim.incId };
}

// ---------------------------------------------------------------- generation

function generateBranch(rng: Rng, children: number[]): Branch {
  const kinds: Branch["kind"][] = children.length > 0 ? ["call", "sleep", "await"] : ["sleep", "await"]; // prettier-ignore
  const kind = rng.pick(kinds);
  if (kind === "call") return { kind, child: rng.pick(children) };
  if (kind === "sleep") return { kind, ms: 1 + rng.int(500) };
  return generateAwait(rng);
}

function generateAwait(rng: Rng): { kind: "await"; timeoutMs?: number; signalled: boolean } {
  // An await with no deadline is always signalled — every generated run must be able to finish, or
  // "a run is stranded" could not be told from "the program waits for something nobody sends".
  if (!rng.chance(0.4)) return { kind: "await", signalled: true };
  return { kind: "await", timeoutMs: 1 + rng.int(500), signalled: rng.chance(0.6) };
}

function generateOp(rng: Rng, children: number[]): Op {
  const kinds = ["run", "value", "sleep", "await", "call", "fanout", "onCancel"] as const;
  const kind = kinds[rng.weighted([4, 1, 1.5, 1.5, children.length > 0 ? 2 : 0, 1.5, 0.5])];
  const caught = rng.chance(0.5);
  switch (kind) {
    case "value":
      return { op: "value", kind: rng.pick(["now", "random", "uuid"] as const) };
    case "sleep":
      return { op: "sleep", ms: 1 + rng.int(500) };
    case "await": {
      const a = generateAwait(rng);
      return a.timeoutMs !== undefined
        ? { op: "await", timeoutMs: a.timeoutMs, signalled: a.signalled, caught }
        : { op: "await", signalled: a.signalled, caught };
    }
    case "call":
      return { op: "call", child: rng.pick(children), caught };
    case "fanout": {
      const branches = Array.from({ length: 2 + rng.int(2) }, () => generateBranch(rng, children));
      return { op: "fanout", branches, join: rng.pick(["all", "allSettled", "race"] as const), caught }; // prettier-ignore
    }
    case "onCancel":
      return { op: "onCancel" };
    default:
      return { op: "run", throws: rng.chance(0.1), caught };
  }
}

/** Program 0 is the root; program k may call only programs > k, so the call graph is a DAG. */
export function generatePrograms(rng: Rng): Program[] {
  const count = 1 + rng.int(3);
  const programs: Program[] = [];
  for (let i = 0; i < count; i++) {
    const children = Array.from({ length: count - i - 1 }, (_, k) => i + 1 + k);
    const length = i === 0 ? 2 + rng.int(5) : 1 + rng.int(3);
    const ops = Array.from({ length }, () => generateOp(rng, children));
    const generations = rng.chance(i === 0 ? 0.25 : 0.1) ? 1 + rng.int(i === 0 ? 2 : 1) : 0;
    programs.push({ ops, generations });
  }
  return programs;
}

/** A compact one-line rendering, for a failing case's report. */
export function describeProgram(p: Program, index: number): string {
  const branch = (b: Branch): string =>
    b.kind === "call"
      ? `call(p${b.child})`
      : b.kind === "sleep"
        ? `sleep(${b.ms})`
        : `await(${b.timeoutMs ?? "∞"}${b.signalled ? ",sig" : ""})`;
  const ops = p.ops.map((op, i) => {
    const c = "caught" in op && op.caught ? "?" : "";
    switch (op.op) {
      case "run":
        return `${i}:run${op.throws ? "!" : ""}${c}`;
      case "value":
        return `${i}:${op.kind}`;
      case "sleep":
        return `${i}:sleep(${op.ms})`;
      case "await":
        return `${i}:await(${op.timeoutMs ?? "∞"}${op.signalled ? ",sig" : ""})${c}`;
      case "call":
        return `${i}:call(p${op.child})${c}`;
      case "fanout":
        return `${i}:${op.join}[${op.branches.map(branch).join(",")}]${c}`;
      case "onCancel":
        return `${i}:onCancel`;
    }
  });
  return `p${index}{${ops.join(" ")}}${p.generations > 0 ? `×CAN${p.generations}` : ""}`;
}
