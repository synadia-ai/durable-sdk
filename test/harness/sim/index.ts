// The simulation's front door: a case from a seed, a sweep over seeds, and a shrinker that turns a
// failing case into the smallest one that still fails the same way. See `world.ts` for what is
// simulated and `program.ts` for what runs in it.

import { type Op, type Program, describeProgram, generatePrograms } from "./program.js";
import { Rng } from "./rng.js";
import { type SimCase, type SimOptions, type SimReport, runCase } from "./world.js";

export { type Program, type Op } from "./program.js";
export { type Faults, type SimCase, type SimOptions, type SimReport, type SimStats, runCase } from "./world.js"; // prettier-ignore

/** Everything about a case derives from its seed: the programs from one stream, the rest from another. */
export function caseFromSeed(seed: number): SimCase {
  const rng = new Rng(seed);
  const programs = generatePrograms(rng.fork(1));
  const s = rng.fork(2);
  return {
    programs,
    schedSeed: s.int(0x7fffffff),
    workers: 1 + s.int(3),
    faults: {
      crashes: s.int(3),
      redeliveries: s.int(3),
      infraErrors: s.int(3),
      lostAcks: s.int(2),
    },
    journalDedup: s.chance(0.75),
    queueDedup: s.chance(0.75),
    cancelRoot: s.chance(0.3),
  };
}

/** The same world with nothing going wrong in it: one worker, no faults, no duplicates. */
export function calm(c: SimCase): SimCase {
  return {
    ...c,
    workers: 1,
    faults: { crashes: 0, redeliveries: 0, infraErrors: 0, lostAcks: 0 },
    journalDedup: true,
    queueDedup: true,
  };
}

/** A violation with everything case-specific removed, so that two cases can fail "the same way". */
export function violationClass(v: string): string {
  return v
    .replace(/\{.*$/s, "")
    .replace(/root\w*/g, "R")
    .replace(/\d+/g, "N")
    .slice(0, 60);
}

export interface Failure {
  case: SimCase;
  report: SimReport;
}

/** Run a case; a failure is returned, never thrown (an exception in the simulator itself is one too). */
export async function failureOf(c: SimCase, opts?: SimOptions): Promise<Failure | undefined> {
  let report: SimReport;
  try {
    report = await runCase(c, opts);
  } catch (e) {
    const message = `the simulator threw: ${e instanceof Error ? (e.stack ?? e.message) : String(e)}`;
    report = { violations: [message], known: [], trace: [], stats: emptyStats() };
  }
  return report.violations.length > 0 ? { case: c, report } : undefined;
}

const emptyStats = (): SimReport["stats"] => ({
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
});

// ---------------------------------------------------------------- shrinking

function* smallerPrograms(programs: Program[]): Generator<Program[]> {
  const referenced = (k: number): boolean =>
    programs.some((p) =>
      p.ops.some(
        (op) =>
          (op.op === "call" && op.child === k) ||
          (op.op === "fanout" && op.branches.some((b) => b.kind === "call" && b.child === k)),
      ),
    );
  const last = programs.length - 1;
  if (last > 0 && !referenced(last)) yield programs.slice(0, last);
  for (const [k, p] of programs.entries()) {
    const put = (next: Program): Program[] => programs.map((q, i) => (i === k ? next : q));
    const putOp = (i: number, op: Op): Program[] =>
      put({ ...p, ops: p.ops.map((o, n) => (n === i ? op : o)) });
    if (p.generations > 0) yield put({ ...p, generations: p.generations - 1 });
    for (const [i, op] of p.ops.entries()) {
      if (p.ops.length > 1 || k > 0) yield put({ ...p, ops: p.ops.filter((_, n) => n !== i) });
      if ("caught" in op && op.caught) yield putOp(i, { ...op, caught: false });
      if (op.op === "run" && op.throws) yield putOp(i, { ...op, throws: false });
      if (op.op === "fanout") {
        if (op.branches.length > 1) {
          for (const b of op.branches.keys()) {
            yield putOp(i, { ...op, branches: op.branches.filter((_, n) => n !== b) });
          }
        }
        if (op.join !== "all") yield putOp(i, { ...op, join: "all" });
      }
    }
  }
}

function* smallerCases(c: SimCase): Generator<SimCase> {
  for (const key of ["crashes", "redeliveries", "infraErrors", "lostAcks"] as const) {
    if (c.faults[key] > 0) yield { ...c, faults: { ...c.faults, [key]: 0 } };
    if (c.faults[key] > 1) yield { ...c, faults: { ...c.faults, [key]: c.faults[key] - 1 } };
  }
  if (c.workers > 1) yield { ...c, workers: c.workers - 1 };
  if (c.cancelRoot) yield { ...c, cancelRoot: false };
  if (!c.journalDedup) yield { ...c, journalDedup: true };
  if (!c.queueDedup) yield { ...c, queueDedup: true };
  for (const programs of smallerPrograms(c.programs)) yield { ...c, programs };
}

/**
 * Greedy shrink: take the first smaller case that still fails THE SAME WAY, repeat until none does.
 * Removing anything perturbs the schedule, so each candidate is tried under its own schedule seed
 * and then under a handful of neighbours — a smaller case that fails under a different interleaving
 * is still a smaller reproduction.
 */
export async function shrink(
  failure: Failure,
  opts?: SimOptions,
  budget = 1_500,
  neighbours = 24,
): Promise<Failure> {
  const wanted = violationClass(failure.report.violations[0] ?? "");
  let best = failure;
  let runs = 0;
  search: for (;;) {
    for (const candidate of smallerCases(best.case)) {
      for (let n = 0; n <= neighbours; n++) {
        if (++runs > budget) break search;
        const f = await failureOf({ ...candidate, schedSeed: candidate.schedSeed + n }, opts);
        if (f && violationClass(f.report.violations[0] ?? "") === wanted) {
          best = f;
          continue search;
        }
      }
    }
    break;
  }
  return best;
}

export function describeFailure(f: Failure, traceLines = 60): string {
  const c = f.case;
  return [
    ...f.report.violations.map((v) => `VIOLATION: ${v}`),
    `programs: ${c.programs.map(describeProgram).join("  ")}`,
    `workers ${c.workers} · faults ${JSON.stringify(c.faults)} · journalDedup ${c.journalDedup} · ` +
      `queueDedup ${c.queueDedup} · cancelRoot ${c.cancelRoot} · schedSeed ${c.schedSeed}`,
    `case: ${JSON.stringify(c)}`,
    `trace (last ${traceLines} of ${f.report.trace.length}):`,
    ...f.report.trace.slice(-traceLines).map((t) => `  ${t}`),
  ].join("\n");
}
