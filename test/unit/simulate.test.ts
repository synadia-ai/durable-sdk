// Deterministic simulation of the pure core (test/harness/sim): several workers, one in-memory
// journal, a seeded scheduler choosing every interleaving and every fault. It looks for what no
// scenario test enumerates — the class of LEARNINGS #51 and #53, found only once two actors touched
// one run — and what it has found is pinned, interleaving forced, in `sim-findings.test.ts`.
//
//   DE_SIM_SEEDS=5000        the first 5000 seeds (the gate runs 400)
//   DE_SIM_SEEDS=1200-1300   a range — e.g. around a seed that failed
//
// A failure prints the SHRUNK case: the smallest programs, fault budget and worker count that
// still break the same invariant, with its trace. The case (not the seed) is the reproduction —
// a seed only means something to the generator that drew it.

import { beforeAll, describe, expect, it } from "vitest";
import { recordInbound } from "../../src/core/inbound.js";
import { FencedError } from "../../src/errors.js";
import { type Journal } from "../../src/journal/store.js";
import {
  type Failure,
  type Op,
  type SimOptions,
  type SimStats,
  calm,
  caseFromSeed,
  describeFailure,
  failureOf,
  runCase,
  shrink,
} from "../harness/sim/index.js";

function seedRange(): [number, number] {
  const spec = process.env.DE_SIM_SEEDS;
  if (spec === undefined || spec === "") return [1, 400];
  const m = /^(\d+)(?:-(\d+))?$/.exec(spec);
  if (!m) throw new Error(`DE_SIM_SEEDS: expected a count or a from-to range, got "${spec}"`);
  return m[2] !== undefined ? [Number(m[1]), Number(m[2])] : [1, Number(m[1])];
}

const [FROM, TO] = seedRange();
const seeds = Array.from({ length: TO - FROM + 1 }, (_, i) => FROM + i);

/** The first failure in a sweep (of the class `matching`, if given), shrunk — or undefined. */
async function firstFailure(
  over: number[],
  opts?: SimOptions,
  matching?: RegExp,
): Promise<Failure | undefined> {
  for (const seed of over) {
    const f = await failureOf(caseFromSeed(seed), opts);
    if (f && (matching === undefined || matching.test(f.report.violations[0] ?? ""))) {
      return shrink(f, opts);
    }
  }
  return undefined;
}

describe("simulation: the engine under seeded interleavings and faults", () => {
  const total: Partial<SimStats> & Record<string, number> = {};
  const known = new Map<string, number>();
  let failure: Failure | undefined;

  beforeAll(async () => {
    for (const seed of seeds) {
      const c = caseFromSeed(seed);
      const f = await failureOf(c);
      if (f) {
        failure = await shrink(f);
        return;
      }
      const report = await runCase(c);
      for (const [k, v] of Object.entries(report.stats)) total[k] = (total[k] ?? 0) + v;
      for (const hit of report.known) {
        const cls = hit.slice(0, hit.indexOf(":"));
        known.set(cls, (known.get(cls) ?? 0) + 1);
      }
    }
  }, 600_000);

  it(`no invariant is violated (seeds ${FROM}–${TO})`, () => {
    if (failure) throw new Error(`\n${describeFailure(failure)}\n`);
  });

  it("the sweep actually produced what it claims to test", () => {
    // A simulation in which nothing ever collides proves nothing (LEARNINGS #55): the conditions
    // the invariants guard against must have OCCURRED, in numbers, or the sweep is a no-op.
    if (failure) return;
    const n = seeds.length;
    const atLeast = (what: keyof SimStats, perSeed: number): void =>
      expect(total[what] ?? 0, what).toBeGreaterThan(n * perSeed);
    atLeast("fences", 0.5); // two writers on one journal, the loser fenced
    atLeast("eventRetries", 0.1); // an inbound event that lost its CAS and had to be retried (#51)
    atLeast("terminalRaces", 0.05); // an execution that ADOPTED another's terminal (#53)
    atLeast("repeatedEffects", 0.1); // the at-least-once window, observed
    atLeast("crashes", 0.2);
    atLeast("redeliveries", 0.2);
    atLeast("lostAcks", 0.05);
    atLeast("infraErrors", 0.2);
    atLeast("cancelsAhead", 0.01); // a cancel journaled while its own worker still executed the run
    expect(total.runs ?? 0).toBeGreaterThan(n * 1.2); // children and continue-as-new generations
  });

  it("the tolerated OPEN findings are still open — and nothing else is tolerated", () => {
    // Each tolerance in world.ts hides one known engine defect (pinned in sim-findings.test.ts).
    // When a defect is fixed its hits vanish and THIS goes red: delete the tolerance with the fix.
    if (failure) return;
    expect([...known.keys()].sort()).toEqual(["premature-suspend", "step-failure-not-journaled"]);
  });

  it("a case is a reproduction: the same case twice is the same trace", async () => {
    for (const seed of seeds.slice(0, 25)) {
      const a = await runCase(caseFromSeed(seed));
      const b = await runCase(caseFromSeed(seed));
      expect(b.trace).toEqual(a.trace);
    }
  });

  it("in a calm world — one worker, no faults — every effect fires exactly once", async () => {
    let effects = 0;
    for (const seed of seeds) {
      const c = calm(caseFromSeed(seed));
      // Exactly-once is claimed for programs WITHOUT the two constructs behind the open findings,
      // until they are fixed: a step that throws is re-run by every replay; and a native
      // Promise.all over a rejected and a pending branch lets the run be acked as suspended while
      // its catch block is mid-step — the next wake finds a step-begin with no end and fires the
      // effect AGAIN. One worker, no fault: 30,000 seeds found it, the gate's 400 had not.
      const open = (op: Op): boolean =>
        (op.op === "run" && op.throws) || (op.op === "fanout" && op.join === "all");
      if (c.programs.some((p) => p.ops.some(open))) continue;
      const report = await runCase(c);
      expect(report.violations, `seed ${seed}`).toEqual([]);
      expect(report.stats.repeatedEffects, `seed ${seed}`).toBe(0);
      effects += report.stats.effects;
    }
    expect(effects).toBeGreaterThan(seeds.length / 4);
  });
});

describe("simulation: a cancel is journaled ahead of its run's chain (F23)", () => {
  it("the worker driving a run journals its cancel mid-execution — and the old model never does", async () => {
    // The control is the model of the worker BEFORE the fix: the cancel waits behind the execution
    // it targets, so a cancel never lands while its own worker is executing the run. Same seeds.
    const count = async (opts: SimOptions): Promise<number> => {
      let n = 0;
      for (const seed of seeds.slice(0, 200)) {
        const report = await runCase(caseFromSeed(seed), opts);
        expect(report.violations, `seed ${seed}`).toEqual([]);
        n += report.stats.cancelsAhead;
      }
      return n;
    };
    expect(await count({})).toBeGreaterThan(0);
    expect(await count({ cancelAhead: false })).toBe(0);
  });
});

describe("simulation controls: a planted engine fault is FOUND", () => {
  // The standing rule: a mechanism without a control that fails without it is not tested. Each of
  // these breaks the engine on purpose and demands that the same sweep that passes above fails.

  it("LEARNINGS #51 — a fenced inbound-event append treated as handled → a stranded run", async () => {
    // Exactly the historical bug: the append did not land, and the item is acked anyway.
    const swallowFences = (j: Journal): Journal => ({
      load: (runId) => j.load(runId),
      append: (runId, a) =>
        j.append(runId, a).catch((e: unknown) => {
          if (e instanceof FencedError) return { streamSeq: 0, duplicate: true };
          throw e;
        }),
    });
    const planted: SimOptions = {
      recordInbound: (journal, work, loaded, clock) =>
        recordInbound(swallowFences(journal), work, loaded, clock),
    };
    const found = await firstFailure(seeds, planted);
    expect(found, "the planted fault was not found").toBeDefined();
    expect(found?.report.violations.join("\n")).toMatch(/STRANDED/);
    // …and shrunk to something a person can read: two actors, a couple of ops.
    expect(found?.case.programs.flatMap((p) => p.ops).length).toBeLessThanOrEqual(4);
  }, 120_000);

  it("a journal that serves STALE reads → a stranded run (the completeness guard is load-bearing)", async () => {
    // Breaks the Journal contract, not the engine: the core acks a `suspended` it computed from a
    // prefix that lacks the very event it was woken for. The NATS shell's load is verified against
    // the leader's count for this reason; on R3 a follower can serve exactly such a prefix.
    // (Stale reads also duplicate effects — an incarnation re-runs a step whose step-end an
    // EARLIER load had shown it — and the simulation reports that too; the claim here is liveness.)
    const found = await firstFailure(seeds, { staleReads: true }, /STRANDED/);
    expect(found, "stale reads stranded nothing").toBeDefined();
  }, 120_000);
});
