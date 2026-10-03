// The crown-jewel unit suite: proves the two-phase replay engine entirely in-memory (no NATS).

import { describe, expect, it } from "vitest";
import { type ChildStart, type Children } from "../../src/core/children.js";
import { executeRun } from "../../src/core/execute.js";
import { defineWorkflow } from "../../src/core/workflow.js";
import { FencedError, SerializationError } from "../../src/errors.js";
import { type JournalEntry, msgId, NON_COMMAND_SEQ, stamp } from "../../src/journal/entry.js";
import {
  type AppendResult,
  InMemoryJournal,
  type Journal,
  type JournalAppend,
} from "../../src/journal/store.js";
import { type Ctx } from "../../src/types.js";
import { beforeTerminal } from "../harness/replayable.js";

async function append(j: Journal, runId: string, entry: JournalEntry, mid: string): Promise<void> {
  const loaded = await j.load(runId);
  const last = loaded.at(-1)?.streamSeq ?? 0;
  await j.append(runId, { entry, msgID: mid, expectLastSeq: last });
}

const seedStart = (j: Journal, runId: string): Promise<void> =>
  append(
    j,
    runId,
    stamp({ kind: "start", seq: 0, wf: "w", wfVersion: 1, input: {} }, 0),
    msgId.start(runId),
  );

function countingWf(counter: Record<string, number>, names = ["a", "b", "c"]) {
  return defineWorkflow("w", async (ctx) => {
    const out: string[] = [];
    for (const nm of names) {
      out.push(
        await ctx.run(nm, () => {
          counter[nm] = (counter[nm] ?? 0) + 1;
          return nm.toUpperCase();
        }),
      );
    }
    return out.join("");
  });
}

describe("replay engine (two-phase, pure)", () => {
  it("happy path completes and runs each step exactly once", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "r1");
    const counter: Record<string, number> = {};
    const out = await executeRun("r1", {
      workflow: countingWf(counter),
      journal: j,
      loaded: await j.load("r1"),
    });
    expect(out).toEqual({ kind: "completed", value: "ABC" });
    expect(counter).toEqual({ a: 1, b: 1, c: 1 });
  });

  it("replays completed steps WITHOUT re-executing them", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "r2");
    const counter: Record<string, number> = {};
    await executeRun("r2", {
      workflow: countingWf(counter),
      journal: j,
      loaded: await j.load("r2"),
    });
    expect(counter).toEqual({ a: 1, b: 1, c: 1 });
    // a second incarnation over the SAME journal replays everything from the log
    const out2 = await executeRun("r2", {
      workflow: countingWf(counter),
      journal: j,
      loaded: beforeTerminal(await j.load("r2")),
    });
    expect(out2).toEqual({ kind: "completed", value: "ABC" });
    expect(counter).toEqual({ a: 1, b: 1, c: 1 }); // UNCHANGED
  });

  it("resumes a step that began but never ended (re-runs just that step)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "r3");
    await append(
      j,
      "r3",
      stamp({ kind: "step-begin", seq: 0, name: "a", key: "r3:0" }, 0),
      msgId.stepBegin("r3", 0),
    );
    const counter: Record<string, number> = {};
    const out = await executeRun("r3", {
      workflow: countingWf(counter),
      journal: j,
      loaded: await j.load("r3"),
    });
    expect(out).toEqual({ kind: "completed", value: "ABC" });
    expect(counter).toEqual({ a: 1, b: 1, c: 1 });
  });

  it("does not re-run a fully completed step", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "r4");
    await append(
      j,
      "r4",
      stamp({ kind: "step-begin", seq: 0, name: "a", key: "r4:0" }, 0),
      msgId.stepBegin("r4", 0),
    );
    await append(
      j,
      "r4",
      stamp({ kind: "step-end", seq: 0, name: "a", value: "A" }, 0),
      msgId.stepEnd("r4", 0),
    );
    const counter: Record<string, number> = {};
    const out = await executeRun("r4", {
      workflow: countingWf(counter),
      journal: j,
      loaded: await j.load("r4"),
    });
    expect(out).toEqual({ kind: "completed", value: "ABC" });
    expect(counter.a ?? 0).toBe(0); // replayed from journal — fn NOT run
    expect(counter).toEqual({ b: 1, c: 1 });
  });

  it("fails loud on nondeterminism (different step name at a position)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "r5");
    await append(
      j,
      "r5",
      stamp({ kind: "step-begin", seq: 0, name: "X", key: "r5:0" }, 0),
      msgId.stepBegin("r5", 0),
    );
    await append(
      j,
      "r5",
      stamp({ kind: "step-end", seq: 0, name: "X", value: "x" }, 0),
      msgId.stepEnd("r5", 0),
    );
    const out = await executeRun("r5", {
      workflow: countingWf({}, ["a"]),
      journal: j,
      loaded: await j.load("r5"),
    });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.name).toBe("NonDeterminismError");
  });

  it("ROBUSTNESS: a zombie is fenced at step-begin, BEFORE the side effect fires", async () => {
    const inner = new InMemoryJournal();
    await seedStart(inner, "r6");
    const j: Journal = {
      load: (r) => inner.load(r),
      append: (r, a: JournalAppend) =>
        a.msgID === msgId.stepBegin("r6", 0)
          ? Promise.reject(new FencedError(r, "injected: another worker won step-begin"))
          : inner.append(r, a),
    };
    const counter: Record<string, number> = {};
    const out = await executeRun("r6", {
      workflow: countingWf(counter),
      journal: j,
      loaded: await j.load("r6"),
    });
    expect(out).toEqual({ kind: "fenced" });
    expect(counter.a ?? 0).toBe(0); // the effect NEVER fired — the whole point of intent-first
  });

  it("records and replays now()/uuid() identically across incarnations", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "r7");
    const wf = defineWorkflow("w", async (ctx) => ({ t: await ctx.now(), id: await ctx.uuid() }));
    const first = await executeRun("r7", {
      workflow: wf,
      journal: j,
      loaded: await j.load("r7"),
      clock: () => 12345,
    });
    expect(first.kind).toBe("completed");
    const second = await executeRun("r7", {
      workflow: wf,
      journal: j,
      loaded: beforeTerminal(await j.load("r7")),
      clock: () => 99999,
    });
    expect(second).toEqual(first); // replayed from journal — the new clock is ignored
    if (first.kind === "completed") expect((first.value as { t: number }).t).toBe(12345);
  });

  it("records random() once and replays the SAME value (Math.random not called again)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "rnd");
    const wf = defineWorkflow("w", (ctx) => ctx.random());
    const first = await executeRun("rnd", {
      workflow: wf,
      journal: j,
      loaded: await j.load("rnd"),
    });
    expect(first.kind).toBe("completed");
    const second = await executeRun("rnd", {
      workflow: wf,
      journal: j,
      loaded: beforeTerminal(await j.load("rnd")),
    });
    expect(second).toEqual(first); // replayed from the journal — a fresh Math.random() is NOT used
    expect((await j.load("rnd")).filter((l) => l.entry.kind === "random")).toHaveLength(1);
  });
});

describe("replay engine — edge cases (rock-solid review)", () => {
  it("fails loud when a recorded command KIND differs at a position", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "e1");
    // seq 0 was recorded as a now(); replay calls ctx.run() there instead
    await append(j, "e1", stamp({ kind: "now", seq: 0, value: 111 }, 0), msgId.value("e1", 0));
    const out = await executeRun("e1", {
      workflow: countingWf({}, ["a"]),
      journal: j,
      loaded: await j.load("e1"),
    });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.name).toBe("NonDeterminismError");
  });

  it("fails loud when replay takes a SHORTER path than recorded", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "e2");
    // Three recorded steps and NO terminal (a journal that holds one is never executed at all).
    for (const [seq, nm] of ["a", "b", "c"].entries()) {
      await append(j, "e2", stamp({ kind: "step-begin", seq, name: nm, key: `e2:${seq}` }, 0), msgId.stepBegin("e2", seq)); // prettier-ignore
      await append(j, "e2", stamp({ kind: "step-end", seq, name: nm, value: nm.toUpperCase() }, 0), msgId.stepEnd("e2", seq)); // prettier-ignore
    }
    const out = await executeRun("e2", {
      workflow: countingWf({}, ["a"]), // only 1 step this time
      journal: j,
      loaded: await j.load("e2"),
    });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.name).toBe("NonDeterminismError");
  });

  it("reconciles a fenced step-end by ADOPTING the canonical value", async () => {
    const inner = new InMemoryJournal();
    await seedStart(inner, "e3");
    await append(
      inner,
      "e3",
      stamp({ kind: "step-begin", seq: 0, name: "a", key: "e3:0" }, 0),
      msgId.stepBegin("e3", 0),
    );
    const counter: Record<string, number> = {};
    const j: Journal = {
      load: (r) => inner.load(r),
      append: async (r, a: JournalAppend) => {
        if (a.msgID === msgId.stepEnd("e3", 0)) {
          // a competitor lands the canonical end first, then our append loses the race
          const last = (await inner.load(r)).at(-1)!.streamSeq;
          await inner.append(r, {
            entry: stamp({ kind: "step-end", seq: 0, name: "a", value: "COMPETITOR" }, 0),
            msgID: "competitor:e3:0:e",
            expectLastSeq: last,
          });
          return Promise.reject(new FencedError(r, "injected: competitor finished the step"));
        }
        return inner.append(r, a);
      },
    };
    const wf = defineWorkflow("w", (ctx) =>
      ctx.run("a", () => {
        counter.a = (counter.a ?? 0) + 1;
        return "MINE";
      }),
    );
    const out = await executeRun("e3", { workflow: wf, journal: j, loaded: await j.load("e3") });
    expect(out).toEqual({ kind: "completed", value: "COMPETITOR" }); // adopted, not "MINE"
    expect(counter.a).toBe(1); // our fn ran (at-least-once), but the journal's value wins
  });

  it("halts (fenced) when a step-end loses and no canonical end exists", async () => {
    const inner = new InMemoryJournal();
    await seedStart(inner, "e4");
    await append(
      inner,
      "e4",
      stamp({ kind: "step-begin", seq: 0, name: "a", key: "e4:0" }, 0),
      msgId.stepBegin("e4", 0),
    );
    const counter: Record<string, number> = {};
    const j: Journal = {
      load: (r) => inner.load(r),
      append: (r, a: JournalAppend) =>
        a.msgID === msgId.stepEnd("e4", 0)
          ? Promise.reject(new FencedError(r, "injected: lost step-end race"))
          : inner.append(r, a),
    };
    const wf = defineWorkflow("w", (ctx) =>
      ctx.run("a", () => {
        counter.a = (counter.a ?? 0) + 1;
        return "MINE";
      }),
    );
    const out = await executeRun("e4", { workflow: wf, journal: j, loaded: await j.load("e4") });
    expect(out).toEqual({ kind: "fenced" });
    expect(counter.a).toBe(1);
  });

  it("a thrown error fails the run terminally", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "e5");
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.run("a", () => 1);
      throw new Error("boom");
    });
    const out = await executeRun("e5", { workflow: wf, journal: j, loaded: await j.load("e5") });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") {
      expect(out.error.name).toBe("Error");
      expect(out.error.message).toBe("boom");
    }
  });

  it("ctx.assert(false) fails the run loudly", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "e6");
    const wf = defineWorkflow("w", (ctx): Promise<string> => {
      const two: number = 2;
      ctx.assert(two === 3, "math broke");
      return Promise.resolve("unreachable");
    });
    const out = await executeRun("e6", { workflow: wf, journal: j, loaded: await j.load("e6") });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") {
      expect(out.error.name).toBe("WorkflowFailure");
      expect(out.error.message).toBe("math broke");
    }
  });

  it("retries a failing step per its policy, then succeeds", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "e7");
    let attempts = 0;
    const wf = defineWorkflow("w", (ctx) =>
      ctx.run(
        "flaky",
        () => {
          attempts += 1;
          if (attempts < 3) throw new Error("transient");
          return "ok";
        },
        { retry: { kind: "fixed", attempts: 3, delayMs: 0 } },
      ),
    );
    const out = await executeRun("e7", { workflow: wf, journal: j, loaded: await j.load("e7") });
    expect(out).toEqual({ kind: "completed", value: "ok" });
    expect(attempts).toBe(3);
  });

  it("fails the run when a step exhausts its retries", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "e8");
    let attempts = 0;
    const wf = defineWorkflow("w", (ctx) =>
      ctx.run(
        "always-fails",
        () => {
          attempts += 1;
          throw new Error("nope");
        },
        { retry: { kind: "fixed", attempts: 2, delayMs: 0 } },
      ),
    );
    const out = await executeRun("e8", { workflow: wf, journal: j, loaded: await j.load("e8") });
    expect(out.kind).toBe("failed");
    expect(attempts).toBe(2);
  });

  it("an empty workflow completes immediately", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "e9");
    const wf = defineWorkflow("w", () => Promise.resolve(42));
    const out = await executeRun("e9", { workflow: wf, journal: j, loaded: await j.load("e9") });
    expect(out).toEqual({ kind: "completed", value: 42 });
  });

  it("retries a failing step under an EXPONENTIAL policy, then succeeds (maxMs clamps the backoff)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "e10");
    let attempts = 0;
    const wf = defineWorkflow("w", (ctx) =>
      ctx.run(
        "flaky",
        () => {
          attempts += 1;
          if (attempts < 3) throw new Error("transient");
          return "ok";
        },
        // baseMs*factor^n would be 1000ms/2000ms, but maxMs:1 clamps each backoff to ~1ms (fast test).
        { retry: { kind: "exponential", attempts: 3, baseMs: 1000, factor: 2, maxMs: 1 } },
      ),
    );
    const out = await executeRun("e10", { workflow: wf, journal: j, loaded: await j.load("e10") });
    expect(out).toEqual({ kind: "completed", value: "ok" });
    expect(attempts).toBe(3);
  });

  it("normalizes a non-Error throw via toErrorShape (name 'Error', String(value) message)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "e11");
    const wf = defineWorkflow("w", (): Promise<never> => {
      const thrown: unknown = "boom"; // a non-Error rejection (e.g. a string thrown by a dependency)
      throw thrown;
    });
    const out = await executeRun("e11", { workflow: wf, journal: j, loaded: await j.load("e11") });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") {
      expect(out.error.name).toBe("Error");
      expect(out.error.message).toBe("boom");
    }
  });

  it("supports the SAME step name reused across seqs (loop pattern), replayed cleanly", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "samename");
    let calls = 0;
    const wf = defineWorkflow("w", async (ctx) => {
      const results: string[] = [];
      for (let i = 0; i < 3; i += 1) {
        results.push(
          await ctx.run("iter", () => {
            calls += 1;
            return `r${i}`;
          }),
        );
      }
      return results.join(",");
    });
    const out = await executeRun("samename", {
      workflow: wf,
      journal: j,
      loaded: await j.load("samename"),
    });
    expect(out).toEqual({ kind: "completed", value: "r0,r1,r2" });
    expect(calls).toBe(3);
    // three step-begins, all named "iter", at distinct seqs 0,1,2 (name is matched per-seq, not globally)
    const begins = (await j.load("samename")).filter((l) => l.entry.kind === "step-begin");
    expect(begins.map((l) => l.entry.seq)).toEqual([0, 1, 2]);
    // a fresh incarnation replays all three from the journal without re-running any
    const out2 = await executeRun("samename", {
      workflow: wf,
      journal: j,
      loaded: beforeTerminal(await j.load("samename")),
    });
    expect(out2).toEqual({ kind: "completed", value: "r0,r1,r2" });
    expect(calls).toBe(3); // UNCHANGED on replay
  });
});

describe("serialization failures (pure)", () => {
  // The in-memory double now round-trips through the codec, faithful to the NATS shell — so the pure
  // suite is no longer blind to serialization (the gap that hid A2). A bare InMemoryJournal suffices.

  it("the in-memory journal rejects an unencodable entry with SerializationError", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "ser0");
    const last = (await j.load("ser0")).at(-1)!.streamSeq;
    await expect(
      j.append("ser0", {
        entry: stamp({ kind: "step-end", seq: 0, name: "a", value: BigInt(10) }, 0),
        msgID: msgId.stepEnd("ser0", 0),
        expectLastSeq: last,
      }),
    ).rejects.toBeInstanceOf(SerializationError);
  });

  it("a non-serializable STEP RESULT fails the run terminally (not an infinite infra-retry)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "ser1");
    const wf = defineWorkflow("w", (ctx) => ctx.run("a", () => BigInt(10) as unknown));
    const out = await executeRun("ser1", {
      workflow: wf,
      journal: j,
      loaded: await j.load("ser1"),
    });
    expect(out.kind).toBe("failed"); // terminally failed — NOT { kind: "retry" }
    if (out.kind === "failed") expect(out.error.name).toBe("SerializationError");
    expect((await j.load("ser1")).some((l) => l.entry.kind === "fail")).toBe(true); // observable terminal
  });

  it("a non-serializable RETURN VALUE fails the run terminally", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "ser2");
    const wf = defineWorkflow("w", () => Promise.resolve(BigInt(10) as unknown));
    const out = await executeRun("ser2", {
      workflow: wf,
      journal: j,
      loaded: await j.load("ser2"),
    });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.name).toBe("SerializationError");
    expect((await j.load("ser2")).some((l) => l.entry.kind === "fail")).toBe(true);
  });
});

describe("ctx.* nesting guard (pure)", () => {
  it("fails loud if a ctx command is called inside a ctx.run step fn", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "nest1");
    const wf = defineWorkflow("w", (ctx) =>
      ctx.run("outer", async () => {
        await ctx.run("inner", () => "x"); // illegal: a ctx command inside a step fn
        return "y";
      }),
    );
    const out = await executeRun("nest1", {
      workflow: wf,
      journal: j,
      loaded: await j.load("nest1"),
    });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.name).toBe("StepNestingError");
  });

  it("guards a non-run command (ctx.sleep) inside a step fn too — before any scheduling", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "nest2");
    const wf = defineWorkflow("w", (ctx) =>
      ctx.run("outer", async () => {
        await ctx.sleep("nap", 10); // illegal — the default (unavailable) scheduler is never reached
        return "y";
      }),
    );
    const out = await executeRun("nest2", {
      workflow: wf,
      journal: j,
      loaded: await j.load("nest2"),
    });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.name).toBe("StepNestingError");
  });

  it("still allows ctx.key()/ctx.get()/ctx.assert() inside a step fn (they are not commands)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "nest3");
    const wf = defineWorkflow("w", (ctx) =>
      ctx.run("s", () => {
        ctx.assert(true); // passing assert — allowed inside a step
        const dep = ctx.get<string>("tenant"); // dependency lookup — allowed
        return `${ctx.key()}:${dep}`; // ctx.key() = `${runId}:${activeStepSeq}` — the idempotency key
      }),
    );
    const out = await executeRun("nest3", {
      workflow: wf,
      journal: j,
      loaded: await j.load("nest3"),
      dependencies: { tenant: "acme" },
    });
    expect(out).toEqual({ kind: "completed", value: "nest3:0:acme" });
  });

  // Every suspending/journaling command guards itself with assertNotInStep (nesting interleaves command
  // seqs → the run would complete once then fail every replay). run/sleep/now are pinned above + in the
  // fan-out suite; cover the rest so a refactor can't silently drop the guard from one of them.
  const nestChild = defineWorkflow("child", () => Promise.resolve(1));
  const NESTED: ReadonlyArray<readonly [string, (ctx: Ctx) => unknown]> = [
    ["random", (ctx) => ctx.random()],
    ["uuid", (ctx) => ctx.uuid()],
    ["await", (ctx) => ctx.await("x")],
    ["call", (ctx) => ctx.call(nestChild, undefined)],
    ["startCall", (ctx) => ctx.startCall(nestChild, undefined)],
    ["startSleep", (ctx) => ctx.startSleep("s", 10)],
    ["startAwait", (ctx) => ctx.startAwait("x")],
    ["continueAsNew", (ctx) => ctx.continueAsNew({})],
  ];
  for (const [label, invoke] of NESTED) {
    it(`ctx.${label} inside a ctx.run step fn throws StepNestingError`, async () => {
      const j = new InMemoryJournal();
      const run = `nest-${label}`;
      await seedStart(j, run);
      const wf = defineWorkflow("w", (ctx) =>
        ctx.run("outer", async () => {
          await invoke(ctx); // illegal: a command issued inside a step fn
          return 1;
        }),
      );
      const out = await executeRun(run, { workflow: wf, journal: j, loaded: await j.load(run) });
      expect(out).toMatchObject({ kind: "failed", error: { name: "StepNestingError" } });
    });
  }
});

describe("ctx.now/random/uuid — fence & divergence (pure)", () => {
  it("a zombie is fenced at a value command (now), reaching haltFenced — no progress", async () => {
    const inner = new InMemoryJournal();
    await seedStart(inner, "vz1");
    const j: Journal = {
      load: (r) => inner.load(r),
      append: (r, a: JournalAppend) =>
        a.msgID === msgId.value("vz1", 0)
          ? Promise.reject(new FencedError(r, "injected: another worker won the now command"))
          : inner.append(r, a),
    };
    const wf = defineWorkflow("w", (ctx) => ctx.now());
    const out = await executeRun("vz1", {
      workflow: wf,
      journal: j,
      loaded: await j.load("vz1"),
      clock: () => 7,
    });
    expect(out).toEqual({ kind: "fenced" });
  });

  it("fails loud on value-command kind divergence (recorded now, replayed as random)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "vd1");
    // seq 0 was recorded as a now(); this incarnation calls ctx.random() there (value-vs-value divergence).
    await append(j, "vd1", stamp({ kind: "now", seq: 0, value: 123 }, 0), msgId.value("vd1", 0));
    const wf = defineWorkflow("w", (ctx) => ctx.random());
    const out = await executeRun("vd1", { workflow: wf, journal: j, loaded: await j.load("vd1") });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.name).toBe("NonDeterminismError");
  });
});

describe("ctx.sleep (durable timers, pure)", () => {
  function recordingScheduler() {
    const wakes: Array<{ seq: number; dueMs: number }> = [];
    const scheduler = {
      scheduleWake: (_runId: string, _wf: string, seq: number, dueMs: number) => {
        wakes.push({ seq, dueMs });
        return Promise.resolve();
      },
    };
    return { wakes, scheduler };
  }

  it("suspends at the timer and resumes when it fires (prior steps not re-run)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "s1");
    const { wakes, scheduler } = recordingScheduler();
    let ran = 0;
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.run("before", () => {
        ran += 1;
        return 1;
      });
      await ctx.sleep("nap", 1000);
      return ctx.run("after", () => {
        ran += 1;
        return 2;
      });
    });
    const out1 = await executeRun("s1", {
      workflow: wf,
      journal: j,
      loaded: await j.load("s1"),
      scheduler,
      clock: () => 5000,
    });
    expect(out1).toEqual({ kind: "suspended" });
    expect(wakes).toEqual([{ seq: 1, dueMs: 6000 }]); // sleep is seq 1; due = clock + 1000
    expect(ran).toBe(1); // only "before" ran

    // the timer fires → the worker records a resolve for the sleep's seq
    await append(
      j,
      "s1",
      stamp({ kind: "resolve", seq: -1, resolvesSeq: 1 }, 0),
      msgId.resolve("s1", 1),
    );

    const out2 = await executeRun("s1", {
      workflow: wf,
      journal: j,
      loaded: await j.load("s1"),
      scheduler,
      clock: () => 9999,
    });
    expect(out2).toEqual({ kind: "completed", value: 2 });
    expect(ran).toBe(2); // "before" replayed (not re-run); "after" ran
  });

  it("re-ensures the schedule and re-suspends if the timer has not fired (deterministic dueMs)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "s2");
    const { wakes, scheduler } = recordingScheduler();
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.sleep("nap", 1000);
      return "woke";
    });
    const out1 = await executeRun("s2", {
      workflow: wf,
      journal: j,
      loaded: await j.load("s2"),
      scheduler,
      clock: () => 5000,
    });
    expect(out1).toEqual({ kind: "suspended" });
    const out2 = await executeRun("s2", {
      workflow: wf,
      journal: j,
      loaded: await j.load("s2"),
      scheduler,
      clock: () => 99999, // a different clock — must be ignored on replay
    });
    expect(out2).toEqual({ kind: "suspended" });
    expect(wakes).toEqual([
      { seq: 0, dueMs: 6000 },
      { seq: 0, dueMs: 6000 }, // re-ensured with the RECORDED dueMs, not the new clock
    ]);
  });

  it("sleep(0) still suspends and schedules a wake at the current clock (no short-circuit)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "sleep0");
    const { wakes, scheduler } = recordingScheduler();
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.sleep("immediate", 0);
      return "woke";
    });
    const o1 = await executeRun("sleep0", {
      workflow: wf,
      journal: j,
      loaded: await j.load("sleep0"),
      scheduler,
      clock: () => 5000,
    });
    expect(o1).toEqual({ kind: "suspended" }); // a 0/past deadline still parks deterministically
    expect(wakes).toEqual([{ seq: 0, dueMs: 5000 }]); // dueMs = clock + 0
    await append(
      j,
      "sleep0",
      stamp({ kind: "resolve", seq: -1, resolvesSeq: 0 }, 0),
      msgId.resolve("sleep0", 0),
    );
    const o2 = await executeRun("sleep0", {
      workflow: wf,
      journal: j,
      loaded: await j.load("sleep0"),
      scheduler,
      clock: () => 5000,
    });
    expect(o2).toEqual({ kind: "completed", value: "woke" });
  });
});

describe("rock-solid: infra-retry & more sleep edge cases", () => {
  const okScheduler = { scheduleWake: () => Promise.resolve() };

  it("a transient infra error during an append RETRIES the run (does not fail it)", async () => {
    const inner = new InMemoryJournal();
    await seedStart(inner, "ir1");
    const j: Journal = {
      load: (r) => inner.load(r),
      append: (r, a: JournalAppend) =>
        a.msgID === msgId.stepBegin("ir1", 0)
          ? Promise.reject(new Error("transient publish timeout")) // NOT a FencedError
          : inner.append(r, a),
    };
    let ran = 0;
    const wf = defineWorkflow("w", (ctx) =>
      ctx.run("a", () => {
        ran += 1;
        return "x";
      }),
    );
    const out = await executeRun("ir1", { workflow: wf, journal: j, loaded: await j.load("ir1") });
    expect(out.kind).toBe("retry"); // retry, NOT failed
    expect(ran).toBe(0); // step-begin failed before the effect
  });

  it("a scheduleWake failure RETRIES the run (does not fail it)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "ir2");
    const scheduler = { scheduleWake: () => Promise.reject(new Error("schedule publish failed")) };
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.sleep("nap", 1000);
      return "woke";
    });
    const out = await executeRun("ir2", {
      workflow: wf,
      journal: j,
      loaded: await j.load("ir2"),
      scheduler,
    });
    expect(out.kind).toBe("retry");
  });

  it("a zombie is fenced at the sleep command, before scheduling", async () => {
    const inner = new InMemoryJournal();
    await seedStart(inner, "se1");
    const wakes: number[] = [];
    const scheduler = {
      scheduleWake: (_r: string, _w: string, seq: number) => {
        wakes.push(seq);
        return Promise.resolve();
      },
    };
    const j: Journal = {
      load: (r) => inner.load(r),
      append: (r, a: JournalAppend) =>
        a.msgID === msgId.command("se1", 0)
          ? Promise.reject(new FencedError(r, "injected: lost the sleep command"))
          : inner.append(r, a),
    };
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.sleep("nap", 1000);
      return "woke";
    });
    const out = await executeRun("se1", {
      workflow: wf,
      journal: j,
      loaded: await j.load("se1"),
      scheduler,
    });
    expect(out).toEqual({ kind: "fenced" });
    expect(wakes).toEqual([]); // never scheduled — fenced first
  });

  it("fails loud on sleep name divergence", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "se2");
    await append(
      j,
      "se2",
      stamp({ kind: "sleep", seq: 0, name: "X", dueMs: 6000 }, 0),
      msgId.command("se2", 0),
    );
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.sleep("Y", 1000);
      return "woke";
    });
    const out = await executeRun("se2", {
      workflow: wf,
      journal: j,
      loaded: await j.load("se2"),
      scheduler: okScheduler,
    });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.name).toBe("NonDeterminismError");
  });

  it("fails loud when a step position is replayed as a sleep (kind divergence)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "se3");
    await append(
      j,
      "se3",
      stamp({ kind: "step-begin", seq: 0, name: "a", key: "se3:0" }, 0),
      msgId.stepBegin("se3", 0),
    );
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.sleep("nap", 1000);
      return "woke";
    });
    const out = await executeRun("se3", {
      workflow: wf,
      journal: j,
      loaded: await j.load("se3"),
      scheduler: okScheduler,
    });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.name).toBe("NonDeterminismError");
  });

  it("two sequential sleeps each suspend and resume independently", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "se4");
    const wakes: number[] = [];
    const scheduler = {
      scheduleWake: (_r: string, _w: string, seq: number) => {
        wakes.push(seq);
        return Promise.resolve();
      },
    };
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.sleep("a", 100);
      await ctx.sleep("b", 200);
      return "done";
    });
    const o1 = await executeRun("se4", {
      workflow: wf,
      journal: j,
      loaded: await j.load("se4"),
      scheduler,
      clock: () => 1000,
    });
    expect(o1).toEqual({ kind: "suspended" });
    expect(wakes).toEqual([0]);

    await append(
      j,
      "se4",
      stamp({ kind: "resolve", seq: -1, resolvesSeq: 0 }, 0),
      msgId.resolve("se4", 0),
    );
    const o2 = await executeRun("se4", {
      workflow: wf,
      journal: j,
      loaded: await j.load("se4"),
      scheduler,
      clock: () => 1000,
    });
    expect(o2).toEqual({ kind: "suspended" });
    expect(wakes).toEqual([0, 1]); // sleep "b" (seq 1) now scheduled

    await append(
      j,
      "se4",
      stamp({ kind: "resolve", seq: -1, resolvesSeq: 1 }, 0),
      msgId.resolve("se4", 1),
    );
    const o3 = await executeRun("se4", {
      workflow: wf,
      journal: j,
      loaded: await j.load("se4"),
      scheduler,
      clock: () => 1000,
    });
    expect(o3).toEqual({ kind: "completed", value: "done" });
  });
});

describe("ctx.await (signals, pure)", () => {
  const sig = (name: string, data: unknown, id: string) =>
    stamp({ kind: "signal", seq: -1, name, data, signalId: id }, 0);

  it("suspends until a signal arrives, then resumes with its data", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "aw1");
    const wf = defineWorkflow("w", async (ctx) => {
      const approval = await ctx.await<{ ok: boolean }>("payment");
      return approval.ok ? "shipped" : "abandoned";
    });
    const o1 = await executeRun("aw1", { workflow: wf, journal: j, loaded: await j.load("aw1") });
    expect(o1).toEqual({ kind: "suspended" });
    await append(j, "aw1", sig("payment", { ok: true }, "s1"), msgId.signal("aw1", "s1"));
    const o2 = await executeRun("aw1", { workflow: wf, journal: j, loaded: await j.load("aw1") });
    expect(o2).toEqual({ kind: "completed", value: "shipped" });
  });

  it("an early signal (arriving before the await) resolves it immediately", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "aw2");
    await append(j, "aw2", sig("go", 42, "s1"), msgId.signal("aw2", "s1"));
    const wf = defineWorkflow("w", (ctx) => ctx.await<number>("go"));
    const o = await executeRun("aw2", { workflow: wf, journal: j, loaded: await j.load("aw2") });
    expect(o).toEqual({ kind: "completed", value: 42 });
  });

  it("matches signals to awaits FIFO, by name", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "aw3");
    await append(j, "aw3", sig("n", "first", "s1"), msgId.signal("aw3", "s1"));
    await append(j, "aw3", sig("n", "second", "s2"), msgId.signal("aw3", "s2"));
    const wf = defineWorkflow("w", async (ctx) => {
      const a = await ctx.await<string>("n");
      const b = await ctx.await<string>("n");
      return `${a},${b}`;
    });
    const o = await executeRun("aw3", { workflow: wf, journal: j, loaded: await j.load("aw3") });
    expect(o).toEqual({ kind: "completed", value: "first,second" });
  });

  it("a zombie is fenced at the await command", async () => {
    const inner = new InMemoryJournal();
    await seedStart(inner, "aw4");
    const j: Journal = {
      load: (r) => inner.load(r),
      append: (r, a: JournalAppend) =>
        a.msgID === msgId.command("aw4", 0)
          ? Promise.reject(new FencedError(r, "injected"))
          : inner.append(r, a),
    };
    const wf = defineWorkflow("w", (ctx) => ctx.await<number>("x"));
    const o = await executeRun("aw4", { workflow: wf, journal: j, loaded: await j.load("aw4") });
    expect(o).toEqual({ kind: "fenced" });
  });

  it("fails loud on await name divergence", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "aw5");
    await append(j, "aw5", stamp({ kind: "await", seq: 0, name: "X" }, 0), msgId.command("aw5", 0));
    const wf = defineWorkflow("w", (ctx) => ctx.await<number>("Y"));
    const o = await executeRun("aw5", { workflow: wf, journal: j, loaded: await j.load("aw5") });
    expect(o.kind).toBe("failed");
    if (o.kind === "failed") expect(o.error.name).toBe("NonDeterminismError");
  });

  it("fails loud when a step position is replayed as an await (kind divergence)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "aw6");
    await append(
      j,
      "aw6",
      stamp({ kind: "step-begin", seq: 0, name: "a", key: "aw6:0" }, 0),
      msgId.stepBegin("aw6", 0),
    );
    const wf = defineWorkflow("w", (ctx) => ctx.await<number>("x"));
    const o = await executeRun("aw6", { workflow: wf, journal: j, loaded: await j.load("aw6") });
    expect(o.kind).toBe("failed");
    if (o.kind === "failed") expect(o.error.name).toBe("NonDeterminismError");
  });

  it("matches signals to awaits by NAME, independent of global arrival order", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "aw7");
    // signals arrive b-then-a; awaits are a-then-b
    await append(j, "aw7", sig("b", "B", "s1"), msgId.signal("aw7", "s1"));
    await append(j, "aw7", sig("a", "A", "s2"), msgId.signal("aw7", "s2"));
    const wf = defineWorkflow("w", async (ctx) => {
      const a = await ctx.await<string>("a");
      const b = await ctx.await<string>("b");
      return `${a}${b}`;
    });
    const o = await executeRun("aw7", { workflow: wf, journal: j, loaded: await j.load("aw7") });
    expect(o).toEqual({ kind: "completed", value: "AB" });
  });

  it("FIFO holds across suspensions (the second signal arrives later)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "aw8");
    const wf = defineWorkflow("w", async (ctx) => {
      const a = await ctx.await<string>("n");
      const b = await ctx.await<string>("n");
      return `${a}+${b}`;
    });
    await append(j, "aw8", sig("n", "one", "s1"), msgId.signal("aw8", "s1"));
    const o1 = await executeRun("aw8", { workflow: wf, journal: j, loaded: await j.load("aw8") });
    expect(o1).toEqual({ kind: "suspended" }); // await#1 resolved "one"; await#2 waiting
    await append(j, "aw8", sig("n", "two", "s2"), msgId.signal("aw8", "s2"));
    const o2 = await executeRun("aw8", { workflow: wf, journal: j, loaded: await j.load("aw8") });
    expect(o2).toEqual({ kind: "completed", value: "one+two" });
  });

  it("an unconsumed (never-awaited) signal does not break completion", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "aw9");
    await append(j, "aw9", sig("never-awaited", 1, "s1"), msgId.signal("aw9", "s1"));
    const wf = defineWorkflow("w", (ctx) => ctx.run("a", () => "done"));
    const o = await executeRun("aw9", { workflow: wf, journal: j, loaded: await j.load("aw9") });
    expect(o).toEqual({ kind: "completed", value: "done" });
  });

  it("composes sleep and await in one workflow", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "aw10");
    const wakes: number[] = [];
    const scheduler = {
      scheduleWake: (_r: string, _w: string, seq: number) => {
        wakes.push(seq);
        return Promise.resolve();
      },
    };
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.sleep("nap", 100);
      const x = await ctx.await<number>("go");
      return x * 2;
    });
    const o1 = await executeRun("aw10", {
      workflow: wf,
      journal: j,
      loaded: await j.load("aw10"),
      scheduler,
      clock: () => 1000,
    });
    expect(o1).toEqual({ kind: "suspended" }); // at the sleep
    await append(
      j,
      "aw10",
      stamp({ kind: "resolve", seq: -1, resolvesSeq: 0 }, 0),
      msgId.resolve("aw10", 0),
    );
    const o2 = await executeRun("aw10", {
      workflow: wf,
      journal: j,
      loaded: await j.load("aw10"),
      scheduler,
      clock: () => 1000,
    });
    expect(o2).toEqual({ kind: "suspended" }); // past the sleep, now at the await
    await append(j, "aw10", sig("go", 21, "s1"), msgId.signal("aw10", "s1"));
    const o3 = await executeRun("aw10", {
      workflow: wf,
      journal: j,
      loaded: await j.load("aw10"),
      scheduler,
      clock: () => 1000,
    });
    expect(o3).toEqual({ kind: "completed", value: 42 });
  });
});

describe("ctx.call (sub-workflows, pure)", () => {
  const recordingChildren = () => {
    const starts: ChildStart[] = [];
    return { starts, children: { start: (s: ChildStart) => (starts.push(s), Promise.resolve()) } };
  };

  it("suspends on a call, starting the child, then resumes with the child's result", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "c1");
    const { starts, children } = recordingChildren();
    const child = defineWorkflow("child", (_ctx, input: number) => Promise.resolve(input * 2));
    const wf = defineWorkflow("w", async (ctx) => {
      const r = await ctx.call(child, 21);
      return `got ${r}`;
    });
    const o1 = await executeRun("c1", {
      workflow: wf,
      journal: j,
      loaded: await j.load("c1"),
      children,
    });
    expect(o1).toEqual({ kind: "suspended" });
    expect(starts).toHaveLength(1);
    expect(starts[0]).toMatchObject({
      childRunId: "c1_0",
      workflow: "child",
      input: 21,
      parent: { runId: "c1", seq: 0, wf: "w" },
    });
    await append(
      j,
      "c1",
      stamp({ kind: "resolve", seq: -1, resolvesSeq: 0, value: 42 }, 0),
      msgId.resolve("c1", 0),
    );
    const o2 = await executeRun("c1", {
      workflow: wf,
      journal: j,
      loaded: await j.load("c1"),
      children,
    });
    expect(o2).toEqual({ kind: "completed", value: "got 42" });
  });

  it("uses opts.runId to override the child's deterministic runId", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "co");
    const { starts, children } = recordingChildren();
    const child = defineWorkflow("child", () => Promise.resolve("x"));
    const wf = defineWorkflow("w", (ctx) => ctx.call(child, 1, { runId: "custom_child" }));
    const o = await executeRun("co", {
      workflow: wf,
      journal: j,
      loaded: await j.load("co"),
      children,
    });
    expect(o.kind).toBe("suspended");
    expect(starts).toHaveLength(1);
    expect(starts[0]?.childRunId).toBe("custom_child"); // not the default "co_0"
  });

  it("a sub-workflow failure fails the parent", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "c2");
    const { children } = recordingChildren();
    const child = defineWorkflow("child", () => Promise.resolve("x"));
    const wf = defineWorkflow("w", (ctx) => ctx.call(child, undefined));
    const o1 = await executeRun("c2", {
      workflow: wf,
      journal: j,
      loaded: await j.load("c2"),
      children,
    });
    expect(o1.kind).toBe("suspended");
    await append(
      j,
      "c2",
      stamp(
        {
          kind: "resolve",
          seq: -1,
          resolvesSeq: 0,
          error: { name: "WorkflowFailure", message: "boom" },
        },
        0,
      ),
      msgId.resolve("c2", 0),
    );
    const o2 = await executeRun("c2", {
      workflow: wf,
      journal: j,
      loaded: await j.load("c2"),
      children,
    });
    expect(o2.kind).toBe("failed");
    if (o2.kind === "failed") expect(o2.error.name).toBe("WorkflowFailure");
  });

  it("a zombie is fenced at the call command, before starting the child", async () => {
    const inner = new InMemoryJournal();
    await seedStart(inner, "c3");
    const { starts, children } = recordingChildren();
    const j: Journal = {
      load: (r) => inner.load(r),
      append: (r, a: JournalAppend) =>
        a.msgID === msgId.command("c3", 0)
          ? Promise.reject(new FencedError(r, "injected"))
          : inner.append(r, a),
    };
    const child = defineWorkflow("child", () => Promise.resolve("x"));
    const wf = defineWorkflow("w", (ctx) => ctx.call(child, undefined));
    const o = await executeRun("c3", {
      workflow: wf,
      journal: j,
      loaded: await j.load("c3"),
      children,
    });
    expect(o).toEqual({ kind: "fenced" });
    expect(starts).toHaveLength(0); // never started the child
  });

  it("fails loud on call divergence (different sub-workflow at a position)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "c4");
    const { children } = recordingChildren();
    await append(
      j,
      "c4",
      stamp(
        { kind: "call", seq: 0, name: "childX", childRunId: "c4_0", wf: "childX", input: 1 },
        0,
      ),
      msgId.command("c4", 0),
    );
    const childY = defineWorkflow("childY", () => Promise.resolve("y"));
    const wf = defineWorkflow("w", (ctx) => ctx.call(childY, 1));
    const o = await executeRun("c4", {
      workflow: wf,
      journal: j,
      loaded: await j.load("c4"),
      children,
    });
    expect(o.kind).toBe("failed");
    if (o.kind === "failed") expect(o.error.name).toBe("NonDeterminismError");
  });

  it("re-ensures the child start on replay (same deterministic childRunId) until it completes", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "c5");
    const { starts, children } = recordingChildren();
    const child = defineWorkflow("child", () => Promise.resolve("x"));
    const wf = defineWorkflow("w", (ctx) => ctx.call(child, 7));
    const o1 = await executeRun("c5", {
      workflow: wf,
      journal: j,
      loaded: await j.load("c5"),
      children,
    });
    expect(o1.kind).toBe("suspended");
    const o2 = await executeRun("c5", {
      workflow: wf,
      journal: j,
      loaded: await j.load("c5"),
      children,
    });
    expect(o2.kind).toBe("suspended");
    expect(starts).toHaveLength(2); // re-ensured (idempotent at the shell)
    expect(starts[0]?.childRunId).toBe("c5_0");
    expect(starts[1]?.childRunId).toBe("c5_0"); // same deterministic id
  });

  it("two sequential calls each suspend and resume independently", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "c6");
    const { starts, children } = recordingChildren();
    const a = defineWorkflow("a", () => Promise.resolve("A"));
    const b = defineWorkflow("b", () => Promise.resolve("B"));
    const wf = defineWorkflow("w", async (ctx) => {
      const x = await ctx.call(a, 1);
      const y = await ctx.call(b, 2);
      return `${x}${y}`;
    });
    const o1 = await executeRun("c6", {
      workflow: wf,
      journal: j,
      loaded: await j.load("c6"),
      children,
    });
    expect(o1.kind).toBe("suspended");
    expect(starts.map((s) => s.childRunId)).toEqual(["c6_0"]);
    await append(
      j,
      "c6",
      stamp({ kind: "resolve", seq: -1, resolvesSeq: 0, value: "A" }, 0),
      msgId.resolve("c6", 0),
    );
    const o2 = await executeRun("c6", {
      workflow: wf,
      journal: j,
      loaded: await j.load("c6"),
      children,
    });
    expect(o2.kind).toBe("suspended");
    expect(starts.map((s) => s.childRunId)).toEqual(["c6_0", "c6_1"]); // call "b" is seq 1
    await append(
      j,
      "c6",
      stamp({ kind: "resolve", seq: -1, resolvesSeq: 1, value: "B" }, 0),
      msgId.resolve("c6", 1),
    );
    const o3 = await executeRun("c6", {
      workflow: wf,
      journal: j,
      loaded: await j.load("c6"),
      children,
    });
    expect(o3).toEqual({ kind: "completed", value: "AB" });
  });

  it("fails loud when a step position is replayed as a call (kind divergence)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "c7");
    const { children } = recordingChildren();
    await append(
      j,
      "c7",
      stamp({ kind: "step-begin", seq: 0, name: "s", key: "c7:0" }, 0),
      msgId.stepBegin("c7", 0),
    );
    const child = defineWorkflow("child", () => Promise.resolve("x"));
    const wf = defineWorkflow("w", (ctx) => ctx.call(child, undefined));
    const o = await executeRun("c7", {
      workflow: wf,
      journal: j,
      loaded: await j.load("c7"),
      children,
    });
    expect(o.kind).toBe("failed");
    if (o.kind === "failed") expect(o.error.name).toBe("NonDeterminismError");
  });

  it("a child-start failure RETRIES the run (does not fail it)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "c8");
    const children = { start: () => Promise.reject(new Error("child start publish failed")) };
    const child = defineWorkflow("child", () => Promise.resolve("x"));
    const wf = defineWorkflow("w", (ctx) => ctx.call(child, undefined));
    const o = await executeRun("c8", {
      workflow: wf,
      journal: j,
      loaded: await j.load("c8"),
      children,
    });
    expect(o.kind).toBe("retry");
  });
});

describe("ctx.continueAsNew (chained generations, pure)", () => {
  // A looping workflow: tick once per generation, continue until n reaches the limit, then return.
  const loopWf = (counter: Record<string, number>, limit = 3) =>
    defineWorkflow("w", async (ctx, input: { n: number }) => {
      await ctx.run("tick", () => {
        counter.tick = (counter.tick ?? 0) + 1;
        return input.n;
      });
      if (input.n >= limit) return `done@${input.n}`;
      return ctx.continueAsNew({ n: input.n + 1 });
    });

  const seedStartWith = (
    j: Journal,
    runId: string,
    input: unknown,
    extra: { generation?: number; base?: string } = {},
  ): Promise<void> =>
    append(
      j,
      runId,
      stamp({ kind: "start", seq: 0, wf: "w", wfVersion: 1, input, ...extra }, 0),
      msgId.start(runId),
    );

  it("records a `continued` terminal and reports the next generation (step ran once)", async () => {
    const j = new InMemoryJournal();
    await seedStartWith(j, "c1", { n: 1 });
    const counter: Record<string, number> = {};
    const out = await executeRun("c1", {
      workflow: loopWf(counter),
      journal: j,
      loaded: await j.load("c1"),
    });
    expect(out.kind).toBe("continue");
    if (out.kind === "continue") {
      // version defaults to the current generation's version (the "w" token is version 1).
      expect(out.next).toEqual({ nextRunId: "c1_g1", input: { n: 2 }, generation: 1, base: "c1", version: 1 }); // prettier-ignore
    }
    const cont = (await j.load("c1")).find((l) => l.entry.kind === "continued");
    expect(cont?.entry).toMatchObject({ kind: "continued", nextRunId: "c1_g1", version: 1 });
    expect(counter.tick).toBe(1);
  });

  it("continueAsNew({version}) migrates the next generation onto that version (persisted on `continued`)", async () => {
    const j = new InMemoryJournal();
    await seedStartWith(j, "cv", { n: 1 });
    // The in-flight migration lever: roll the chain explicitly onto version 2. The current generation
    // still runs as v1 (its `start` recorded wfVersion 1); only the NEXT generation moves to v2.
    const migrateWf = defineWorkflow("w", (ctx, input: { n: number }) =>
      ctx.continueAsNew({ n: input.n + 1 }, { version: 2 }),
    );
    const out = await executeRun("cv", { workflow: migrateWf, journal: j, loaded: await j.load("cv") }); // prettier-ignore
    expect(out.kind).toBe("continue");
    if (out.kind === "continue") expect(out.next.version).toBe(2);
    // Persisted on the terminal so a crash mid-rollover deterministically re-drives to v2 (not v1):
    const cont = (await j.load("cv")).find((l) => l.entry.kind === "continued");
    expect(cont?.entry).toMatchObject({ kind: "continued", nextRunId: "cv_g1", version: 2 });
  });

  it("continueAsNew rejects a non-positive-integer version (fail loud, not a silent default)", async () => {
    const j = new InMemoryJournal();
    await seedStartWith(j, "cvbad", { n: 1 });
    const badWf = defineWorkflow("w", (ctx, input: { n: number }) =>
      ctx.continueAsNew({ n: input.n }, { version: 0 }),
    );
    const out = await executeRun("cvbad", { workflow: badWf, journal: j, loaded: await j.load("cvbad") }); // prettier-ignore
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.message).toMatch(/positive integer/);
  });

  it("replaying a continued generation is idempotent (same next; step NOT re-run)", async () => {
    const j = new InMemoryJournal();
    await seedStartWith(j, "c2", { n: 1 });
    const counter: Record<string, number> = {};
    await executeRun("c2", { workflow: loopWf(counter), journal: j, loaded: await j.load("c2") });
    expect(counter.tick).toBe(1);
    // a second incarnation over the SAME journal (already has `continued`) replays everything
    const out2 = await executeRun("c2", {
      workflow: loopWf(counter),
      journal: j,
      loaded: beforeTerminal(await j.load("c2")),
    });
    expect(out2.kind).toBe("continue");
    if (out2.kind === "continue") expect(out2.next.nextRunId).toBe("c2_g1");
    expect(counter.tick).toBe(1); // UNCHANGED — the side effect did not fire again
    expect((await j.load("c2")).filter((l) => l.entry.kind === "continued")).toHaveLength(1);
  });

  it("the next generation begins fresh with the carried input", async () => {
    const j = new InMemoryJournal();
    await seedStartWith(j, "c3_g1", { n: 2 }, { generation: 1, base: "c3" });
    const counter: Record<string, number> = {};
    const out = await executeRun("c3_g1", {
      workflow: loopWf(counter),
      journal: j,
      loaded: await j.load("c3_g1"),
    });
    expect(out.kind).toBe("continue");
    if (out.kind === "continue") {
      expect(out.next).toEqual({ nextRunId: "c3_g2", input: { n: 3 }, generation: 2, base: "c3", version: 1 }); // prettier-ignore
    }
  });

  it("drives a whole chain to completion, each generation's journal bounded", async () => {
    // Simulate the worker loop: executeRun → on continue, seed the next gen's start, repeat.
    const j = new InMemoryJournal();
    await seedStartWith(j, "c4", { n: 1 });
    const counter: Record<string, number> = {};
    let runId = "c4";
    for (let guard = 0; ; guard++) {
      if (guard > 10) throw new Error("chain did not converge");
      const out = await executeRun(runId, {
        workflow: loopWf(counter),
        journal: j,
        loaded: await j.load(runId),
      });
      if (out.kind === "completed") {
        expect(out.value).toBe("done@3");
        break;
      }
      expect(out.kind).toBe("continue");
      if (out.kind !== "continue") break;
      await seedStartWith(j, out.next.nextRunId, out.next.input, {
        generation: out.next.generation,
        base: out.next.base,
      });
      runId = out.next.nextRunId;
    }
    expect(counter.tick).toBe(3); // n = 1,2,3 — exactly one tick per generation
    // every generation's journal stays small + bounded: start + tick(begin,end) + one terminal
    for (const id of ["c4", "c4_g1", "c4_g2"]) {
      expect((await j.load(id)).length).toBe(4);
    }
    expect((await j.load("c4")).some((l) => l.entry.kind === "continued")).toBe(true);
    expect((await j.load("c4_g2")).some((l) => l.entry.kind === "result")).toBe(true);
  });

  it("fails loud if continue-as-new is reached via a SHORTER path than recorded", async () => {
    const j = new InMemoryJournal();
    await seedStartWith(j, "c5", { n: 0 });
    // record TWO steps (seq 0 = "a", seq 1 = "b")
    await append(j, "c5", stamp({ kind: "step-begin", seq: 0, name: "a", key: "c5:0" }, 0), msgId.stepBegin("c5", 0)); // prettier-ignore
    await append(j, "c5", stamp({ kind: "step-end", seq: 0, name: "a", value: 1 }, 0), msgId.stepEnd("c5", 0)); // prettier-ignore
    await append(j, "c5", stamp({ kind: "step-begin", seq: 1, name: "b", key: "c5:1" }, 0), msgId.stepBegin("c5", 1)); // prettier-ignore
    await append(j, "c5", stamp({ kind: "step-end", seq: 1, name: "b", value: 2 }, 0), msgId.stepEnd("c5", 1)); // prettier-ignore
    // a workflow that consumes only "a" then continues — a shorter path (skips recorded "b")
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.run("a", () => 1);
      return ctx.continueAsNew({ n: 1 });
    });
    const out = await executeRun("c5", { workflow: wf, journal: j, loaded: await j.load("c5") });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.name).toBe("NonDeterminismError");
    expect((await j.load("c5")).some((l) => l.entry.kind === "continued")).toBe(false);
  });

  it("reports the journal's terminal (no next generation) if the run already finalized as a result", async () => {
    const j = new InMemoryJournal();
    await seedStartWith(j, "c6", { n: 1 });
    // a result terminal already landed (e.g. a prior, divergent incarnation)
    await append(j, "c6", stamp({ kind: "result", seq: NON_COMMAND_SEQ, value: "X" }, 0), msgId.terminal("c6")); // prettier-ignore
    let invoked = 0;
    const wf = defineWorkflow("w", (ctx) => {
      invoked += 1;
      return ctx.continueAsNew({ n: 2 });
    });
    const out = await executeRun("c6", { workflow: wf, journal: j, loaded: await j.load("c6") });
    expect(out).toEqual({ kind: "completed", value: "X" }); // the journal's outcome, not ours
    expect(invoked).toBe(0); // a finished run is never executed
    expect((await j.load("c6")).some((l) => l.entry.kind === "continued")).toBe(false);
    expect((await j.load("c6")).some((l) => l.entry.kind === "result")).toBe(true);
  });
});

describe("concurrency & chaos (multi-incarnation, pure)", () => {
  it("CAS dominates dedup: a duplicate msgID with a stale expect FENCES (fidelity guard)", async () => {
    // Guards the InMemoryJournal against regressing to dedup-first — the property is verified against a
    // real server in primitives.test.ts; this keeps the pure double faithful so the races below hold.
    const j = new InMemoryJournal();
    await seedStart(j, "cd");
    const last = (await j.load("cd")).at(-1)!.streamSeq;
    const mid = msgId.stepBegin("cd", 0);
    const begin = (): JournalEntry =>
      stamp({ kind: "step-begin", seq: 0, name: "a", key: "cd:0" }, 0);
    await j.append("cd", { entry: begin(), msgID: mid, expectLastSeq: last });
    await expect(
      j.append("cd", { entry: begin(), msgID: mid, expectLastSeq: last }), // stale expect, same msgID
    ).rejects.toBeInstanceOf(FencedError);
  });

  it("two incarnations race a fresh step: the side effect runs once, the loser is fenced", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "race1");
    let runs = 0;
    const wf = defineWorkflow("w", (ctx) =>
      ctx.run("a", () => {
        runs += 1;
        return "A";
      }),
    );
    const snap = await j.load("race1"); // both load the SAME pre-step state, then race
    const [x, y] = await Promise.all([
      executeRun("race1", { workflow: wf, journal: j, loaded: snap }),
      executeRun("race1", { workflow: wf, journal: j, loaded: snap }),
    ]);
    expect([x.kind, y.kind].sort()).toEqual(["completed", "fenced"]);
    expect(runs).toBe(1); // the fenced incarnation was stopped BEFORE the side effect
  });

  it("two incarnations race a value capture: exactly one value is journaled (no divergence)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "race2");
    const wf = defineWorkflow("w", (ctx) => ctx.uuid());
    const snap = await j.load("race2");
    const [x, y] = await Promise.all([
      executeRun("race2", { workflow: wf, journal: j, loaded: snap }),
      executeRun("race2", { workflow: wf, journal: j, loaded: snap }),
    ]);
    expect([x.kind, y.kind].sort()).toEqual(["completed", "fenced"]);
    expect((await j.load("race2")).filter((l) => l.entry.kind === "uuid")).toHaveLength(1);
  });

  it("five incarnations storm one run: each step runs exactly once, one completes", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "raceN");
    const counter: Record<string, number> = {};
    const snap = await j.load("raceN");
    const outs = await Promise.all(
      Array.from({ length: 5 }, () =>
        executeRun("raceN", {
          workflow: countingWf(counter, ["a", "b", "c"]),
          journal: j,
          loaded: snap,
        }),
      ),
    );
    expect(outs.filter((o) => o.kind === "completed")).toHaveLength(1);
    expect(outs.filter((o) => o.kind === "fenced")).toHaveLength(4);
    expect(counter).toEqual({ a: 1, b: 1, c: 1 }); // every side effect fired exactly once
  });

  it("a terminal CAS-loser ADOPTS the canonical journaled value (not its own)", async () => {
    // Two incarnations race the terminal append with a non-deterministic RETURN value (not journaled,
    // so they differ). The CAS loser must report the WINNER's journaled value — never its own — so the
    // worker's KV finalize stays consistent with what result() reads from the journal.
    const j = new InMemoryJournal();
    await seedStart(j, "adopt");
    let n = 0;
    const wf = defineWorkflow("w", () => Promise.resolve(++n)); // returns 1 and 2 across incarnations
    const snap = await j.load("adopt");
    const [x, y] = await Promise.all([
      executeRun("adopt", { workflow: wf, journal: j, loaded: snap }),
      executeRun("adopt", { workflow: wf, journal: j, loaded: snap }),
    ]);
    expect(x.kind).toBe("completed");
    expect(y.kind).toBe("completed");
    // identical despite ++n differing — the loser adopted the canonical value, not its own
    if (x.kind === "completed" && y.kind === "completed") expect(x.value).toBe(y.value);
    expect((await j.load("adopt")).filter((l) => l.entry.kind === "result")).toHaveLength(1);
  });

  it("a terminal FAIL CAS-loser ADOPTS the canonical journaled error (not its own)", async () => {
    // The result-adopt race on the FAILURE path: two incarnations throw DIFFERENT errors; the CAS loser
    // must report the WINNER's journaled error (failTerminal's adopt branch), never its own.
    const j = new InMemoryJournal();
    await seedStart(j, "adopt-fail");
    let n = 0;
    const wf = defineWorkflow("w", () => Promise.reject(new Error(`boom-${++n}`)));
    const snap = await j.load("adopt-fail");
    const [x, y] = await Promise.all([
      executeRun("adopt-fail", { workflow: wf, journal: j, loaded: snap }),
      executeRun("adopt-fail", { workflow: wf, journal: j, loaded: snap }),
    ]);
    expect(x.kind).toBe("failed");
    expect(y.kind).toBe("failed");
    expect(x).toEqual(y); // identical despite boom-1 vs boom-2 — the loser adopted the canonical error
    expect((await j.load("adopt-fail")).filter((l) => l.entry.kind === "fail")).toHaveLength(1);
  });

  it("a CONTINUED CAS-loser adopts the canonical rollover — one next generation, not two", async () => {
    // The adopt branch with the most to get wrong, because a `continued` terminal is not just a
    // value: it names the NEXT run. Two incarnations racing continueAsNew compute different inputs
    // (nothing about them is journaled before the terminal), and if the loser reported its OWN spec
    // the worker would start a second generation from it — two chains from one run, both live.
    const j = new InMemoryJournal();
    await seedStart(j, "adopt-can");
    let n = 0;
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.continueAsNew({ attempt: ++n });
      return "unreachable";
    });
    const snap = await j.load("adopt-can");
    const [x, y] = await Promise.all([
      executeRun("adopt-can", { workflow: wf, journal: j, loaded: snap }),
      executeRun("adopt-can", { workflow: wf, journal: j, loaded: snap }),
    ]);
    expect(x.kind).toBe("continue");
    expect(y.kind).toBe("continue");
    expect(x).toEqual(y); // the loser adopted the winner's spec — same nextRunId AND same input
    if (x.kind === "continue" && y.kind === "continue") {
      expect(x.next.input).toEqual(y.next.input);
      expect(x.next.nextRunId).toBe("adopt-can_g1");
      expect(x.next.generation).toBe(1);
    }
    expect((await j.load("adopt-can")).filter((l) => l.entry.kind === "continued")).toHaveLength(1);
  });

  it("a workflow with NO command boundary still completes under a pending cancel", async () => {
    // Not an adopt case, and worth stating because it is easy to assume otherwise: the cancel
    // hard-stop lands at the next COMMAND boundary, so a body that reaches its return without
    // touching `ctx` has no stop point and completion wins. The cancelled-adopt arm proper lives in
    // cancel.test.ts, where the body parks on a real boundary.
    const j = new InMemoryJournal();
    await seedStart(j, "cancel-no-boundary");
    await j.append("cancel-no-boundary", {
      entry: stamp({ kind: "cancel-requested", seq: NON_COMMAND_SEQ }, 0),
      msgID: msgId.cancel("cancel-no-boundary"),
      expectLastSeq: (await j.load("cancel-no-boundary")).at(-1)!.streamSeq,
    });
    const wf = defineWorkflow("w", () => Promise.resolve("value"));
    const out = await executeRun("cancel-no-boundary", {
      workflow: wf,
      journal: j,
      loaded: await j.load("cancel-no-boundary"),
    });
    expect(out).toEqual({ kind: "completed", value: "value" });
  });
});

describe("ctx.await timeout (durable, pure)", () => {
  const okScheduler = { scheduleWake: () => Promise.resolve() };
  const sig = (name: string, data: unknown, id: string): JournalEntry =>
    stamp({ kind: "signal", seq: NON_COMMAND_SEQ, name, data, signalId: id }, 0);
  const fireTimer = (j: Journal, runId: string, seq: number): Promise<void> =>
    append(j, runId, stamp({ kind: "resolve", seq: NON_COMMAND_SEQ, resolvesSeq: seq }, 0), msgId.resolve(runId, seq)); // prettier-ignore
  function recScheduler() {
    const wakes: { seq: number; dueMs: number }[] = [];
    return {
      wakes,
      scheduler: {
        scheduleWake: (_r: string, _w: string, seq: number, dueMs: number) => {
          wakes.push({ seq, dueMs });
          return Promise.resolve();
        },
      },
    };
  }

  it("schedules a wake at the absolute deadline and records dueMs", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "to0");
    const { wakes, scheduler } = recScheduler();
    const wf = defineWorkflow("w", (ctx) => ctx.await<string>("go", { timeoutMs: 1500 }));
    const o = await executeRun("to0", {
      workflow: wf,
      journal: j,
      loaded: await j.load("to0"),
      scheduler,
      clock: () => 1000,
    });
    expect(o).toEqual({ kind: "suspended" });
    expect(wakes).toEqual([{ seq: 0, dueMs: 2500 }]); // await is seq 0; due = clock + timeoutMs
    const aw = (await j.load("to0")).find((l) => l.entry.kind === "await");
    expect((aw?.entry as { dueMs?: number }).dueMs).toBe(2500);
  });

  it("a no-timeout await schedules no timer (suspends indefinitely)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "to-none");
    const { wakes, scheduler } = recScheduler();
    const wf = defineWorkflow("w", (ctx) => ctx.await<string>("go"));
    const o = await executeRun("to-none", {
      workflow: wf,
      journal: j,
      loaded: await j.load("to-none"),
      scheduler,
    });
    expect(o).toEqual({ kind: "suspended" });
    expect(wakes).toEqual([]);
  });

  it("times out when no signal arrives → DurableTimeoutError fails an uncaught run", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "to1");
    const wf = defineWorkflow("w", (ctx) => ctx.await<string>("go", { timeoutMs: 1000 }));
    const o1 = await executeRun("to1", {
      workflow: wf,
      journal: j,
      loaded: await j.load("to1"),
      scheduler: okScheduler,
      clock: () => 5000,
    });
    expect(o1).toEqual({ kind: "suspended" });
    await fireTimer(j, "to1", 0); // the timeout timer fires
    const o2 = await executeRun("to1", {
      workflow: wf,
      journal: j,
      loaded: await j.load("to1"),
      scheduler: okScheduler,
    });
    expect(o2.kind).toBe("failed");
    if (o2.kind === "failed") expect(o2.error.name).toBe("DurableTimeoutError");
  });

  it("the timeout is catchable — the workflow takes a fallback path", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "to2");
    const wf = defineWorkflow("w", async (ctx) => {
      try {
        return await ctx.await<string>("go", { timeoutMs: 500 });
      } catch {
        return "fallback";
      }
    });
    await executeRun("to2", {
      workflow: wf,
      journal: j,
      loaded: await j.load("to2"),
      scheduler: okScheduler,
      clock: () => 0,
    });
    await fireTimer(j, "to2", 0);
    const o = await executeRun("to2", {
      workflow: wf,
      journal: j,
      loaded: await j.load("to2"),
      scheduler: okScheduler,
    });
    expect(o).toEqual({ kind: "completed", value: "fallback" });
  });

  it("a signal that beats the timer resolves the await with its data", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "to3");
    const wf = defineWorkflow("w", (ctx) => ctx.await<string>("go", { timeoutMs: 1000 }));
    await executeRun("to3", {
      workflow: wf,
      journal: j,
      loaded: await j.load("to3"),
      scheduler: okScheduler,
      clock: () => 0,
    });
    await append(j, "to3", sig("go", "signalled", "s1"), msgId.signal("to3", "s1"));
    const o = await executeRun("to3", {
      workflow: wf,
      journal: j,
      loaded: await j.load("to3"),
      scheduler: okScheduler,
    });
    expect(o).toEqual({ kind: "completed", value: "signalled" });
  });

  it("the timer wins (deterministically) when its resolve was recorded before a late signal", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "to4");
    const wf = defineWorkflow("w", async (ctx) => {
      try {
        return await ctx.await<string>("go", { timeoutMs: 100 });
      } catch {
        return "timed-out";
      }
    });
    await executeRun("to4", {
      workflow: wf,
      journal: j,
      loaded: await j.load("to4"),
      scheduler: okScheduler,
      clock: () => 0,
    });
    await fireTimer(j, "to4", 0); // timer resolve recorded FIRST
    await append(j, "to4", sig("go", "late", "s1"), msgId.signal("to4", "s1")); // then a late signal
    const o = await executeRun("to4", {
      workflow: wf,
      journal: j,
      loaded: await j.load("to4"),
      scheduler: okScheduler,
    });
    expect(o).toEqual({ kind: "completed", value: "timed-out" }); // earlier stream-seq wins
  });

  it("re-ensures the timeout schedule on replay with the RECORDED dueMs (new clock ignored)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "to-replay");
    const { wakes, scheduler } = recScheduler();
    const wf = defineWorkflow("w", (ctx) => ctx.await<string>("go", { timeoutMs: 1000 }));
    const o1 = await executeRun("to-replay", {
      workflow: wf,
      journal: j,
      loaded: await j.load("to-replay"),
      scheduler,
      clock: () => 5000,
    });
    expect(o1).toEqual({ kind: "suspended" });
    // replay with a DIFFERENT clock, neither signal nor timer fired → re-ensure the SAME deadline
    const o2 = await executeRun("to-replay", {
      workflow: wf,
      journal: j,
      loaded: await j.load("to-replay"),
      scheduler,
      clock: () => 99999,
    });
    expect(o2).toEqual({ kind: "suspended" });
    expect(wakes).toEqual([
      { seq: 0, dueMs: 6000 },
      { seq: 0, dueMs: 6000 }, // recorded dueMs (5000+1000), NOT the new clock
    ]);
  });

  it("a timed-out await does NOT steal a later same-name await's signal (retry-with-timeout loop)", async () => {
    // The realistic pattern: await an approval with a timeout in a loop; on timeout, loop and wait
    // again. When the approval finally arrives it must resolve the CURRENT await. A prior timed-out
    // await consumed no signal, so it must not shift the FIFO match. (Regression: counting timed-out
    // awaits left the late signal orphaned and the run suspended forever.)
    const j = new InMemoryJournal();
    await seedStart(j, "fifo-to");
    const wf = defineWorkflow("w", async (ctx) => {
      for (let i = 0; i < 5; i += 1) {
        try {
          return await ctx.await<string>("approval", { timeoutMs: 100 });
        } catch {
          /* timed out — loop and wait again */
        }
      }
      return "gave-up";
    });
    const o1 = await executeRun("fifo-to", {
      workflow: wf,
      journal: j,
      loaded: await j.load("fifo-to"),
      scheduler: okScheduler,
      clock: () => 0,
    });
    expect(o1).toEqual({ kind: "suspended" }); // await#1 (seq 0) waiting
    await fireTimer(j, "fifo-to", 0); // await#1 times out (resolve recorded before any signal)
    const o2 = await executeRun("fifo-to", {
      workflow: wf,
      journal: j,
      loaded: await j.load("fifo-to"),
      scheduler: okScheduler,
      clock: () => 1000,
    });
    expect(o2).toEqual({ kind: "suspended" }); // await#1 threw→caught; await#2 (seq 1) now waiting
    await append(j, "fifo-to", sig("approval", "approved", "s1"), msgId.signal("fifo-to", "s1"));
    const o3 = await executeRun("fifo-to", {
      workflow: wf,
      journal: j,
      loaded: await j.load("fifo-to"),
      scheduler: okScheduler,
      clock: () => 2000,
    });
    expect(o3).toEqual({ kind: "completed", value: "approved" }); // resolves await#2, not orphaned
  });
});

// Value fidelity: a journaled value must replay byte-identically, INCLUDING falsy values. The engine
// does no `??`/`||` coalescing on recorded values (run returns `end.value`; resolutionOf returns
// `re.value`; settle returns `res.value`), so undefined/null/false/0/"" survive a round-trip exactly.
// These pin that — a regression guard against someone later adding a `value ?? fallback`. They also
// pin the one value JSON cannot preserve: a non-finite number (NaN/Infinity) is rejected (fail loud).
describe("value fidelity (falsy, undefined, and JSON edge values)", () => {
  const noopChildren: Children = { start: () => Promise.resolve() };
  const resolveOk = (j: Journal, run: string, seq: number, value: unknown) =>
    append(
      j,
      run,
      stamp({ kind: "resolve", seq: NON_COMMAND_SEQ, resolvesSeq: seq, value }, 0),
      msgId.resolve(run, seq),
    );

  const FALSY: ReadonlyArray<readonly [string, unknown]> = [
    ["undefined", undefined],
    ["null", null],
    ["false", false],
    ["zero", 0],
    ["empty-string", ""],
  ];

  for (const [label, v] of FALSY) {
    it(`a step returning ${label} replays EXACTLY (fn not re-run, value preserved)`, async () => {
      const j = new InMemoryJournal();
      const run = `vf-${label}`;
      await seedStart(j, run);
      let runs = 0;
      // Summarize the step value crisply (avoids toEqual's undefined-property leniency): type, string
      // form, and identity-vs-expected. Object.is holds on both the live value and the replayed one.
      const wf = defineWorkflow("w", async (ctx) => {
        const got = await ctx.run("s", () => {
          runs++;
          return v;
        });
        return `${typeof got}:${String(got)}:${Object.is(got, v)}`;
      });
      const expected = `${typeof v}:${String(v)}:true`;

      const out1 = await executeRun(run, { workflow: wf, journal: j, loaded: await j.load(run) });
      expect(out1).toEqual({ kind: "completed", value: expected });
      expect(runs).toBe(1);

      // Replay over the SAME journal: the step value comes from the log; the fn is NOT re-run.
      const out2 = await executeRun(run, {
        workflow: wf,
        journal: j,
        loaded: beforeTerminal(await j.load(run)),
      });
      expect(out2).toEqual({ kind: "completed", value: expected });
      expect(runs).toBe(1); // UNCHANGED — replayed from the journal
    });
  }

  it("a child resolving with a FALSY value returns it (0/false/null/empty), not mistaken for pending", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "vfc");
    const child = defineWorkflow("child", (_c, n: number) => Promise.resolve(n));
    const wf = defineWorkflow("w", async (ctx) => {
      const vals = await Promise.all([
        ctx.startCall(child, 1), // seq 0
        ctx.startCall(child, 2), // seq 1
        ctx.startCall(child, 3), // seq 2
        ctx.startCall(child, 4), // seq 3
      ]);
      return vals.map((x) => `${typeof x}:${String(x)}`);
    });
    // Pass 1: children start, suspend (all pending).
    expect(
      await executeRun("vfc", {
        workflow: wf,
        journal: j,
        loaded: await j.load("vfc"),
        children: noopChildren,
      }),
    ).toEqual({ kind: "suspended" });
    // Resolve each child with a distinct FALSY value.
    await resolveOk(j, "vfc", 0, 0);
    await resolveOk(j, "vfc", 1, false);
    await resolveOk(j, "vfc", 2, null);
    await resolveOk(j, "vfc", 3, "");
    // Pass 2: each falsy value is a real resolution (not re-read as pending) — the run completes.
    expect(
      await executeRun("vfc", {
        workflow: wf,
        journal: j,
        loaded: await j.load("vfc"),
        children: noopChildren,
      }),
    ).toEqual({
      kind: "completed",
      value: ["number:0", "boolean:false", "object:null", "string:"],
    });
  });

  it("a child resolving with UNDEFINED (absent value) is a success, NOT a failure", async () => {
    // resolutionOf distinguishes value-vs-error by the presence of `error`, NOT by value truthiness —
    // so a child that returns undefined resolves successfully (undefined), it is not read as a failure.
    const j = new InMemoryJournal();
    await seedStart(j, "vcu");
    const child = defineWorkflow("child", () => Promise.resolve(undefined));
    const wf = defineWorkflow("w", async (ctx) => {
      const r = await ctx.call(child, undefined);
      return r === undefined ? "undefined-is-success" : `wrong:${String(r)}`;
    });
    await executeRun("vcu", {
      workflow: wf,
      journal: j,
      loaded: await j.load("vcu"),
      children: noopChildren,
    });
    // resolve with NEITHER value nor error (a child that returned undefined).
    await append(
      j,
      "vcu",
      stamp({ kind: "resolve", seq: NON_COMMAND_SEQ, resolvesSeq: 0 }, 0),
      msgId.resolve("vcu", 0),
    );
    expect(
      await executeRun("vcu", {
        workflow: wf,
        journal: j,
        loaded: await j.load("vcu"),
        children: noopChildren,
      }),
    ).toEqual({ kind: "completed", value: "undefined-is-success" });
  });

  it("a signal carrying FALSY data resolves the await with exactly that data", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "vsd");
    const wf = defineWorkflow("w", async (ctx) => {
      const got = await Promise.all([
        ctx.await<unknown>("a"),
        ctx.await<unknown>("b"),
        ctx.await<unknown>("c"),
        ctx.await<unknown>("d"),
        ctx.await<unknown>("e"),
      ]);
      return got.map((x) => `${typeof x}:${String(x)}`);
    });
    const sig = (name: string, data: unknown, id: string) =>
      append(
        j,
        "vsd",
        stamp({ kind: "signal", seq: NON_COMMAND_SEQ, name, data, signalId: id }, 0),
        msgId.signal("vsd", id),
      );
    await executeRun("vsd", { workflow: wf, journal: j, loaded: await j.load("vsd") });
    await sig("a", 0, "s-a");
    await sig("b", false, "s-b");
    await sig("c", null, "s-c");
    await sig("d", "", "s-d");
    await sig("e", undefined, "s-e"); // absent data → undefined
    expect(
      await executeRun("vsd", { workflow: wf, journal: j, loaded: await j.load("vsd") }),
    ).toEqual({
      kind: "completed",
      value: ["number:0", "boolean:false", "object:null", "string:", "undefined:undefined"],
    });
  });

  it("a NON-FINITE step result (NaN/Infinity) FAILS the run loudly — not silently coerced to null", async () => {
    // JSON cannot represent NaN/Infinity (they would stringify to null → a first-run vs replay
    // divergence), so the codec rejects them and the run fails terminally as a SerializationError,
    // symmetric with the BigInt guard. Nothing usable is journaled: no step-end lands.
    const j = new InMemoryJournal();
    await seedStart(j, "nan");
    const wf = defineWorkflow("w", (ctx) => ctx.run("s", () => NaN));
    const out = await executeRun("nan", { workflow: wf, journal: j, loaded: await j.load("nan") });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.name).toBe("SerializationError");
    expect((await j.load("nan")).some((l) => l.entry.kind === "step-end")).toBe(false);
  });
});

describe("rock-solid: misc pure pins", () => {
  it("ctx.get throws for an unregistered dependency (fails the run loudly)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "dep1");
    const wf = defineWorkflow("w", (ctx) => Promise.resolve(ctx.get<string>("missing")));
    const out = await executeRun("dep1", {
      workflow: wf,
      journal: j,
      loaded: await j.load("dep1"),
    });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") {
      expect(out.error.message).toMatch(/no dependency registered for "missing"/);
    }
  });

  it("continueAsNew with a non-serializable input FAILS the run (not an infra-retry loop)", async () => {
    // The continue-as-new input is journaled into the `continued` terminal; a BigInt can't encode, so
    // the run must fail loud (SerializationError) — exactly like an unencodable step result / return.
    const j = new InMemoryJournal();
    await seedStart(j, "can-bad");
    const wf = defineWorkflow("w", (ctx) => ctx.continueAsNew(BigInt(7)));
    const out = await executeRun("can-bad", {
      workflow: wf,
      journal: j,
      loaded: await j.load("can-bad"),
    });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.name).toBe("SerializationError");
  });
});

describe("versioning — immutability + migration re-drive (pure)", () => {
  it("a MUTATED pinned version (same version, changed body) fails loud on replay (NonDeterminismError)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "imm");
    // v1 records steps a, b, then suspends (no terminal yet).
    const v1 = defineWorkflow("w", async (ctx) => {
      await ctx.run("a", () => 1);
      await ctx.run("b", () => 2);
      await ctx.await("never");
      return "v1";
    });
    const first = await executeRun("imm", {
      workflow: v1,
      journal: j,
      loaded: await j.load("imm"),
    });
    expect(first.kind).toBe("suspended");
    // Editing v1's body WITHOUT bumping the version: step "b" → "b2". Replay diverges at command seq 1.
    // This is the immutability contract — to change behavior you add a version, never mutate one.
    const v1mutated = defineWorkflow("w", async (ctx) => {
      await ctx.run("a", () => 1);
      await ctx.run("b2", () => 2);
      await ctx.await("never");
      return "v1";
    });
    const out = await executeRun("imm", { workflow: v1mutated, journal: j, loaded: await j.load("imm") }); // prettier-ignore
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.name).toBe("NonDeterminismError");
  });

  it("re-driving a migrated generation recomputes the SAME next-gen version (idempotent)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "mr");
    const migrate = defineWorkflow("w", (ctx) => ctx.continueAsNew({}, { version: 2 }));
    const first = await executeRun("mr", {
      workflow: migrate,
      journal: j,
      loaded: await j.load("mr"),
    });
    expect(first.kind).toBe("continue");
    if (first.kind === "continue") expect(first.next.version).toBe(2);
    // A second incarnation over the SAME journal (already carries `continued`) yields the same version.
    const second = await executeRun("mr", { workflow: migrate, journal: j, loaded: beforeTerminal(await j.load("mr")) }); // prettier-ignore
    expect(second.kind).toBe("continue");
    if (second.kind === "continue") expect(second.next.version).toBe(2);
  });
});

describe("executor append-flush (fan-out intent lands before executeRun returns)", () => {
  it("does not return `suspended` while queued call appends are still in flight", async () => {
    // A journal whose appends land a few microtasks late — the shape of any real transport.
    // Without the executor's flush, a fan-out suspend returns (and the worker ACKS the item)
    // while sibling `call` appends are still queued: a crash right there loses recorded intent
    // with nothing left to redeliver it (the parent would wait forever on children that never
    // started). Found by the offload work's timing perturbation; pinned here forever.
    class SlowAppendJournal extends InMemoryJournal {
      override async append(runId: string, a: JournalAppend): Promise<AppendResult> {
        for (let i = 0; i < 5; i++) await Promise.resolve();
        return super.append(runId, a);
      }
    }
    const j = new SlowAppendJournal();
    await append(
      j,
      "flush",
      stamp({ kind: "start", seq: 0, wf: "w", wfVersion: 1, input: {} }, 0),
      msgId.start("flush"),
    );
    const child = defineWorkflow("child", (_c, n: number) => Promise.resolve(n));
    const wf = defineWorkflow("w", async (ctx) => {
      return Promise.all([
        ctx.startCall(child, 1),
        ctx.startCall(child, 2),
        ctx.startCall(child, 3),
        ctx.startCall(child, 4),
      ]);
    });
    const out = await executeRun("flush", {
      workflow: wf,
      journal: j,
      loaded: await j.load("flush"),
      children: { start: () => Promise.resolve() },
    });
    expect(out).toEqual({ kind: "suspended" });
    // The moment executeRun has returned, every fan-out call intent is durably journaled.
    const calls = (await j.load("flush")).filter((l) => l.entry.kind === "call");
    expect(calls).toHaveLength(4);
  });
});
