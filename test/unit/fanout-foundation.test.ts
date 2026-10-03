// Phase 0 (fan-out foundation): the internal append-queue lets multiple ctx.* ops be in flight at
// once (e.g. native Promise.all over ctx.run) without the concurrent CAS appends self-fencing on
// `lastSeq`. No new public API — this is the engine plumbing that fan-out (Phase 1) builds on.
//
// Before the append-queue, `Promise.all([ctx.run(a), ctx.run(b)])` self-fenced: both begins read the
// same `lastSeq`, so the second lost the CAS (FencedError) → the whole run halted `fenced`. These
// tests reproduce that scenario (InMemoryJournal enforces CAS exactly like NATS — see store.ts) and
// pin the fixed behavior: exactly-once journaling, correct replay, multi-begin resume.

import { describe, expect, it } from "vitest";
import { type ChildStart, type Children } from "../../src/core/children.js";
import { executeRun } from "../../src/core/execute.js";
import { type Scheduler } from "../../src/core/scheduler.js";
import { defineWorkflow } from "../../src/core/workflow.js";
import { FencedError } from "../../src/errors.js";
import { type JournalEntry, msgId, NON_COMMAND_SEQ, stamp } from "../../src/journal/entry.js";
import { InMemoryJournal, type Journal } from "../../src/journal/store.js";
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

/**
 * Runs N steps CONCURRENTLY via native Promise.all (each ctx.run is its own journaled step). Before
 * the append-queue the second step's begin self-fenced on the shared `lastSeq` CAS.
 */
function parallelWf(counter: Record<string, number>, names = ["a", "b", "c"]) {
  return defineWorkflow("w", async (ctx) => {
    const out = await Promise.all(
      names.map((nm) =>
        ctx.run(nm, () => {
          counter[nm] = (counter[nm] ?? 0) + 1;
          return nm.toUpperCase();
        }),
      ),
    );
    return out.join("");
  });
}

describe("fan-out foundation: concurrent appends (append-queue)", () => {
  it("Promise.all over ctx.run completes — concurrent begins do not self-fence", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "f1");
    const counter: Record<string, number> = {};
    const out = await executeRun("f1", {
      workflow: parallelWf(counter),
      journal: j,
      loaded: await j.load("f1"),
    });
    expect(out).toEqual({ kind: "completed", value: "ABC" });
    expect(counter).toEqual({ a: 1, b: 1, c: 1 }); // each step's fn ran exactly once
  });

  it("each concurrent step is journaled exactly once (one begin + one end per seq)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "f2");
    await executeRun("f2", { workflow: parallelWf({}), journal: j, loaded: await j.load("f2") });
    const loaded = await j.load("f2");
    for (let seq = 0; seq < 3; seq++) {
      const begins = loaded.filter((l) => l.entry.kind === "step-begin" && l.entry.seq === seq);
      const ends = loaded.filter((l) => l.entry.kind === "step-end" && l.entry.seq === seq);
      expect(begins).toHaveLength(1);
      expect(ends).toHaveLength(1);
    }
  });

  it("replays concurrent steps from the journal WITHOUT re-running them", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "f3");
    const counter: Record<string, number> = {};
    await executeRun("f3", {
      workflow: parallelWf(counter),
      journal: j,
      loaded: await j.load("f3"),
    });
    expect(counter).toEqual({ a: 1, b: 1, c: 1 });
    const out2 = await executeRun("f3", {
      workflow: parallelWf(counter),
      journal: j,
      loaded: beforeTerminal(await j.load("f3")),
    });
    expect(out2).toEqual({ kind: "completed", value: "ABC" });
    expect(counter).toEqual({ a: 1, b: 1, c: 1 }); // UNCHANGED on replay
  });

  it("resumes multiple steps that began but never ended (re-runs just those)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "f4");
    // Two concurrent begins landed, neither ended (crash mid-fan-out).
    await append(
      j,
      "f4",
      stamp({ kind: "step-begin", seq: 0, name: "a", key: "f4:0" }, 0),
      msgId.stepBegin("f4", 0),
    );
    await append(
      j,
      "f4",
      stamp({ kind: "step-begin", seq: 1, name: "b", key: "f4:1" }, 0),
      msgId.stepBegin("f4", 1),
    );
    const counter: Record<string, number> = {};
    const out = await executeRun("f4", {
      workflow: parallelWf(counter, ["a", "b"]),
      journal: j,
      loaded: await j.load("f4"),
    });
    expect(out).toEqual({ kind: "completed", value: "AB" });
    expect(counter).toEqual({ a: 1, b: 1 }); // both in-flight steps re-ran once (at-least-once)
  });

  it("ctx.key() is scoped per concurrent step, not a shared global", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "f5");
    const keys: Record<string, string> = {};
    const wf = defineWorkflow("w", async (ctx) => {
      await Promise.all(
        ["a", "b"].map((nm) =>
          ctx.run(nm, () => {
            keys[nm] = ctx.key(); // each step must see ITS OWN idempotency key
            return nm.toUpperCase();
          }),
        ),
      );
      return "ok";
    });
    await executeRun("f5", { workflow: wf, journal: j, loaded: await j.load("f5") });
    expect(keys).toEqual({ a: "f5:0", b: "f5:1" }); // a → seq 0, b → seq 1 (deterministic map order)
  });

  it("a ctx.* command nested inside a ctx.run fn still throws StepNestingError", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "f6");
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.run("outer", async () => {
        await ctx.now(); // illegal: a command issued inside a step fn
        return 1;
      });
      return "ok";
    });
    const out = await executeRun("f6", { workflow: wf, journal: j, loaded: await j.load("f6") });
    expect(out).toMatchObject({ kind: "failed", error: { name: "StepNestingError" } });
  });

  it("out-of-order step-end completion still replays correctly (lookup by seq, not position)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "f7");
    // A fan-out where seq 1 finished BEFORE seq 0: ends journaled out of seq order.
    await append(
      j,
      "f7",
      stamp({ kind: "step-begin", seq: 0, name: "a", key: "f7:0" }, 0),
      msgId.stepBegin("f7", 0),
    );
    await append(
      j,
      "f7",
      stamp({ kind: "step-begin", seq: 1, name: "b", key: "f7:1" }, 0),
      msgId.stepBegin("f7", 1),
    );
    await append(
      j,
      "f7",
      stamp({ kind: "step-end", seq: 1, name: "b", value: "B" }, 0),
      msgId.stepEnd("f7", 1),
    );
    await append(
      j,
      "f7",
      stamp({ kind: "step-end", seq: 0, name: "a", value: "A" }, 0),
      msgId.stepEnd("f7", 0),
    );
    const counter: Record<string, number> = {};
    const out = await executeRun("f7", {
      workflow: parallelWf(counter, ["a", "b"]),
      journal: j,
      loaded: await j.load("f7"),
    });
    expect(out).toEqual({ kind: "completed", value: "AB" });
    expect(counter).toEqual({}); // both replayed from journal — neither fn ran
  });

  it("a fenced append on one concurrent branch halts the whole run fenced", async () => {
    const inner = new InMemoryJournal();
    await seedStart(inner, "f8");
    // Fence the SECOND step's begin (a zombie that lost the CAS); the run must halt, not half-complete.
    const j: Journal = {
      load: (r) => inner.load(r),
      append: (r, a) =>
        a.msgID === msgId.stepBegin("f8", 1)
          ? Promise.reject(new FencedError(r, "injected: another worker won"))
          : inner.append(r, a),
    };
    const out = await executeRun("f8", {
      workflow: parallelWf({}, ["a", "b"]),
      journal: j,
      loaded: await j.load("f8"),
    });
    expect(out).toEqual({ kind: "fenced" });
  });

  it("a throwing concurrent branch fails the run cleanly", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "f9");
    const counter: Record<string, number> = {};
    const wf = defineWorkflow("w", async (ctx) => {
      await Promise.all([
        ctx.run("ok", () => {
          counter.ok = (counter.ok ?? 0) + 1;
          return 1;
        }),
        ctx.run("boom", () => {
          throw new Error("kaboom");
        }),
      ]);
      return "done";
    });
    const out = await executeRun("f9", { workflow: wf, journal: j, loaded: await j.load("f9") });
    expect(out).toMatchObject({ kind: "failed", error: { message: "kaboom" } });
  });

  it("Promise.all over mixed non-suspending ops (run / now / uuid) journals each at its own seq", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "f10");
    const wf = defineWorkflow("w", async (ctx) => {
      const [r, n, u] = await Promise.all([ctx.run("step", () => "S"), ctx.now(), ctx.uuid()]);
      return { r, n, uIsStr: typeof u === "string" };
    });
    const out = await executeRun("f10", {
      workflow: wf,
      journal: j,
      loaded: await j.load("f10"),
      clock: () => 12345,
    });
    expect(out).toMatchObject({ kind: "completed", value: { r: "S", n: 12345, uIsStr: true } });
    const loaded = await j.load("f10"); // run=seq 0 (begin+end), now=seq 1, uuid=seq 2
    expect(loaded.some((l) => l.entry.kind === "now" && l.entry.seq === 1)).toBe(true);
    expect(loaded.some((l) => l.entry.kind === "uuid" && l.entry.seq === 2)).toBe(true);
  });

  it("a ctx.run nested inside another ctx.run fn throws StepNestingError", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "f11");
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.run("outer", async () => {
        await ctx.run("inner", () => 1); // nested step — illegal
        return 2;
      });
      return "ok";
    });
    const out = await executeRun("f11", { workflow: wf, journal: j, loaded: await j.load("f11") });
    expect(out).toMatchObject({ kind: "failed", error: { name: "StepNestingError" } });
  });

  it("ctx.key() outside any step returns the run-scoped fallback (seq 0)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "f12");
    let topKey = "";
    const wf = defineWorkflow("w", async (ctx) => {
      topKey = ctx.key(); // top level, not inside a step
      await ctx.run("s", () => 1);
      return "ok";
    });
    await executeRun("f12", { workflow: wf, journal: j, loaded: await j.load("f12") });
    expect(topKey).toBe("f12:0");
  });

  it("ctx.key() is stable across a step retry (ALS context survives the retry await)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "f13");
    const keys: string[] = [];
    let attempts = 0;
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.run(
        "retried",
        () => {
          keys.push(ctx.key());
          attempts += 1;
          if (attempts < 2) throw new Error("transient");
          return "ok";
        },
        { retry: { kind: "fixed", attempts: 3, delayMs: 1 } },
      );
      return "done";
    });
    await executeRun("f13", { workflow: wf, journal: j, loaded: await j.load("f13") });
    expect(attempts).toBe(2);
    expect(keys).toEqual(["f13:0", "f13:0"]);
  });
});

describe("fan-out: durable futures (ctx.startCall)", () => {
  const childWf = defineWorkflow("child", (_ctx, n: number) => Promise.resolve(n));
  const recordingChildren = (started: ChildStart[]): Children => ({
    start: (spec) => {
      started.push(spec);
      return Promise.resolve();
    },
  });
  const recordingScheduler = (): Scheduler => ({ scheduleWake: () => Promise.resolve() });
  /** Starting a child takes long enough that an UNDRAINED arm would still be in flight at the ack. */
  const slowChildren = (started: ChildStart[], ms = 20): Children => ({
    start: async (spec) => {
      await new Promise((r) => setTimeout(r, ms));
      started.push(spec);
    },
  });

  it("Promise.all over two startCall children starts BOTH in one pass, then completes on resume", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "fc1");
    const wf = defineWorkflow("w", async (ctx) => {
      const [a, b] = await Promise.all([ctx.startCall(childWf, 1), ctx.startCall(childWf, 2)]);
      return a + b;
    });
    const started: ChildStart[] = [];

    // Pass 1: both children START concurrently (fan-out), run suspends with both pending.
    const out1 = await executeRun("fc1", {
      workflow: wf,
      journal: j,
      loaded: await j.load("fc1"),
      children: recordingChildren(started),
    });
    expect(out1).toEqual({ kind: "suspended" });
    expect(started.map((s) => s.parent.seq).sort()).toEqual([0, 1]); // BOTH started in one pass

    // both children finish — resolves journaled OUT OF ORDER (seq 1 before seq 0).
    await append(
      j,
      "fc1",
      stamp({ kind: "resolve", seq: NON_COMMAND_SEQ, resolvesSeq: 1, value: 2 }, 0),
      msgId.resolve("fc1", 1),
    );
    await append(
      j,
      "fc1",
      stamp({ kind: "resolve", seq: NON_COMMAND_SEQ, resolvesSeq: 0, value: 1 }, 0),
      msgId.resolve("fc1", 0),
    );

    // Pass 2 (replay): both resolved → Promise.all completes; children NOT re-started.
    const startedAgain: ChildStart[] = [];
    const out2 = await executeRun("fc1", {
      workflow: wf,
      journal: j,
      loaded: await j.load("fc1"),
      children: recordingChildren(startedAgain),
    });
    expect(out2).toEqual({ kind: "completed", value: 3 });
    expect(startedAgain).toHaveLength(0);
  });

  it("a failed child rejects the awaited future and fails the run", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "fc2");
    const wf = defineWorkflow("w", async (ctx) => {
      const [a, b] = await Promise.all([ctx.startCall(childWf, 1), ctx.startCall(childWf, 2)]);
      return a + b;
    });
    await executeRun("fc2", {
      workflow: wf,
      journal: j,
      loaded: await j.load("fc2"),
      children: recordingChildren([]),
    });
    await append(
      j,
      "fc2",
      stamp(
        {
          kind: "resolve",
          seq: NON_COMMAND_SEQ,
          resolvesSeq: 0,
          error: { name: "Error", message: "child boom" },
        },
        0,
      ),
      msgId.resolve("fc2", 0),
    );
    await append(
      j,
      "fc2",
      stamp({ kind: "resolve", seq: NON_COMMAND_SEQ, resolvesSeq: 1, value: 2 }, 0),
      msgId.resolve("fc2", 1),
    );
    const out = await executeRun("fc2", {
      workflow: wf,
      journal: j,
      loaded: await j.load("fc2"),
      children: recordingChildren([]),
    });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.message).toContain("child boom");
  });

  it("Promise.all over two startAwait futures resolves only once BOTH signals arrive", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "fa1");
    const wf = defineWorkflow("w", async (ctx) => {
      const [x, y] = await Promise.all([ctx.startAwait<number>("x"), ctx.startAwait<number>("y")]);
      return x + y;
    });
    const sig = (name: string, data: number, id: string) =>
      append(
        j,
        "fa1",
        stamp({ kind: "signal", seq: NON_COMMAND_SEQ, name, data, signalId: id }, 0),
        msgId.signal("fa1", id),
      );

    // Pass 1: both awaits journaled (seq 0 + 1), run suspends — no signals yet.
    expect(
      await executeRun("fa1", { workflow: wf, journal: j, loaded: await j.load("fa1") }),
    ).toEqual({ kind: "suspended" });
    // Only "y" arrives → still suspended on "x".
    await sig("y", 20, "s-y");
    expect(
      await executeRun("fa1", { workflow: wf, journal: j, loaded: await j.load("fa1") }),
    ).toEqual({ kind: "suspended" });
    // "x" arrives → Promise.all completes (resolved by name regardless of arrival order).
    await sig("x", 5, "s-x");
    expect(
      await executeRun("fa1", { workflow: wf, journal: j, loaded: await j.load("fa1") }),
    ).toEqual({ kind: "completed", value: 25 });
  });

  it("ctx.race resolves the journal-earliest branch, not the array-first one (deterministic)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "rc1");
    const wf = defineWorkflow("w", (ctx) =>
      ctx.race([ctx.startAwait<string>("x"), ctx.startAwait<string>("y")]),
    );
    expect(
      await executeRun("rc1", { workflow: wf, journal: j, loaded: await j.load("rc1") }),
    ).toEqual({ kind: "suspended" });
    // "y" (branch 1) lands BEFORE "x" (branch 0) → y wins despite x being first in the array.
    await append(
      j,
      "rc1",
      stamp({ kind: "signal", seq: NON_COMMAND_SEQ, name: "y", data: "Y", signalId: "sy" }, 0),
      msgId.signal("rc1", "sy"),
    );
    await append(
      j,
      "rc1",
      stamp({ kind: "signal", seq: NON_COMMAND_SEQ, name: "x", data: "X", signalId: "sx" }, 0),
      msgId.signal("rc1", "sx"),
    );
    expect(
      await executeRun("rc1", { workflow: wf, journal: j, loaded: await j.load("rc1") }),
    ).toEqual({ kind: "completed", value: "Y" });
  });

  it("ctx.race against a timer: the timer firing wins the race (timeout pattern)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "rc2");
    const wf = defineWorkflow("w", async (ctx) => {
      const r = await ctx.race([ctx.startAwait<string>("sig"), ctx.startSleep("t", 1000)]);
      return typeof r === "string" ? r : "timed-out";
    });
    const scheduler = recordingScheduler();
    expect(
      await executeRun("rc2", { workflow: wf, journal: j, loaded: await j.load("rc2"), scheduler }),
    ).toEqual({ kind: "suspended" });
    // the timer fires (sleep at seq 1 resolves); no signal ever arrives.
    await append(
      j,
      "rc2",
      stamp({ kind: "resolve", seq: NON_COMMAND_SEQ, resolvesSeq: 1 }, 0),
      msgId.resolve("rc2", 1),
    );
    expect(
      await executeRun("rc2", { workflow: wf, journal: j, loaded: await j.load("rc2"), scheduler }),
    ).toEqual({ kind: "completed", value: "timed-out" });
  });

  it("ctx.race suspends while all branches are pending; an empty race fails", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "rc3");
    const pending = defineWorkflow("w", (ctx) =>
      ctx.race([ctx.startAwait("a"), ctx.startAwait("b")]),
    );
    expect(
      await executeRun("rc3", { workflow: pending, journal: j, loaded: await j.load("rc3") }),
    ).toEqual({ kind: "suspended" });

    const j2 = new InMemoryJournal();
    await seedStart(j2, "rc4");
    const empty = defineWorkflow("w", (ctx) => ctx.race([]));
    const out = await executeRun("rc4", {
      workflow: empty,
      journal: j2,
      loaded: await j2.load("rc4"),
    });
    expect(out.kind).toBe("failed");
  });

  it("a 3-way fan-out SUSPENDS (never spuriously fences a sibling's command), then completes", async () => {
    // The fenced-fix as an OBSERVABLE invariant: pass 1 must resolve to `suspended`, NOT `fenced`.
    // startAwait arms fastest (no child-start / schedule await), so under the old halted-based
    // short-circuit a sibling branch's command-append could be turned into a fence here, aborting the
    // run. With the fix only a real lost-CAS fences, so the run parks cleanly and later completes.
    // (Asserting journal *contents* right after suspend would be racy — a sibling's append can still be
    // an in-flight orphan; the run completing on replay is the deterministic proof of no spurious fence.)
    const j = new InMemoryJournal();
    await seedStart(j, "ff1");
    const wf = defineWorkflow("w", async (ctx) => {
      const [a, b, c] = await Promise.all([
        ctx.startAwait<string>("a"),
        ctx.startAwait<string>("b"),
        ctx.startAwait<string>("c"),
      ]);
      return a + b + c;
    });
    expect(
      await executeRun("ff1", { workflow: wf, journal: j, loaded: await j.load("ff1") }),
    ).toEqual({ kind: "suspended" });
    for (const [n, id] of [
      ["a", "sa"],
      ["b", "sb"],
      ["c", "sc"],
    ] as const) {
      await append(
        j,
        "ff1",
        stamp(
          { kind: "signal", seq: NON_COMMAND_SEQ, name: n, data: n.toUpperCase(), signalId: id },
          0,
        ),
        msgId.signal("ff1", id),
      );
    }
    expect(
      await executeRun("ff1", { workflow: wf, journal: j, loaded: await j.load("ff1") }),
    ).toEqual({ kind: "completed", value: "ABC" });
  });

  it("ctx.race surfaces a failure when the journal-earliest branch FAILED", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "rcf");
    const wf = defineWorkflow("w", (ctx) =>
      ctx.race([ctx.startCall(childWf, 1), ctx.startCall(childWf, 2)]),
    );
    await executeRun("rcf", {
      workflow: wf,
      journal: j,
      loaded: await j.load("rcf"),
      children: recordingChildren([]),
    });
    // branch 0 FAILS first (lower stream-seq); branch 1 would have succeeded later.
    await append(
      j,
      "rcf",
      stamp(
        {
          kind: "resolve",
          seq: NON_COMMAND_SEQ,
          resolvesSeq: 0,
          error: { name: "Error", message: "first-and-failed" },
        },
        0,
      ),
      msgId.resolve("rcf", 0),
    );
    await append(
      j,
      "rcf",
      stamp({ kind: "resolve", seq: NON_COMMAND_SEQ, resolvesSeq: 1, value: 2 }, 0),
      msgId.resolve("rcf", 1),
    );
    const out = await executeRun("rcf", {
      workflow: wf,
      journal: j,
      loaded: await j.load("rcf"),
      children: recordingChildren([]),
    });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.message).toContain("first-and-failed");
  });

  it("a replay that drops a fan-out branch fails loud (assertFullyConsumed)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "fd1");
    // Seed a journal as if TWO children ran (commands seq 0 + 1, both resolved).
    for (const seq of [0, 1]) {
      await append(
        j,
        "fd1",
        stamp(
          { kind: "call", seq, name: "child", childRunId: `fd1_${seq}`, wf: "child", input: seq },
          0,
        ),
        msgId.command("fd1", seq),
      );
      await append(
        j,
        "fd1",
        stamp({ kind: "resolve", seq: NON_COMMAND_SEQ, resolvesSeq: seq, value: seq }, 0),
        msgId.resolve("fd1", seq),
      );
    }
    // Replay with a workflow that starts only ONE child — a shorter path than the journal recorded.
    const shrunk = defineWorkflow("w", async (ctx) => {
      await ctx.startCall(childWf, 0);
      return "done";
    });
    const out = await executeRun("fd1", {
      workflow: shrunk,
      journal: j,
      loaded: await j.load("fd1"),
      children: recordingChildren([]),
    });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.name).toBe("NonDeterminismError");
  });

  it("Promise.all over mixed future kinds (call + sleep + await) completes", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "mx1");
    const wf = defineWorkflow("w", async (ctx) => {
      const [c, s, a] = await Promise.all([
        ctx.startCall(childWf, 7),
        ctx.startSleep("t", 100),
        ctx.startAwait<string>("sig"),
      ]);
      return `${c}-${s ?? "slept"}-${a}`;
    });
    const deps = { children: recordingChildren([]), scheduler: recordingScheduler() };
    expect(
      await executeRun("mx1", { workflow: wf, journal: j, loaded: await j.load("mx1"), ...deps }),
    ).toEqual({ kind: "suspended" });
    await append(
      j,
      "mx1",
      stamp({ kind: "resolve", seq: NON_COMMAND_SEQ, resolvesSeq: 0, value: 7 }, 0),
      msgId.resolve("mx1", 0),
    );
    await append(
      j,
      "mx1",
      stamp({ kind: "resolve", seq: NON_COMMAND_SEQ, resolvesSeq: 1 }, 0),
      msgId.resolve("mx1", 1),
    );
    await append(
      j,
      "mx1",
      stamp({ kind: "signal", seq: NON_COMMAND_SEQ, name: "sig", data: "S", signalId: "s1" }, 0),
      msgId.signal("mx1", "s1"),
    );
    expect(
      await executeRun("mx1", { workflow: wf, journal: j, loaded: await j.load("mx1"), ...deps }),
    ).toEqual({ kind: "completed", value: "7-slept-S" });
  });

  it("an infra failure while arming a fan-out branch retries the run (not fail, not fence)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "rt1");
    const throwingChildren: Children = { start: () => Promise.reject(new Error("nats down")) };
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.startCall(childWf, 1);
      return "x";
    });
    const out = await executeRun("rt1", {
      workflow: wf,
      journal: j,
      loaded: await j.load("rt1"),
      children: throwingChildren,
    });
    expect(out.kind).toBe("retry");
  });

  it("a DISCARDED (never-awaited) future is misuse: result ignored, run not blocked — but fully armed", async () => {
    // Durable futures must be CONSUMED (await / Promise.all / ctx.race). A discarded startCall does NOT
    // block the run — it returns without the child's result, which is the sharp edge the DurableFuture
    // docs warn about. What is NOT left to chance is the arming: flushAppends drains the arm itself
    // (not just its command append), so the command is journaled and the child started BEFORE the
    // outcome is reported. The SLOW children seam is the control — a floating arm would still be
    // in flight here, and there is no await between executeRun resolving and the assertions.
    const j = new InMemoryJournal();
    await seedStart(j, "ff1");
    const started: ChildStart[] = [];
    const wf = defineWorkflow("w", (ctx) => {
      void ctx.startCall(childWf, 1); // discarded — never awaited
      return Promise.resolve("done");
    });
    const out = await executeRun("ff1", {
      workflow: wf,
      journal: j,
      loaded: await j.load("ff1"),
      children: slowChildren(started),
    });
    expect(out).toEqual({ kind: "completed", value: "done" }); // run does NOT block on the future
    expect(started).toHaveLength(1); // child WAS started — before the worker could ack
    expect((await j.load("ff1")).some((l) => l.entry.kind === "call")).toBe(true);
  });

  it("a SUSPEND waits for an in-flight branch's child start (the ack is final — LEARNINGS #48)", async () => {
    // One branch suspends the run while a sibling's child start is still in flight. The worker acks
    // the instant executeRun returns and an acked item never comes back, so a start lost in that
    // window would leave the parent parked forever on a child that was never enqueued.
    const j = new InMemoryJournal();
    await seedStart(j, "ff2");
    const started: ChildStart[] = [];
    const wf = defineWorkflow("w", async (ctx) => {
      const slow = ctx.startCall(childWf, 1); // seq 0 — its child start takes a while
      await ctx.startAwait("x"); // seq 1 — suspends the run first
      return slow;
    });
    const out = await executeRun("ff2", {
      workflow: wf,
      journal: j,
      loaded: await j.load("ff2"),
      children: slowChildren(started),
    });
    expect(out).toEqual({ kind: "suspended" });
    expect(started.map((s) => s.parent.seq)).toEqual([0]); // no await in between: that IS the assertion
  });

  it("a discarded future whose arm THROWS does not blow up the run (no unhandled rejection)", async () => {
    // A divergence at a recorded position rejects the arm, and nobody awaits it here. The handler
    // attached at issue time is what keeps that from being an unhandled rejection — which Node treats
    // as fatal. (flushAppends' allSettled attaches one too, but only once the run reaches an outcome,
    // which can be a turn or more later.) The run's own outcome is unaffected: a branch nobody
    // consumed is not part of it.
    const j = new InMemoryJournal();
    await seedStart(j, "ff3");
    // A `sleep` is recorded at seq 0 under a DIFFERENT name than the code now issues.
    await append(
      j,
      "ff3",
      stamp({ kind: "sleep", seq: 0, name: "recorded", dueMs: 1 }, 0),
      msgId.command("ff3", 0),
    );
    const wf = defineWorkflow("w", (ctx) => {
      void ctx.startSleep("renamed", 1000); // diverges → armSleep rejects, and nothing awaits it
      return Promise.resolve("done");
    });
    const out = await executeRun("ff3", { workflow: wf, journal: j, loaded: await j.load("ff3") });
    expect(out).toEqual({ kind: "completed", value: "done" });
  });
});

// Promise.allSettled is advertised in the public surface (types.ts) but was never exercised. Its
// behavior is the COMPLEMENT of Promise.all and turns on a single distinction:
//   - an IN-BAND rejection (a failed child / a timed-out await throws from settle()) is a real promise
//     rejection — allSettled CAPTURES it as {status:"rejected"} and the workflow keeps running;
//   - an OUT-OF-BAND halt (pending → suspend, lost CAS → fenced) returns the never-settling NEVER and
//     trips the executor's halt race — allSettled can't capture a non-settling promise, so the halt wins.
// These tests pin both halves: a captured failure does NOT fail the run, yet a pending sibling still
// suspends it even after a sibling failure was "caught".
describe("fan-out: Promise.allSettled — in-band failures captured, out-of-band halts dominate", () => {
  const childWf = defineWorkflow("child", (_ctx, n: number) => Promise.resolve(n));
  const noopChildren = (): Children => ({ start: () => Promise.resolve() });
  const noopScheduler = (): Scheduler => ({ scheduleWake: () => Promise.resolve() });
  const resolveOk = (j: Journal, run: string, seq: number, value: unknown) =>
    append(
      j,
      run,
      stamp({ kind: "resolve", seq: NON_COMMAND_SEQ, resolvesSeq: seq, value }, 0),
      msgId.resolve(run, seq),
    );
  const resolveErr = (j: Journal, run: string, seq: number, message: string) =>
    append(
      j,
      run,
      stamp(
        {
          kind: "resolve",
          seq: NON_COMMAND_SEQ,
          resolvesSeq: seq,
          error: { name: "Error", message },
        },
        0,
      ),
      msgId.resolve(run, seq),
    );

  it("CAPTURES a failed child (in-band) — the run COMPLETES with mixed statuses (contrast Promise.all)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "as1");
    const wf = defineWorkflow("w", async (ctx) => {
      const r = await Promise.allSettled([ctx.startCall(childWf, 1), ctx.startCall(childWf, 2)]);
      return r.map((x) => (x.status === "fulfilled" ? `ok:${x.value}` : "rejected"));
    });
    // Pass 1: both children start, run suspends (both pending).
    expect(
      await executeRun("as1", {
        workflow: wf,
        journal: j,
        loaded: await j.load("as1"),
        children: noopChildren(),
      }),
    ).toEqual({ kind: "suspended" });
    // child 0 FAILS, child 1 succeeds.
    await resolveErr(j, "as1", 0, "boom");
    await resolveOk(j, "as1", 1, 2);
    // Pass 2: allSettled CAPTURES the rejection → the run COMPLETES (it does NOT fail, unlike Promise.all).
    expect(
      await executeRun("as1", {
        workflow: wf,
        journal: j,
        loaded: await j.load("as1"),
        children: noopChildren(),
      }),
    ).toEqual({ kind: "completed", value: ["rejected", "ok:2"] });
  });

  it("a PENDING sibling still SUSPENDS allSettled — an out-of-band halt dominates a captured failure", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "as2");
    const wf = defineWorkflow("w", async (ctx) => {
      const r = await Promise.allSettled([ctx.startCall(childWf, 1), ctx.startCall(childWf, 2)]);
      return r.map((x) => x.status);
    });
    await executeRun("as2", {
      workflow: wf,
      journal: j,
      loaded: await j.load("as2"),
      children: noopChildren(),
    });
    // child 0 FAILS (in-band, captured by allSettled); child 1 stays PENDING.
    await resolveErr(j, "as2", 0, "boom");
    // The pending branch returns NEVER + fires haltSuspend out of band → the whole run SUSPENDS, even
    // though allSettled already "caught" child 0's failure. (Proves the halt is not swallowable.)
    expect(
      await executeRun("as2", {
        workflow: wf,
        journal: j,
        loaded: await j.load("as2"),
        children: noopChildren(),
      }),
    ).toEqual({ kind: "suspended" });
  });

  it("CAPTURES a timed-out await (a different in-band rejection) alongside a fulfilled signal", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "as3");
    const wf = defineWorkflow("w", async (ctx) => {
      const r = await Promise.allSettled([
        ctx.startAwait<number>("a", { timeoutMs: 1000 }), // seq 0 — will time out
        ctx.startAwait<number>("b"), // seq 1 — will be signalled
      ]);
      return r.map((x) => (x.status === "fulfilled" ? `ok:${x.value}` : "timed-out"));
    });
    expect(
      await executeRun("as3", {
        workflow: wf,
        journal: j,
        loaded: await j.load("as3"),
        scheduler: noopScheduler(),
      }),
    ).toEqual({ kind: "suspended" });
    // The timer for await "a" fires (a resolve on seq 0 with no signal named "a" → timeout); "b" is signalled.
    await append(
      j,
      "as3",
      stamp({ kind: "resolve", seq: NON_COMMAND_SEQ, resolvesSeq: 0 }, 0),
      msgId.resolve("as3", 0),
    );
    await append(
      j,
      "as3",
      stamp({ kind: "signal", seq: NON_COMMAND_SEQ, name: "b", data: 9, signalId: "sb" }, 0),
      msgId.signal("as3", "sb"),
    );
    // DurableTimeoutError throws in-band from settle() → captured as rejected; "b" fulfils with 9.
    expect(
      await executeRun("as3", {
        workflow: wf,
        journal: j,
        loaded: await j.load("as3"),
        scheduler: noopScheduler(),
      }),
    ).toEqual({ kind: "completed", value: ["timed-out", "ok:9"] });
  });
});

// Degenerate combinator shapes that are legal but unusual — easy to break in a refactor.
describe("fan-out: degenerate combinators (single future, double-await)", () => {
  const sig = (j: Journal, run: string, name: string, data: unknown, id: string) =>
    append(
      j,
      run,
      stamp({ kind: "signal", seq: NON_COMMAND_SEQ, name, data, signalId: id }, 0),
      msgId.signal(run, id),
    );

  it("ctx.race over a SINGLE future suspends while pending, then resolves to that future", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "dg1");
    const wf = defineWorkflow("w", (ctx) => ctx.race([ctx.startAwait<string>("only")]));
    expect(
      await executeRun("dg1", { workflow: wf, journal: j, loaded: await j.load("dg1") }),
    ).toEqual({ kind: "suspended" });
    await sig(j, "dg1", "only", "V", "s1");
    expect(
      await executeRun("dg1", { workflow: wf, journal: j, loaded: await j.load("dg1") }),
    ).toEqual({ kind: "completed", value: "V" });
  });

  it("awaiting the SAME future twice reads the same journaled value (no double-consume)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "dg2");
    const wf = defineWorkflow("w", async (ctx) => {
      const f = ctx.startAwait<number>("x");
      const a = await f;
      const b = await f; // same future → same command seq → same resolution (resolveAwait is by seq)
      return a + b;
    });
    expect(
      await executeRun("dg2", { workflow: wf, journal: j, loaded: await j.load("dg2") }),
    ).toEqual({ kind: "suspended" });
    await sig(j, "dg2", "x", 5, "sx"); // exactly ONE signal...
    // ...read twice → 5 + 5 (the lone signal isn't consumed twice, nor is a 2nd await left pending).
    expect(
      await executeRun("dg2", { workflow: wf, journal: j, loaded: await j.load("dg2") }),
    ).toEqual({ kind: "completed", value: 10 });
  });
});
