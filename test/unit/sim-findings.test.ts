// What the deterministic simulation (test/unit/simulate.test.ts) FOUND, pinned as direct tests.
//
// A shrunk simulator case is a fine bug report and a brittle regression test: its schedule is a
// function of how many gates the engine passes through, so an unrelated change moves the race
// somewhere else and the case passes for the wrong reason. Each finding is therefore rebuilt here
// with the interleaving FORCED — a journal that loses the CAS at a chosen append, another
// incarnation's entries written at a chosen moment — and no scheduler at all.

import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import v8 from "node:v8";
import vm from "node:vm";
import { describe, expect, it } from "vitest";
import { executeRun } from "../../src/core/execute.js";
import { defineWorkflow } from "../../src/core/workflow.js";
import { type JournalEntry, msgId, NON_COMMAND_SEQ, stamp } from "../../src/journal/entry.js";
import {
  type AppendResult,
  InMemoryJournal,
  type Journal,
  type JournalAppend,
} from "../../src/journal/store.js";

async function append(j: Journal, runId: string, entry: JournalEntry, mid: string): Promise<void> {
  const last = (await j.load(runId)).at(-1)?.streamSeq ?? 0;
  await j.append(runId, { entry, msgID: mid, expectLastSeq: last });
}

const seedStart = (j: Journal, runId: string, input: unknown = {}): Promise<void> =>
  append(
    j,
    runId,
    stamp({ kind: "start", seq: 0, wf: "w", wfVersion: 1, input }, 0),
    msgId.start(runId),
  );

const kinds = async (j: Journal, runId: string): Promise<string[]> =>
  (await j.load(runId)).map((l) => l.entry.kind);

const terminals = async (j: Journal, runId: string): Promise<JournalEntry[]> =>
  (await j.load(runId))
    .map((l) => l.entry)
    .filter((e) => ["result", "fail", "continued", "cancelled"].includes(e.kind));

/**
 * A journal whose duplicate window has EXPIRED: every msgID is unique, so nothing but the CAS
 * decides what lands. "One terminal per run" must hold here too — dedup is a convenience.
 */
function withoutDedup(inner: Journal): Journal {
  let n = 0;
  return {
    load: (runId) => inner.load(runId),
    append: (runId, a) => inner.append(runId, { ...a, msgID: `${a.msgID}~${++n}` }),
  };
}

/** Run `before` once, just ahead of the first append matching `when`. */
function intercept(
  inner: Journal,
  when: (a: JournalAppend) => boolean,
  before: () => Promise<void>,
): Journal {
  let done = false;
  return {
    load: (runId) => inner.load(runId),
    append: async (runId, a): Promise<AppendResult> => {
      if (!done && when(a)) {
        done = true;
        await before();
      }
      return inner.append(runId, a);
    },
  };
}

describe("a journal that holds a terminal is never executed", () => {
  // The worker checks for a terminal on its FIRST load; what it executes is a later one (after
  // recording the inbound event, after the lease), and another worker can finish the run between.

  it("a cancel journaled AFTER `continued`: the rollover stands — no cleanup, no second terminal", async () => {
    const j = withoutDedup(new InMemoryJournal());
    await seedStart(j, "t1");
    await append(j, "t1", stamp({ kind: "continued", seq: NON_COMMAND_SEQ, nextRunId: "t1_g1", input: { n: 7 }, generation: 1, base: "t1", version: 1 }, 0), msgId.terminal("t1")); // prettier-ignore
    await append(j, "t1", stamp({ kind: "cancel-requested", seq: NON_COMMAND_SEQ }, 0), msgId.cancel("t1")); // prettier-ignore
    let invoked = 0;
    let compensated = 0;
    const wf = defineWorkflow("w", (ctx) => {
      invoked += 1;
      ctx.onCancel(() => ctx.run("undo", () => (compensated += 1)));
      return ctx.continueAsNew({ n: 8 }); // a different input than the journal's, on purpose
    });
    const out = await executeRun("t1", { workflow: wf, journal: j, loaded: await j.load("t1") });
    // Before the fix: `cancelled` (ctx.continueAsNew cancels in place under a journaled cancel),
    // the compensation fired, and a second terminal landed after the first.
    expect(out).toEqual({
      kind: "continue",
      next: { nextRunId: "t1_g1", input: { n: 7 }, generation: 1, base: "t1", version: 1 },
    });
    expect(invoked).toBe(0);
    expect(compensated).toBe(0);
    expect(await kinds(j, "t1")).toEqual(["start", "continued", "cancel-requested"]);
  });

  it("the step-end reconcile adopts a journal another incarnation FINISHED: one terminal, its value", async () => {
    const inner = new InMemoryJournal();
    await seedStart(inner, "t2");
    await append(inner, "t2", stamp({ kind: "step-begin", seq: 0, name: "a", key: "t2:0" }, 0), msgId.stepBegin("t2", 0)); // prettier-ignore
    // We resume step "a". Just before our step-end lands, another incarnation completes the step
    // AND the run. Our step-end fences, the reconcile adopts — and the adopted tail is a terminal.
    const j = intercept(
      withoutDedup(inner),
      (a) => a.entry.kind === "step-end",
      async () => {
        await append(inner, "t2", stamp({ kind: "step-end", seq: 0, name: "a", value: "theirs" }, 0), "other:e"); // prettier-ignore
        await append(inner, "t2", stamp({ kind: "result", seq: NON_COMMAND_SEQ, value: "their result" }, 0), "other:t"); // prettier-ignore
      },
    );
    const wf = defineWorkflow(
      "w",
      async (ctx) => `mine, after ${await ctx.run("a", () => "mine")}`,
    );
    const out = await executeRun("t2", { workflow: wf, journal: j, loaded: await j.load("t2") });
    // Before the fix: our terminal append passed its CAS (the adopted tail IS their terminal), so
    // with the window expired a SECOND `result` landed; inside the window the duplicate ack read
    // as "mine landed" and we reported our own value over the journal's.
    expect(out).toEqual({ kind: "completed", value: "their result" });
    expect(await terminals(inner, "t2")).toHaveLength(1);
  });
});

describe("a halt raised inside onCancel cleanup ends the execution (it used to hang it)", () => {
  // Cleanup runs AFTER the `cancelled` halt, so a second halt could not reach the executor; the
  // cleanup step handed its never-settling promise to runCleanup, which awaited it forever.
  const cancelledRun = async (runId: string): Promise<InMemoryJournal> => {
    const j = new InMemoryJournal();
    await seedStart(j, runId);
    await append(j, runId, stamp({ kind: "cancel-requested", seq: NON_COMMAND_SEQ }, 0), msgId.cancel(runId)); // prettier-ignore
    return j;
  };
  const wf = defineWorkflow("w", async (ctx) => {
    ctx.onCancel(() => ctx.run("undo", () => "undone"));
    await ctx.run("work", () => "never runs — the cancel is already journaled");
    return "done";
  });

  it("an infrastructure error on a cleanup step's append → retry (one transient publish failure)", async () => {
    const inner = await cancelledRun("h1");
    let failed = false;
    const j: Journal = {
      load: (runId) => inner.load(runId),
      append: (runId, a) => {
        if (!failed && a.entry.kind === "step-begin") {
          failed = true;
          return Promise.reject(new Error("publish failed"));
        }
        return inner.append(runId, a);
      },
    };
    const out = await executeRun("h1", { workflow: wf, journal: j, loaded: await j.load("h1") });
    expect(out.kind).toBe("retry");
    expect(await terminals(inner, "h1")).toHaveLength(0); // no terminal: the redelivery finishes it
    // …and the redelivery does: the cleanup step runs, then the terminal.
    const again = await executeRun("h1", { workflow: wf, journal: inner, loaded: await inner.load("h1") }); // prettier-ignore
    expect(again).toEqual({ kind: "cancelled" });
    expect(await kinds(inner, "h1")).toEqual(["start", "cancel-requested", "step-begin", "step-end", "cancelled"]); // prettier-ignore
  });

  it("a lost CAS on a cleanup step's append → fenced", async () => {
    const inner = await cancelledRun("h2");
    const j = intercept(
      inner,
      (a) => a.entry.kind === "step-begin",
      () => append(inner, "h2", stamp({ kind: "signal", seq: NON_COMMAND_SEQ, name: "s", data: 1, signalId: "x" }, 0), "h2:sig:x"), // prettier-ignore
    );
    const out = await executeRun("h2", { workflow: wf, journal: j, loaded: await j.load("h2") });
    expect(out).toEqual({ kind: "fenced" });
  });

  it("an unencodable cleanup step result ends only ITS handler — the next one is still waited for", async () => {
    // Two handlers; the first one's step result cannot be encoded (a `fail` abort ends only that
    // handler). The second must still run to completion before the terminal is written.
    const inner = await cancelledRun("h3");
    const order: string[] = [];
    const two = defineWorkflow("w", async (ctx) => {
      ctx.onCancel(() => ctx.run("second", () => void order.push("second")));
      ctx.onCancel(() => ctx.run("first", () => (order.push("first"), 10n))); // a BigInt: unencodable
      await ctx.run("work", () => "never runs");
    });
    const out = await executeRun("h3", { workflow: two, journal: inner, loaded: await inner.load("h3") }); // prettier-ignore
    expect(out).toEqual({ kind: "cancelled" });
    expect(order).toEqual(["first", "second"]);
    const ks = await kinds(inner, "h3");
    expect(ks.filter((k) => k === "step-end")).toHaveLength(1); // only "second" has an end
    expect(ks.at(-1)).toBe("cancelled");
  });
});

describe("a halted execution is garbage once executeRun returns", () => {
  // Found because the simulator itself ran out of memory: 30,000 cases, 4 GB. Every halted ctx.*
  // call returned ONE module-level never-settling promise, and `await p` parks the awaiting frame
  // on `p` — so each suspension's continuation (the workflow's locals, the ctx, the loaded journal)
  // stayed reachable for the life of the process. A worker parks runs all day.
  it("the workflow's local state is collectable after a suspend", async () => {
    v8.setFlagsFromString("--expose-gc");
    const gc = vm.runInNewContext("gc") as () => void;
    const j = new InMemoryJournal();
    await seedStart(j, "m1");
    const states: WeakRef<Uint8Array>[] = [];
    const wf = defineWorkflow("w", async (ctx) => {
      const state = new Uint8Array(64 * 1024);
      states.push(new WeakRef(state));
      await ctx.await("a signal that never comes");
      return state.length;
    });
    for (let i = 0; i < 200; i++) {
      const out = await executeRun("m1", { workflow: wf, journal: j, loaded: await j.load("m1") });
      expect(out).toEqual({ kind: "suspended" });
    }
    await new Promise((r) => setTimeout(r, 0));
    gc();
    const alive = states.filter((w) => w.deref() !== undefined).length;
    // With the shared promise: all 200. A few may be held by frames still on their way out.
    expect(alive).toBeLessThan(10);
  });
});

describe("one step-scope store for the whole process, never one per execution", () => {
  // Found when this suite ran 2 s on Node 24 and 100 s on Node 22. Below Node 24, AsyncLocalStorage
  // rides async_hooks: every instance that has ever `run()` is visited on EVERY promise the process
  // creates, until `disable()`. One per CtxImpl made a worker slower with each run it had ever
  // driven, and the application's own promises with it. Timing it would be flaky and, on Node 24+,
  // blind — so the shape is pinned instead: one construction, at module level.
  it("src/ constructs exactly one AsyncLocalStorage, and not inside a class", () => {
    const root = fileURLToPath(new URL("../../src", import.meta.url));
    const hits: string[] = [];
    for (const file of readdirSync(root, { recursive: true, encoding: "utf8" })) {
      if (!file.endsWith(".ts")) continue;
      for (const line of readFileSync(join(root, file), "utf8").split("\n")) {
        if (/new AsyncLocalStorage\b/.test(line)) hits.push(`${file}: ${line.trim()}`);
      }
    }
    expect(hits).toHaveLength(1);
    expect(hits[0]).toMatch(/^core\/ctx\.ts: const STEP_SCOPE = new AsyncLocalStorage/);
  });
});

describe("OPEN findings — each pinned by the behaviour it SHOULD have (`it.fails` until it does)", () => {
  // `it.fails` passes while the assertion fails and turns the suite RED the day the engine is
  // fixed, which is the reminder to delete the matching tolerance in test/harness/sim/world.ts.

  it.fails(
    "step-failure-not-journaled: a step that threw, and was caught, is not run again on replay",
    async () => {
      const j = new InMemoryJournal();
      await seedStart(j, "o1");
      let charges = 0;
      const wf = defineWorkflow("w", async (ctx) => {
        let how = "charged";
        try {
          await ctx.run("charge", () => {
            charges += 1;
            if (charges === 1) throw new Error("card network timeout");
            return "CHG-1";
          });
        } catch {
          await ctx.run("apologise", () => "mailed");
          how = "apologised";
        }
        await ctx.sleep("cool-off", 10);
        return how;
      });
      const scheduler = { scheduleWake: () => Promise.resolve() };
      const first = await executeRun("o1", { workflow: wf, journal: j, loaded: await j.load("o1"), scheduler }); // prettier-ignore
      expect(first).toEqual({ kind: "suspended" });
      await append(j, "o1", stamp({ kind: "resolve", seq: NON_COMMAND_SEQ, resolvesSeq: 2 }, 0), msgId.resolve("o1", 2)); // prettier-ignore
      const second = await executeRun("o1", { workflow: wf, journal: j, loaded: await j.load("o1"), scheduler }); // prettier-ignore
      // Today: the replay runs "charge" AGAIN (nothing recorded that it failed); this time it
      // succeeds, the workflow takes the other branch, and the run dies of NonDeterminismError —
      // with the card charged and the customer already apologised to.
      expect(charges).toBe(1);
      expect(second).toEqual({ kind: "completed", value: "apologised" });
    },
  );

  it.fails(
    "premature-suspend: the run is not reported suspended while its code is still running",
    async () => {
      const j = new InMemoryJournal();
      await seedStart(j, "o2");
      const child = defineWorkflow("child", () => Promise.resolve("unused"));
      let refunds = 0;
      const wf = defineWorkflow("w", async (ctx) => {
        const c = ctx.startCall(child, {});
        const approval = ctx.startAwait("approval"); // never sent
        try {
          await Promise.all([c, approval]);
          return "approved";
        } catch {
          await ctx.run("refund", async () => {
            refunds += 1;
            await new Promise((r) => setTimeout(r, 15));
            return "refunded";
          });
          return "the child failed — gave up";
        }
      });
      const children = { start: () => Promise.resolve() };
      await executeRun("o2", { workflow: wf, journal: j, loaded: await j.load("o2"), children });
      await append(j, "o2", stamp({ kind: "resolve", seq: NON_COMMAND_SEQ, resolvesSeq: 0, error: { name: "Error", message: "boom" } }, 0), msgId.resolve("o2", 0)); // prettier-ignore
      const out = await executeRun("o2", { workflow: wf, journal: j, loaded: await j.load("o2"), children }); // prettier-ignore
      // Today: `suspended` — the pending await's halt reaches the executor before the rejection's
      // catch block has finished. The worker acks and releases the lease while "refund" is still in
      // flight (its step-end lands afterwards, from nobody's execution), and the workflow's return
      // value is never recorded: the run waits forever for an approval its code stopped wanting.
      expect(refunds).toBe(1);
      expect(out).toEqual({ kind: "completed", value: "the child failed — gave up" });
    },
  );
});
