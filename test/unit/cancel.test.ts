// Cancellation core (pure engine): a `cancel-requested` journal entry is an uncatchable hard-stop at
// the next COMMAND boundary — recorded commands replay (and an op resolved BEFORE the cancel delivers
// its value), but the first fresh command (or the resume of a begun-but-unfinished step) halts without
// executing: no new effects, no new intent, no new children once cancellation is journaled. A replay
// that reaches `return` with no stop point completes (completion wins). The cancel-vs-completion race
// is resolved deterministically by stream-seq. No client/worker here: the `cancel-requested` entry is
// seeded directly (what the worker does on client.cancel), so the engine logic is provable in-memory.

import { describe, expect, it } from "vitest";
import { type Children } from "../../src/core/children.js";
import { executeRun } from "../../src/core/execute.js";
import { defineWorkflow } from "../../src/core/workflow.js";
import { type JournalEntry, msgId, NON_COMMAND_SEQ, stamp } from "../../src/journal/entry.js";
import { InMemoryJournal, type Journal } from "../../src/journal/store.js";
import { type AnyWorkflow } from "../../src/types.js";
import { beforeTerminal } from "../harness/replayable.js";

async function append(j: Journal, runId: string, entry: JournalEntry, mid: string): Promise<void> {
  const loaded = await j.load(runId);
  await j.append(runId, { entry, msgID: mid, expectLastSeq: loaded.at(-1)?.streamSeq ?? 0 });
}
const seedStart = (j: Journal, runId: string): Promise<void> =>
  append(
    j,
    runId,
    stamp({ kind: "start", seq: 0, wf: "w", wfVersion: 1, input: {} }, 0),
    msgId.start(runId),
  );
const requestCancel = (j: Journal, runId: string, reason?: string): Promise<void> =>
  append(
    j,
    runId,
    stamp(
      reason !== undefined
        ? { kind: "cancel-requested", seq: NON_COMMAND_SEQ, reason }
        : { kind: "cancel-requested", seq: NON_COMMAND_SEQ },
      0,
    ),
    msgId.cancel(runId),
  );
const signal = (
  j: Journal,
  runId: string,
  name: string,
  data: unknown,
  id: string,
): Promise<void> =>
  append(
    j,
    runId,
    stamp({ kind: "signal", seq: NON_COMMAND_SEQ, name, data, signalId: id }, 0),
    msgId.signal(runId, id),
  );
const drive = (j: Journal, runId: string, wf: AnyWorkflow): Promise<{ kind: string }> =>
  j.load(runId).then((loaded) => executeRun(runId, { workflow: wf, journal: j, loaded }));

describe("cancellation core (pure engine)", () => {
  it("a run suspended on an await is cancelled when cancel-requested is present", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "c1");
    const wf = defineWorkflow("w", (ctx) => ctx.await<string>("go"));
    expect((await drive(j, "c1", wf)).kind).toBe("suspended");
    await requestCancel(j, "c1", "user cancelled");
    expect(await drive(j, "c1", wf)).toEqual({ kind: "cancelled" });
    const term = (await j.load("c1")).find((l) => l.entry.kind === "cancelled");
    expect(term?.entry).toMatchObject({ kind: "cancelled", reason: "user cancelled" });
  });

  it("cancellation lands at the next COMMAND boundary — a fresh step never executes", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "c2");
    const counter = { a: 0 };
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.run("a", () => {
        counter.a++;
        return "A";
      });
      await ctx.await("go");
      return "done";
    });
    await requestCancel(j, "c2");
    expect(await drive(j, "c2", wf)).toEqual({ kind: "cancelled" });
    expect(counter.a).toBe(0); // once cancel is journaled, no fresh side effect fires
    expect((await j.load("c2")).some((l) => l.entry.kind === "step-begin")).toBe(false); // no new intent
  });

  it("recorded steps still REPLAY under a pending cancel; the hard-stop is the first fresh command", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "c2b");
    const counter = { a: 0, b: 0 };
    const wf = defineWorkflow("w", async (ctx) => {
      const a = await ctx.run("a", () => {
        counter.a++;
        return "A";
      });
      await ctx.run("b", () => {
        counter.b++;
        return a + "B";
      });
      await ctx.await("go");
      return "done";
    });
    // First incarnation (no cancel yet) records step "a", then is abandoned mid-run: seed its entries.
    await append(
      j,
      "c2b",
      stamp({ kind: "step-begin", seq: 0, name: "a", key: "c2b:0" }, 0),
      msgId.stepBegin("c2b", 0),
    );
    await append(
      j,
      "c2b",
      stamp({ kind: "step-end", seq: 0, name: "a", value: "A" }, 0),
      msgId.stepEnd("c2b", 0),
    );
    await requestCancel(j, "c2b");
    expect(await drive(j, "c2b", wf)).toEqual({ kind: "cancelled" });
    expect(counter.a).toBe(0); // recorded → replayed, not re-executed
    expect(counter.b).toBe(0); // fresh → hard-stopped before executing
  });

  it("a resolve that landed BEFORE the cancel still counts (run completes; cancel ignored)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "c3");
    const wf = defineWorkflow("w", (ctx) => ctx.await<string>("go"));
    expect((await drive(j, "c3", wf)).kind).toBe("suspended");
    await signal(j, "c3", "go", "X", "s1"); // resolve lands BEFORE the cancel
    await requestCancel(j, "c3");
    expect(await drive(j, "c3", wf)).toEqual({ kind: "completed", value: "X" });
  });

  it("a resolve that landed AFTER the cancel is frozen out (run cancels at that op)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "c4");
    const wf = defineWorkflow("w", (ctx) => ctx.await<string>("go"));
    expect((await drive(j, "c4", wf)).kind).toBe("suspended");
    await requestCancel(j, "c4"); // cancel lands BEFORE the signal
    await signal(j, "c4", "go", "X", "s1"); // arrives after the cancel → frozen out
    expect(await drive(j, "c4", wf)).toEqual({ kind: "cancelled" });
  });

  it("a step-only run cancels at its first fresh step once cancel is journaled", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "c5");
    const counter = { a: 0 };
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.run("a", () => {
        counter.a++;
        return "A";
      });
      return "done";
    });
    await requestCancel(j, "c5");
    expect(await drive(j, "c5", wf)).toEqual({ kind: "cancelled" }); // the kill switch kills hot loops too
    expect(counter.a).toBe(0);
  });

  it("COMPLETION WINS: a replay that reaches return with no fresh command completes despite the cancel", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "c5b");
    // Everything the workflow needs is already recorded (step "a" completed pre-cancel).
    await append(
      j,
      "c5b",
      stamp({ kind: "step-begin", seq: 0, name: "a", key: "c5b:0" }, 0),
      msgId.stepBegin("c5b", 0),
    );
    await append(
      j,
      "c5b",
      stamp({ kind: "step-end", seq: 0, name: "a", value: "A" }, 0),
      msgId.stepEnd("c5b", 0),
    );
    await requestCancel(j, "c5b");
    const counter = { a: 0 };
    const wf = defineWorkflow("w", async (ctx) => {
      const a = await ctx.run("a", () => {
        counter.a++;
        return "A";
      });
      return a + "-done"; // pure code after the last recorded command — no stop point
    });
    expect(await drive(j, "c5b", wf)).toEqual({ kind: "completed", value: "A-done" });
    expect(counter.a).toBe(0); // replayed, not re-run
  });

  it("cancellation hard-stops a run parked on a fan-out (Promise.all over pending ops)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "c6");
    const childWf = defineWorkflow("child", (_c, n: number) => Promise.resolve(n));
    const children: Children = { start: () => Promise.resolve() };
    const wf = defineWorkflow("w", async (ctx) => {
      await Promise.all([ctx.startCall(childWf, 1), ctx.startCall(childWf, 2)]);
      return "done";
    });
    const o1 = await executeRun("c6", {
      workflow: wf,
      journal: j,
      loaded: await j.load("c6"),
      children,
    });
    expect(o1.kind).toBe("suspended"); // both children started, run parked on Promise.all
    await requestCancel(j, "c6");
    const out = await executeRun("c6", {
      workflow: wf,
      journal: j,
      loaded: await j.load("c6"),
      children,
    });
    expect(out).toEqual({ kind: "cancelled" }); // cancel hard-stops at the fan-out frontier
  });

  it("the RESUME of a begun-but-unfinished step does NOT re-fire under a pending cancel", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "c7");
    // A step began but never ended (crash mid-step), AND a cancel is now pending. Re-running fn
    // would fire the side effect for a run being killed — the replay hard-stops at the step instead
    // (whether the effect already fired once is the same uncertainty as any crash; onCancel
    // compensation + ctx.key() is the tool for that).
    await append(
      j,
      "c7",
      stamp({ kind: "step-begin", seq: 0, name: "s", key: "c7:0" }, 0),
      msgId.stepBegin("c7", 0),
    );
    await requestCancel(j, "c7");
    const counter = { s: 0 };
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.run("s", () => {
        counter.s++;
        return "S";
      });
      await ctx.await("go");
      return "done";
    });
    expect(await drive(j, "c7", wf)).toEqual({ kind: "cancelled" });
    expect(counter.s).toBe(0); // NOT re-fired
    expect((await j.load("c7")).some((l) => l.entry.kind === "step-end" && l.entry.seq === 0)).toBe(
      false, // the step stays begun-but-unfinished; the cancelled terminal ends the run
    );
  });

  it("ctx.onCancel runs a compensation step before the cancelled terminal", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "h1");
    const log: string[] = [];
    const wf = defineWorkflow("w", async (ctx) => {
      ctx.onCancel(() =>
        ctx.run("refund", () => {
          log.push("refund");
          return 1;
        }),
      );
      await ctx.await("go");
      return "done";
    });
    expect((await drive(j, "h1", wf)).kind).toBe("suspended");
    await requestCancel(j, "h1");
    expect(await drive(j, "h1", wf)).toEqual({ kind: "cancelled" });
    expect(log).toEqual(["refund"]); // compensation ran
    expect(
      (await j.load("h1")).some((l) => l.entry.kind === "step-end" && l.entry.name === "refund"),
    ).toBe(true);
  });

  it("multiple ctx.onCancel handlers run LIFO", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "h2");
    const order: string[] = [];
    const wf = defineWorkflow("w", async (ctx) => {
      ctx.onCancel(() => ctx.run("first", () => void order.push("first")));
      ctx.onCancel(() => ctx.run("second", () => void order.push("second")));
      await ctx.await("go");
      return "done";
    });
    await drive(j, "h2", wf);
    await requestCancel(j, "h2");
    await drive(j, "h2", wf);
    expect(order).toEqual(["second", "first"]); // last-registered runs first
  });

  it("ctx.onCancel handlers do NOT run when the workflow completes normally", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "h3");
    const log: string[] = [];
    const wf = defineWorkflow("w", (ctx) => {
      ctx.onCancel(() => ctx.run("refund", () => void log.push("refund")));
      return Promise.resolve("done"); // no suspend, no cancel → completes
    });
    expect(await drive(j, "h3", wf)).toEqual({ kind: "completed", value: "done" });
    expect(log).toEqual([]);
  });

  it("compensation steps are exactly-once across replay of a cancelled run", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "h4");
    let refunds = 0;
    const wf = defineWorkflow("w", async (ctx) => {
      ctx.onCancel(() =>
        ctx.run("refund", () => {
          refunds++;
          return 1;
        }),
      );
      await ctx.await("go");
      return "done";
    });
    await drive(j, "h4", wf);
    await requestCancel(j, "h4");
    expect((await drive(j, "h4", wf)).kind).toBe("cancelled");
    expect(refunds).toBe(1);
    // Replay the cancelled run from the history BEFORE its terminal (a journal that holds one is
    // never executed) — compensation replays from the journal, it is not re-run.
    const replayed = await executeRun("h4", {
      workflow: wf,
      journal: j,
      loaded: beforeTerminal(await j.load("h4")),
    });
    expect(replayed.kind).toBe("cancelled");
    expect(refunds).toBe(1);
    // …and a plain redelivery over the finished journal reports the terminal without running anything.
    expect((await drive(j, "h4", wf)).kind).toBe("cancelled");
    expect(refunds).toBe(1);
  });

  it("an onCancel handler that attempts to suspend does not block the cancel (best-effort)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "h5");
    const wf = defineWorkflow("w", async (ctx) => {
      ctx.onCancel(async () => {
        await ctx.await("never2"); // illegal: a handler may not suspend (v0.2) — thrown + swallowed
      });
      await ctx.await("go");
      return "done";
    });
    await drive(j, "h5", wf);
    await requestCancel(j, "h5");
    expect((await drive(j, "h5", wf)).kind).toBe("cancelled"); // the run still cancels cleanly
  });

  it("continueAsNew with a pending cancel cancels in place (next generation not started)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "c8");
    // No step before the rollover: this pins continueAsNew's OWN cancel check (a fresh step would
    // now hard-stop earlier and never reach it).
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.continueAsNew({ n: 1 });
      return "unreachable";
    });
    await requestCancel(j, "c8");
    expect(await drive(j, "c8", wf)).toEqual({ kind: "cancelled" });
    const loaded = await j.load("c8");
    expect(loaded.some((l) => l.entry.kind === "continued")).toBe(false); // did NOT roll over
    expect(loaded.some((l) => l.entry.kind === "cancelled")).toBe(true);
  });

  it("an onCancel handler calling continueAsNew throws (local cleanup only) and never hangs the cancel", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "h7");
    const log: string[] = [];
    const wf = defineWorkflow("w", async (ctx) => {
      ctx.onCancel(async () => {
        await ctx.continueAsNew({ n: 1 }); // illegal in cleanup: would halt into runCleanup and hang
      });
      ctx.onCancel(() => ctx.run("comp", () => void log.push("comp")));
      await ctx.await("go");
      return "done";
    });
    await drive(j, "h7", wf);
    await requestCancel(j, "h7");
    expect((await drive(j, "h7", wf)).kind).toBe("cancelled"); // thrown + swallowed; cancel completes
    expect(log).toEqual(["comp"]); // the other handler still ran
    expect((await j.load("h7")).some((l) => l.entry.kind === "continued")).toBe(false);
  });

  it("an onCancel handler that throws does not block the other handlers or the cancel", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "h6");
    const log: string[] = [];
    const wf = defineWorkflow("w", async (ctx) => {
      ctx.onCancel(() => ctx.run("a", () => void log.push("a")));
      ctx.onCancel(() => {
        throw new Error("handler boom");
      });
      ctx.onCancel(() => ctx.run("b", () => void log.push("b")));
      await ctx.await("go");
      return "done";
    });
    await drive(j, "h6", wf);
    await requestCancel(j, "h6");
    expect((await drive(j, "h6", wf)).kind).toBe("cancelled");
    expect(log).toEqual(["b", "a"]); // LIFO: b, (throw → swallowed), a — both still ran
  });
});

// Cancel × fan-out: the existing tests cancel a single pending op. These pin the cancel-vs-completion
// race INSIDE a multi-branch resolution, where `beforeCancel` decides per branch whether a resolve
// counts. The D1↔D3 pair is the sharp contrast: the SAME workflow completes or cancels solely on
// whether one branch's resolve landed before or after the cancel-requested stream-seq.
describe("cancellation × fan-out (pure engine)", () => {
  const childWf = defineWorkflow("child", (_c, n: number) => Promise.resolve(n));
  const children: Children = { start: () => Promise.resolve() };
  const seedCall = (j: Journal, run: string, seq: number): Promise<void> =>
    append(
      j,
      run,
      stamp(
        { kind: "call", seq, name: "child", childRunId: `${run}_${seq}`, wf: "child", input: seq },
        0,
      ),
      msgId.command(run, seq),
    );
  const seedAwait = (j: Journal, run: string, seq: number, name: string): Promise<void> =>
    append(j, run, stamp({ kind: "await", seq, name }, 0), msgId.command(run, seq));
  const resolveVal = (j: Journal, run: string, seq: number, value: unknown): Promise<void> =>
    append(
      j,
      run,
      stamp({ kind: "resolve", seq: NON_COMMAND_SEQ, resolvesSeq: seq, value }, 0),
      msgId.resolve(run, seq),
    );
  const sumWf = defineWorkflow("w", async (ctx) => {
    const [a, b] = await Promise.all([ctx.startCall(childWf, 1), ctx.startCall(childWf, 2)]);
    return a + b;
  });

  it("D1: a fan-out with BOTH branches resolved BEFORE the cancel COMPLETES (cancel ignored — no frontier)", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "cf1");
    await seedCall(j, "cf1", 0);
    await seedCall(j, "cf1", 1);
    await resolveVal(j, "cf1", 0, 10); // both resolves land...
    await resolveVal(j, "cf1", 1, 20);
    await requestCancel(j, "cf1"); // ...BEFORE the cancel-requested
    const out = await executeRun("cf1", {
      workflow: sumWf,
      journal: j,
      loaded: await j.load("cf1"),
      children,
    });
    // beforeCancel admits BOTH pre-cancel resolves → Promise.all completes with their real values →
    // the workflow returns (no suspend point), so the cancel never lands. (Sum proves both were used.)
    expect(out).toEqual({ kind: "completed", value: 30 });
  });

  it("D2: a fan-out with one branch resolved BEFORE the cancel and a PENDING sibling CANCELS at the sibling", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "cf2");
    await seedCall(j, "cf2", 0);
    await seedAwait(j, "cf2", 1, "go");
    await resolveVal(j, "cf2", 0, 10); // branch 0 resolved before the cancel
    await requestCancel(j, "cf2"); // branch 1 ("go") never signalled → pending
    const wf = defineWorkflow("w", async (ctx) => {
      await Promise.all([ctx.startCall(childWf, 1), ctx.startAwait<number>("go")]);
      return "done";
    });
    const out = await executeRun("cf2", {
      workflow: wf,
      journal: j,
      loaded: await j.load("cf2"),
      children,
    });
    // Branch 0 resolves fine; the pending sibling is the suspend frontier → an uncatchable hard-stop.
    expect(out).toEqual({ kind: "cancelled" });
  });

  it("D3: SAME fan-out, but branch 1's resolve landed AFTER the cancel → frozen out → the run CANCELS", async () => {
    const j = new InMemoryJournal();
    await seedStart(j, "cf3");
    await seedCall(j, "cf3", 0);
    await seedCall(j, "cf3", 1);
    await resolveVal(j, "cf3", 0, 10); // branch 0 resolved before the cancel
    await requestCancel(j, "cf3");
    await resolveVal(j, "cf3", 1, 20); // branch 1's resolve is AFTER the cancel-requested → frozen
    const out = await executeRun("cf3", {
      workflow: sumWf,
      journal: j,
      loaded: await j.load("cf3"),
      children,
    });
    // beforeCancel freezes branch 1's post-cancel resolve → it reads as pending → the run cancels
    // (contrast D1: identical workflow, completed 30, solely because both resolves beat the cancel).
    expect(out).toEqual({ kind: "cancelled" });
  });
});

describe("cancellation terminal adopt (pure)", () => {
  it("a terminal CANCELLED CAS-loser adopts — exactly one cancelled entry is journaled", async () => {
    // Two incarnations of a cancel-requested run race the `cancelled` terminal; the CAS loser must adopt
    // the canonical terminal (no duplicate) — the cancelled-kind branch of adoptCanonical/reconcile.
    const j = new InMemoryJournal();
    await seedStart(j, "adopt-cancel");
    // Pre-seed the await command so BOTH incarnations REPLAY it (no fresh command append to race, which
    // would fence the loser at the await before it ever reaches the terminal) — isolating the race to
    // the terminal, which is what this test pins.
    await append(
      j,
      "adopt-cancel",
      stamp({ kind: "await", seq: 0, name: "go" }, 0),
      msgId.command("adopt-cancel", 0),
    );
    await requestCancel(j, "adopt-cancel");
    const wf = defineWorkflow("w", async (ctx) => {
      await ctx.await("go");
      return "done";
    });
    const snap = await j.load("adopt-cancel");
    const [x, y] = await Promise.all([
      executeRun("adopt-cancel", { workflow: wf, journal: j, loaded: snap }),
      executeRun("adopt-cancel", { workflow: wf, journal: j, loaded: snap }),
    ]);
    expect(x).toEqual({ kind: "cancelled" });
    expect(y).toEqual({ kind: "cancelled" });
    expect((await j.load("adopt-cancel")).filter((l) => l.entry.kind === "cancelled")).toHaveLength(
      1,
    );
  });
});
