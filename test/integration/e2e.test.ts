// End-to-end against a real nats-server: the worker+client happy path, and a crash-mid-run proof
// that completed steps replay WITHOUT re-executing (the headline correctness claim).

import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import { connect, type NatsConnection } from "@nats-io/transport-node";
import { afterAll, beforeAll, describe, expect, inject, it } from "vitest";
import { over } from "../harness/payload.js";
import { executeRun } from "../../src/core/execute.js";
import { defineWorkflow } from "../../src/core/workflow.js";
import { CancelledError, DurableTimeoutError } from "../../src/errors.js";
import { jsonCodec } from "../../src/journal/codec.js";
import { msgId, NON_COMMAND_SEQ, stamp } from "../../src/journal/entry.js";
import { createClient, createWorker, provision } from "../harness/engine.js";
import { NatsJournal } from "../harness/shell/journal-store.js";
import { names } from "../harness/shell/names.js";
import { RunsStore } from "../harness/shell/runs.js";

const natsUrl = inject("natsUrl");

async function waitUntil(cond: () => Promise<boolean>, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await cond()) return;
    if (Date.now() > deadline) throw new Error("waitUntil timed out");
    await new Promise((r) => setTimeout(r, 50));
  }
}

describe.skipIf(!natsUrl)("end-to-end (worker + client, real nats-server)", () => {
  let nc: NatsConnection;
  beforeAll(async () => {
    nc = await connect({ servers: natsUrl! });
  });
  afterAll(async () => {
    await nc.drain();
  });

  it("happy path: start → worker drives the run → result", async () => {
    const charge = defineWorkflow("charge_e2e", async (ctx, input: { amount: number }) => {
      const a = await ctx.run("s1", () => "reserved");
      const b = await ctx.run("s2", () => input.amount * 2);
      return { a, b };
    });
    const worker = createWorker({ nc, workflows: [charge], prefix: "e2e1" });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e1" });
      const handle = await client.start(charge, { amount: 21 });
      expect(await handle.result(15_000)).toEqual({ a: "reserved", b: 42 });
      expect((await handle.status()).state).toBe("completed");
    } finally {
      await worker.stop();
    }
  });

  // durable-sdk: "load() deletes its ephemeral consumer (no per-replay consumer leak)" is dropped.
  // It tested the library's NatsJournal reader, which is the engine's now; the engine's reads are
  // covered by the leftover-consumer check after every test (harness/setup-each.ts).
  it("crash mid-run: completed steps replay WITHOUT re-executing", async () => {
    const n = names("e2e2");
    await provision(nc, n, { streamMaxBytes: 64 * 1024 * 1024 });
    const journal = new NatsJournal(jetstream(nc), await jetstreamManager(nc), n, jsonCodec);
    const runId = `crash-${Date.now()}`;
    await journal.append(runId, {
      entry: stamp({ kind: "start", seq: 0, wf: "crashwf", wfVersion: 1, input: {} }, Date.now()),
      msgID: msgId.start(runId),
      expectLastSeq: 0,
    });

    const counter = { s1: 0, s2: 0, s3: 0 };
    const build = (gate3: () => Promise<void>) =>
      defineWorkflow("crashwf", async (ctx) => {
        const a = await ctx.run("s1", () => {
          counter.s1 += 1;
          return 1;
        });
        const b = await ctx.run("s2", () => {
          counter.s2 += 1;
          return 2;
        });
        const c = await ctx.run("s3", async () => {
          await gate3();
          counter.s3 += 1;
          return 3;
        });
        return a + b + c;
      });

    // Incarnation 1 blocks forever inside step 3 (simulating a crash) — abandoned, never finalized.
    void executeRun(runId, {
      workflow: build(() => new Promise<void>(() => {})),
      journal,
      loaded: await journal.load(runId),
    });
    await waitUntil(
      async () =>
        (await journal.load(runId)).some((l) => l.entry.kind === "step-end" && l.entry.seq === 1),
      5_000,
    );
    expect(counter).toEqual({ s1: 1, s2: 1, s3: 0 });

    // Incarnation 2 (restart): replays s1+s2 from the journal, runs only s3.
    const out = await executeRun(runId, {
      workflow: build(() => Promise.resolve()),
      journal,
      loaded: await journal.load(runId),
    });
    expect(out).toEqual({ kind: "completed", value: 6 });
    expect(counter).toEqual({ s1: 1, s2: 1, s3: 1 }); // s1 & s2 were NOT re-executed
  });

  it("a failing workflow surfaces via result() rejection and failed status", async () => {
    const boom = defineWorkflow("boom_e2e", async (ctx) => {
      await ctx.run("s1", () => "ok");
      throw new Error("kaboom");
    });
    const worker = createWorker({ nc, workflows: [boom], prefix: "e2e3" });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e3" });
      const handle = await client.start(boom, {});
      await expect(handle.result(15_000)).rejects.toThrow(/kaboom/);
      expect((await handle.status()).state).toBe("failed");
    } finally {
      await worker.stop();
    }
  });

  it("idempotent start: the same runId starts a run exactly once", async () => {
    const counter = { n: 0 };
    const once = defineWorkflow("once_e2e", (ctx) =>
      ctx.run("s1", () => {
        counter.n += 1;
        return "done";
      }),
    );
    const worker = createWorker({ nc, workflows: [once], prefix: "e2e4" });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e4" });
      const runId = `dup-run-${Date.now()}`;
      const r1 = await (await client.start(once, {}, { runId })).result(15_000);
      const r2 = await (await client.start(once, {}, { runId })).result(15_000); // same runId again
      expect(r1).toBe("done");
      expect(r2).toBe("done");
      expect(counter.n).toBe(1); // the side effect ran exactly once
    } finally {
      await worker.stop();
    }
  });

  it("ctx.sleep durably suspends and resumes via native scheduling", async () => {
    const napped = defineWorkflow("nap_e2e", async (ctx) => {
      const a = await ctx.run("s1", () => "a");
      await ctx.sleep("wait", 800);
      const b = await ctx.run("s2", () => "b");
      return `${a}-${b}`;
    });
    const worker = createWorker({ nc, workflows: [napped], prefix: "e2e5" });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e5" });
      const t0 = Date.now();
      const handle = await client.start(napped, {});
      const res = await handle.result(20_000);
      expect(res).toBe("a-b");
      expect(Date.now() - t0).toBeGreaterThanOrEqual(700); // it actually waited ~800ms
    } finally {
      await worker.stop();
    }
  });

  it("client.signal resolves a ctx.await end-to-end", async () => {
    const approval = defineWorkflow("approval_e2e", async (ctx) => {
      const a = await ctx.run("s1", () => "reserved");
      const decision = await ctx.await<{ ok: boolean }>("decision");
      return `${a}:${decision.ok ? "yes" : "no"}`;
    });
    const worker = createWorker({ nc, workflows: [approval], prefix: "e2e6" });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e6" });
      const handle = await client.start(approval, {});
      await client.signal(handle.runId, "decision", { ok: true });
      expect(await handle.result(15_000)).toBe("reserved:yes");
    } finally {
      await worker.stop();
    }
  });

  it("ctx.call runs a sub-workflow end-to-end", async () => {
    const double = defineWorkflow("double_e2e", (_ctx, input: number) =>
      Promise.resolve(input * 2),
    );
    const parent = defineWorkflow("parent_e2e", async (ctx) => {
      const a = await ctx.call(double, 21);
      return ctx.run("after", () => a + 1);
    });
    const worker = createWorker({ nc, workflows: [parent, double], prefix: "e2e7" });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e7" });
      const handle = await client.start(parent, {});
      expect(await handle.result(15_000)).toBe(43); // (21 * 2) + 1
    } finally {
      await worker.stop();
    }
  });

  it("nested sub-workflows propagate results up the chain", async () => {
    const c = defineWorkflow("c_e2e", (_ctx, input: number) => Promise.resolve(input + 1));
    const p = defineWorkflow(
      "p_e2e",
      async (ctx, input: number) => (await ctx.call(c, input)) * 10,
    );
    const g = defineWorkflow("g_e2e", async (ctx) => `result=${await ctx.call(p, 4)}`);
    const worker = createWorker({ nc, workflows: [g, p, c], prefix: "e2e8" });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e8" });
      const handle = await client.start(g, {});
      expect(await handle.result(15_000)).toBe("result=50"); // ((4 + 1) * 10) = 50
    } finally {
      await worker.stop();
    }
  });

  it("a failing sub-workflow fails the parent end-to-end", async () => {
    const badChild = defineWorkflow("badchild_e2e", () => Promise.reject(new Error("child boom")));
    const parent = defineWorkflow("parentfail_e2e", async (ctx) => {
      await ctx.call(badChild, undefined);
      return "unreachable";
    });
    const worker = createWorker({ nc, workflows: [parent, badChild], prefix: "e2e9" });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e9" });
      const handle = await client.start(parent, {});
      await expect(handle.result(15_000)).rejects.toThrow(/sub-workflow|child boom/);
      expect((await handle.status()).state).toBe("failed");
    } finally {
      await worker.stop();
    }
  });

  it("continue-as-new chains generations; result() follows the chain to the final value", async () => {
    const loop = defineWorkflow("loop_e2e", async (ctx, input: { n: number }) => {
      await ctx.run("tick", () => input.n);
      if (input.n >= 3) return `done@${input.n}`;
      return ctx.continueAsNew({ n: input.n + 1 });
    });
    const worker = createWorker({ nc, workflows: [loop], prefix: "e2e10" });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e10" });
      const handle = await client.start(loop, { n: 1 });
      // result() transparently follows runId_g1, _g2, … to the generation that finally returns
      expect(await handle.result(20_000)).toBe("done@3");
      // the original generation finalized as `continued`, pointing at the next one
      const st = await handle.status();
      expect(st.state).toBe("continued");
      expect(st.next).toBe(`${handle.runId}_g1`);
      // INTERMEDIATE generations must also land in the KV status index — i.e. their generated runIds
      // must be valid KV keys. (A `~`-separated id is a valid subject but an INVALID KV key, so
      // markRunning/markContinued silently failed and these vanished from status(). Guards that.)
      const g1 = await client.get(`${handle.runId}_g1`).status();
      expect(g1.state).toBe("continued");
      expect(g1.next).toBe(`${handle.runId}_g2`);
      const g2 = await client.get(`${handle.runId}_g2`).status();
      expect(g2.state).toBe("completed");
    } finally {
      await worker.stop();
    }
  });

  it("CHAOS: two workers + concurrent signals — exactly-once JOURNALING, at-least-once execution", async () => {
    // The contract under concurrent multi-worker overlap: each step is JOURNALED exactly once (so the
    // result is deterministic), but a step's fn may EXECUTE more than once — a second worker that picks
    // up the run while a step is in-flight (begin written, end not yet) re-runs it via the resume path.
    // That at-least-once window is what ctx.key() is for; a gating lease would narrow it. See DESIGN §7.
    const ran: string[] = []; // shared across BOTH workers (same process) → every fn execution
    const chaos = defineWorkflow("chaos_e2e", async (ctx) => {
      await ctx.await("go");
      const step = (name: string) => () => {
        ran.push(name);
        return name;
      };
      const a = await ctx.run("s1", step("s1"));
      const b = await ctx.run("s2", step("s2"));
      const c = await ctx.run("s3", step("s3"));
      return `${a}-${b}-${c}`;
    });
    // two worker processes sharing the same durable consumer (started sequentially to avoid a
    // consumer-provisioning race); both pump and can processOne the same run concurrently.
    // lease: false ON PURPOSE — this test pins the LEASE-LESS contract (the exact window the
    // gating lease narrows); the gated twin lives in lease.test.ts.
    const w1 = createWorker({ nc, workflows: [chaos], prefix: "e2e11", lease: false });
    const w2 = createWorker({ nc, workflows: [chaos], prefix: "e2e11", lease: false });
    await w1.start();
    await w2.start();
    try {
      const client = createClient({ nc, prefix: "e2e11" });
      const handle = await client.start(chaos, {});
      // fire several concurrent "go" signals: only the first resolves the await, the rest are extra
      // work items that make BOTH workers race the post-await steps.
      await Promise.all(Array.from({ length: 6 }, () => client.signal(handle.runId, "go", {})));
      // exactly-once JOURNALING: the result is built from the recorded step values, never duplicated.
      expect(await handle.result(20_000)).toBe("s1-s2-s3");
      // at-least-once EXECUTION: every step ran (≥ once); under overlap some may have re-run.
      expect(["s1", "s2", "s3"].every((s) => ran.includes(s))).toBe(true);
    } finally {
      await w1.stop();
      await w2.stop();
    }
  });

  // durable-sdk: "a FENCED inbound event is retried, not lost" is dropped. It injected the fence by
  // patching NatsJournal.prototype.append inside the worker; recording an inbound event is the
  // engine's now (it journals the event before the poll hands the item out, retried past a fence:
  // durable-engine core.RecordInbound and its tests), and no SDK request carries it.
  it("ctx.await timeout fires when no signal arrives (real timer)", async () => {
    const wf = defineWorkflow("await_to_e2e", async (ctx) => {
      try {
        await ctx.await<string>("never", { timeoutMs: 600 });
        return "got-signal";
      } catch {
        return "timed-out";
      }
    });
    const worker = createWorker({ nc, workflows: [wf], prefix: "e2e12" });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e12" });
      const t0 = Date.now();
      const handle = await client.start(wf, {});
      expect(await handle.result(20_000)).toBe("timed-out");
      expect(Date.now() - t0).toBeGreaterThanOrEqual(500); // it actually waited ~600ms for the deadline
    } finally {
      await worker.stop();
    }
  });

  it("ctx.await timeout resolves on a signal that beats the deadline (real)", async () => {
    const wf = defineWorkflow("await_to2_e2e", async (ctx) => {
      try {
        return await ctx.await<string>("go", { timeoutMs: 10_000 });
      } catch {
        return "timed-out";
      }
    });
    const worker = createWorker({ nc, workflows: [wf], prefix: "e2e13" });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e13" });
      const handle = await client.start(wf, {});
      await client.signal(handle.runId, "go", "ok");
      expect(await handle.result(15_000)).toBe("ok"); // signal beat the 10s deadline
    } finally {
      await worker.stop();
    }
  });

  it("a sub-workflow can continue-as-new; the parent resumes with the final generation's result", async () => {
    // The child runs across generations; only the FINAL generation notifies the parent (the parent
    // link must thread forward through each `continued`). If threading broke, the parent would hang.
    const child = defineWorkflow("can_child_e2e", async (ctx, input: { n: number }) => {
      await ctx.run("step", () => input.n);
      if (input.n >= 2) return `child-done@${input.n}`;
      return ctx.continueAsNew({ n: input.n + 1 });
    });
    const parent = defineWorkflow("can_parent_e2e", async (ctx) => {
      const r = await ctx.call(child, { n: 0 });
      return `parent-got:${r}`;
    });
    const worker = createWorker({ nc, workflows: [parent, child], prefix: "e2e14" });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e14" });
      const handle = await client.start(parent, {});
      expect(await handle.result(20_000)).toBe("parent-got:child-done@2");
    } finally {
      await worker.stop();
    }
  });

  it("a failing generation in a continue-as-new chain surfaces via result() rejection", async () => {
    const loop = defineWorkflow("loopfail_e2e", async (ctx, input: { n: number }) => {
      await ctx.run("tick", () => input.n);
      if (input.n >= 2) throw new Error("boom at gen 2");
      return ctx.continueAsNew({ n: input.n + 1 });
    });
    const worker = createWorker({ nc, workflows: [loop], prefix: "e2e15" });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e15" });
      const handle = await client.start(loop, { n: 0 });
      // result() follows the chain gen0 → gen1 → gen2 and surfaces gen2's failure
      await expect(handle.result(20_000)).rejects.toThrow(/boom at gen 2/);
    } finally {
      await worker.stop();
    }
  });

  it("a non-serializable step result fails the run instead of looping forever", async () => {
    // A BigInt cannot be JSON-encoded, so the step-end append throws deterministically. The run must
    // FAIL terminally (SerializationError), not NAK-loop forever until result() times out. Regression
    // for audit finding A2 (a deterministic encode error was misclassified as a transient infra retry).
    const bad = defineWorkflow("ser_e2e", (ctx) => ctx.run("s1", () => BigInt(1) as unknown));
    const worker = createWorker({ nc, workflows: [bad], prefix: "e2e16" });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e16" });
      const handle = await client.start(bad, {});
      await expect(handle.result(10_000)).rejects.toThrow(/Serializ|encode|BigInt/i);
      expect((await handle.status()).state).toBe("failed");
    } finally {
      await worker.stop();
    }
  });

  it("CONCURRENCY: runs of one workflow proceed in parallel on a single worker (no head-of-line blocking)", async () => {
    // Pre-fix the pump processed one item at a time per workflow: six runs with a 400ms step took
    // ≥ 2400ms serially, and one slow step starved every other run of that workflow on the worker.
    const slow = defineWorkflow("conc_wf", (ctx) =>
      ctx.run("slow", async () => {
        await new Promise((r) => setTimeout(r, 400));
        return 1;
      }),
    );
    const worker = createWorker({
      nc,
      workflows: [slow],
      prefix: "e2econc",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2econc" });
      // Warm up first: the client provisions on its first call, and at a WAN round trip
      // (DE_TEST_RTT_MS) that alone outlasts the bound below — it is not what is being measured.
      await (await client.start(slow, {})).result(20_000);
      const t0 = Date.now();
      const handles = await Promise.all(Array.from({ length: 6 }, () => client.start(slow, {})));
      await Promise.all(handles.map((h) => h.result(20_000)));
      const elapsed = Date.now() - t0;
      // Serial floor: 6 × (the 400 ms step + a run's ~11 dependent round trips). Parallel: one of
      // those, plus overhead. The bound sits between the two at any round-trip time.
      const rtt = inject("natsRttMs");
      expect(elapsed).toBeLessThan(2_000 + 25 * rtt);
      expect(2_000 + 25 * rtt).toBeLessThan(6 * (400 + 11 * rtt));
    } finally {
      await worker.stop();
    }
  });

  it("HEAL: client.start re-enqueues an orphaned run (start entry landed, work item lost)", async () => {
    // Simulate the crash window: a previous start() appended the journal start entry, then died
    // BEFORE publishing the work item (or the publish failed and the caller is retrying). The
    // journal says the run exists — so the retry's append FENCES (CAS dominates dedup) — but no
    // work item exists and nothing re-drives it. start() must ALWAYS enqueue, not only when new.
    const n = names("e2ehealA");
    await provision(nc, n, { streamMaxBytes: 64 * 1024 * 1024 });
    const heal = defineWorkflow("heal_wf", (ctx, input: { x: number }) =>
      ctx.run("s1", () => input.x * 2),
    );
    const runId = `orphan-${Date.now()}`;
    const journal = new NatsJournal(jetstream(nc), await jetstreamManager(nc), n, jsonCodec);
    await journal.append(runId, {
      entry: stamp(
        { kind: "start", seq: 0, wf: "heal_wf", wfVersion: 1, input: { x: 21 } },
        Date.now(),
      ),
      msgID: msgId.start(runId),
      expectLastSeq: 0,
    });

    const worker = createWorker({
      nc,
      workflows: [heal],
      prefix: "e2ehealA",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2ehealA" });
      const handle = await client.start(heal, { x: 21 }, { runId }); // the caller's retry
      expect(await handle.result(15_000)).toBe(42); // pre-fix: result() times out (never enqueued)
      // and the start entry is still exactly-once (the retry fenced; it did not double-append)
      const starts = (await journal.load(runId)).filter((l) => l.entry.kind === "start");
      expect(starts).toHaveLength(1);
    } finally {
      await worker.stop();
    }
  });

  it("HEAL: a parent replay re-enqueues an orphaned child (child start landed, work item lost)", async () => {
    // Same window one level down: a worker died between the CHILD's start-append and its enqueue.
    // On the parent's replay, ensureChild re-runs children.start — whose append fences (the entry
    // landed) — and before the fix that swallowed-fence SKIPPED the enqueue, so the parent
    // suspended forever on a child no worker would ever drive.
    const n = names("e2ehealB");
    await provision(nc, n, { streamMaxBytes: 64 * 1024 * 1024 });
    const child = defineWorkflow("heal_child", (ctx, input: { x: number }) =>
      ctx.run("c1", () => input.x * 2),
    );
    const parent = defineWorkflow("heal_parent", (ctx, input: { x: number }) =>
      ctx.call(child, input),
    );
    const parentRunId = `phealer-${Date.now()}`;
    const childRunId = `${parentRunId}_0`; // deterministic: the call is the parent's command seq 0
    const journal = new NatsJournal(jetstream(nc), await jetstreamManager(nc), n, jsonCodec);
    await journal.append(childRunId, {
      entry: stamp(
        {
          kind: "start",
          seq: 0,
          wf: "heal_child",
          wfVersion: 1,
          input: { x: 21 },
          parent: { runId: parentRunId, seq: 0, wf: "heal_parent" },
        },
        Date.now(),
      ),
      msgID: msgId.start(childRunId),
      expectLastSeq: 0,
    });

    const worker = createWorker({
      nc,
      workflows: [parent, child],
      prefix: "e2ehealB",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2ehealB" });
      const handle = await client.start(parent, { x: 21 }, { runId: parentRunId });
      expect(await handle.result(15_000)).toBe(42); // pre-fix: parent suspends forever
      const starts = (await journal.load(childRunId)).filter((l) => l.entry.kind === "start");
      expect(starts).toHaveLength(1); // the re-start fenced — exactly one child start entry
    } finally {
      await worker.stop();
    }
  });

  it("a work item for a run with NO journal retries briefly, then is dropped — worker stays healthy", async () => {
    // A no-start read is either a transient read hiccup (must NOT be dropped — a timer wake is
    // one-shot) or a genuinely gone journal (must not redeliver forever). Policy: NAK-with-backoff,
    // term after bounded redeliveries. The forged item must drain and never wedge the consumer.
    const n = names("e2eghost");
    const ghost = defineWorkflow("ghost_wf", (ctx, input: { x: number }) =>
      ctx.run("s1", () => input.x + 1),
    );
    const worker = createWorker({
      nc,
      workflows: [ghost],
      prefix: "e2eghost",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const js = jetstream(nc);
      const jsm = await jetstreamManager(nc);
      await js.publish(
        n.workSubject("ghost_wf"),
        new TextEncoder().encode(JSON.stringify({ runId: "ghost-no-journal", kind: "start" })),
      );
      // Drains after the bounded NAK-with-backoff cycle (250+500+1000+2000ms, then term).
      await waitUntil(async () => {
        const ci = await jsm.consumers.info(n.workStream, "de-worker-ghost_wf");
        return ci.num_pending === 0 && ci.num_ack_pending === 0;
      }, 20_000);
      // The consumer is not wedged: a real run through the same worker completes normally.
      const client = createClient({ nc, prefix: "e2eghost" });
      expect(await (await client.start(ghost, { x: 41 })).result(15_000)).toBe(42);
    } finally {
      await worker.stop();
    }
  });

  it("HEAL: re-driving a terminal repairs a stale KV status (crash between journal terminal and KV finalize)", async () => {
    // The journal terminal landed but the worker died before the KV finalize: status() said
    // 'running' forever while result() (journal truth) already resolved. The terminal re-drive
    // must mirror the journaled outcome into KV, not just ack.
    const n = names("e2erdrv");
    await provision(nc, n, { streamMaxBytes: 64 * 1024 * 1024 });
    const journal = new NatsJournal(jetstream(nc), await jetstreamManager(nc), n, jsonCodec);
    const runs = new RunsStore(await new Kvm(nc).create(n.runsKv));
    const rdrv = defineWorkflow("rdrv_wf", () => Promise.resolve("never-runs"));
    const runId = `rdrv-${Date.now()}`;
    let last = 0;
    for (const [entry, mid] of [
      [stamp({ kind: "start", seq: 0, wf: "rdrv_wf", wfVersion: 1, input: {} }, Date.now()), msgId.start(runId)], // prettier-ignore
      [stamp({ kind: "result", seq: NON_COMMAND_SEQ, value: 7 }, Date.now()), msgId.terminal(runId)], // prettier-ignore
    ] as const) {
      const ack = await journal.append(runId, { entry, msgID: mid, expectLastSeq: last });
      last = ack.streamSeq;
    }
    await runs.markRunning(runId, "rdrv_wf", 1); // the stale hint the crashed worker left behind

    const worker = createWorker({
      nc,
      workflows: [rdrv],
      prefix: "e2erdrv",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2erdrv" });
      expect((await client.get(runId).status()).state).toBe("running"); // stale before the wake
      await jetstream(nc).publish(
        n.workSubject("rdrv_wf"),
        new TextEncoder().encode(JSON.stringify({ runId, kind: "start" })),
      );
      await waitUntil(async () => (await client.get(runId).status()).state === "completed", 15_000);
      expect(await client.get(runId).result(5_000)).toBe(7); // journal truth, unchanged
    } finally {
      await worker.stop();
    }
  });

  it("with offload DISABLED, a step result over max_payload FAILS the run fast instead of NAK-looping", async () => {
    // Over THIS server's max_payload (harness/payload.ts). An oversized step result is deterministic
    // for its value — before the fix it was classified as transient infra and hot-NAK-looped forever
    // (result() would hang and the poison item head-of-line-blocked the consumer). Audit P0.
    // Offload (default ON) now absorbs this value instead — offload.test.ts proves that path —
    // so this pins the `offload: false` semantics: fail LOUD and fast, exactly as before.
    const big = defineWorkflow("pay_e2e", (ctx) => ctx.run("huge", () => "x".repeat(over(nc))));
    const worker = createWorker({
      nc,
      workflows: [big],
      prefix: "e2epay",
      streamMaxBytes: 64 * 1024 * 1024,
      offload: false,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2epay" });
      const handle = await client.start(big, {});
      await expect(handle.result(10_000)).rejects.toThrow(/PayloadTooLarge|max_payload/);
      expect((await handle.status()).state).toBe("failed");
    } finally {
      await worker.stop();
    }
  });

  it("result() resolves via the journal fallback even when NO status hint is ever written", async () => {
    // The KV record is only a wake-up call — result() must converge on journal truth alone. Staged:
    // a run whose terminal lands while a caller is already waiting, with NO worker and NO KV write
    // ever (the hint path fully absent) → the coarse fallback tick must still deliver the result.
    const n = names("e2ewatch");
    await provision(nc, n, { streamMaxBytes: 64 * 1024 * 1024 });
    const journal = new NatsJournal(jetstream(nc), await jetstreamManager(nc), n, jsonCodec);
    const runId = `watch-${Date.now()}`;
    const ack = await journal.append(runId, {
      entry: stamp({ kind: "start", seq: 0, wf: "watch_wf", wfVersion: 1, input: {} }, Date.now()),
      msgID: msgId.start(runId),
      expectLastSeq: 0,
    });

    const client = createClient({ nc, prefix: "e2ewatch" });
    const pending = client.get<number>(runId).result(10_000);
    await new Promise((r) => setTimeout(r, 1_500)); // let it settle into hint-or-tick waiting
    await journal.append(runId, {
      entry: stamp({ kind: "result", seq: NON_COMMAND_SEQ, value: 7 }, Date.now()),
      msgID: msgId.terminal(runId),
      expectLastSeq: ack.streamSeq,
    });
    expect(await pending).toBe(7);
  });

  it("result() rejects with DurableTimeoutError when the run never reaches a terminal", async () => {
    const blocked = defineWorkflow("blocked_e2e", (ctx) => ctx.await<string>("never-arrives"));
    const worker = createWorker({
      nc,
      workflows: [blocked],
      prefix: "e2e17",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e17" });
      const handle = await client.start(blocked, {});
      // the run suspends forever (no signal); result() must reject on its own deadline, not hang
      await expect(handle.result(600)).rejects.toThrow(DurableTimeoutError);
    } finally {
      await worker.stop();
    }
  });

  it("signal throws and status() is 'pending' for an unknown run", async () => {
    const client = createClient({ nc, prefix: "e2e18", streamMaxBytes: 64 * 1024 * 1024 });
    await expect(client.signal("no-such-run", "go", {})).rejects.toThrow(/not found/);
    expect((await client.get("no-such-run").status()).state).toBe("pending");
  });

  it("a redelivered work item for a finalized run is a no-op (terminal short-circuit)", async () => {
    const counter = { n: 0 };
    const once = defineWorkflow("redeliver_e2e", (ctx) =>
      ctx.run("s1", () => {
        counter.n += 1;
        return "done";
      }),
    );
    const worker = createWorker({
      nc,
      workflows: [once],
      prefix: "e2e19",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e19" });
      const handle = await client.start(once, {});
      expect(await handle.result(15_000)).toBe("done");
      expect(counter.n).toBe(1);
      // Re-enqueue a work item for the already-finalized run. The worker must re-drive the terminal
      // idempotently (ack, NO re-execution) — the journal already holds a `result` terminal.
      const n = names("e2e19");
      await jetstream(nc).publish(
        n.workSubject("redeliver_e2e"),
        new TextEncoder().encode(JSON.stringify({ runId: handle.runId, kind: "start" })),
        { msgID: `redeliver-${handle.runId}` },
      );
      // wait until the workqueue drains the redelivered item (i.e. the worker acked it)
      const jsm = await jetstreamManager(nc);
      await waitUntil(
        async () => (await jsm.streams.info(n.workStream)).state.messages === 0,
        5_000,
      );
      expect(counter.n).toBe(1); // still exactly once — the side effect was not re-run
    } finally {
      await worker.stop();
    }
  });

  it("status() reports 'suspended' while a run is parked on an await, then 'completed'", async () => {
    const parked = defineWorkflow("suspend_e2e", async (ctx) => {
      await ctx.run("before", () => "b");
      return await ctx.await<string>("go");
    });
    const worker = createWorker({
      nc,
      workflows: [parked],
      prefix: "e2e20",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e20" });
      const handle = await client.start(parked, {});
      // it runs "before", then parks on the await → status flips running → suspended
      await waitUntil(async () => (await handle.status()).state === "suspended", 10_000);
      await client.signal(handle.runId, "go", "done");
      expect(await handle.result(15_000)).toBe("done");
      expect((await handle.status()).state).toBe("completed"); // suspended → completed on resume
    } finally {
      await worker.stop();
    }
  });

  it("FAN-OUT: Promise.all over concurrent ctx.run steps completes (real CAS, no self-fence)", async () => {
    // Before the append-queue, two concurrent ctx.run begins read the same lastSeq and the loser
    // fenced (10071). Through a real nats-server the append latency WIDENS that race window, so this
    // is the strongest proof the queue serializes each run's CAS: result() resolving at all means no
    // branch self-fenced (a fence would halt the run → result() would time out instead).
    const ran: Record<string, number> = {};
    const fanout = defineWorkflow("fanout_e2e", async (ctx, input: { items: string[] }) => {
      const results = await Promise.all(
        input.items.map((it) =>
          ctx.run(`step-${it}`, () => {
            ran[it] = (ran[it] ?? 0) + 1;
            return it.toUpperCase();
          }),
        ),
      );
      return results.join("");
    });
    const worker = createWorker({
      nc,
      workflows: [fanout],
      prefix: "e2e21",
      streamMaxBytes: 64 * 1024 * 1024, // conserve JS store budget (max_bytes is reserved, not on-demand)
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e21" });
      const handle = await client.start(fanout, { items: ["a", "b", "c", "d", "e"] });
      expect(await handle.result(15_000)).toBe("ABCDE"); // input order preserved by Promise.all
      expect((await handle.status()).state).toBe("completed");
      expect(ran).toEqual({ a: 1, b: 1, c: 1, d: 1, e: 1 }); // each step ran exactly once (one worker)
    } finally {
      await worker.stop();
    }
  });

  it("FAN-OUT: Promise.all over startCall children runs them concurrently end-to-end", async () => {
    // The parent starts N children in ONE pass (durable futures), suspends, and each child-done
    // re-drives it until Promise.all has every result. Proves real child fan-out over the work queue.
    const child = defineWorkflow("fanchild_e2e", (_ctx, n: number) => Promise.resolve(n * 10));
    const parent = defineWorkflow("fanparent_e2e", async (ctx, input: { ns: number[] }) => {
      const results = await Promise.all(input.ns.map((n) => ctx.startCall(child, n)));
      return results.reduce((a, b) => a + b, 0);
    });
    const worker = createWorker({
      nc,
      workflows: [parent, child],
      prefix: "e2e22",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e22" });
      const handle = await client.start(parent, { ns: [1, 2, 3] });
      expect(await handle.result(15_000)).toBe(60); // (1 + 2 + 3) * 10
      expect((await handle.status()).state).toBe("completed");
    } finally {
      await worker.stop();
    }
  });

  it("FAN-OUT: ctx.race against a real timer resolves as a timeout when no signal arrives", async () => {
    // Races a signal-await against a 600ms durable timer; with no signal, the timer wins the race.
    const wf = defineWorkflow("racetimer_e2e", async (ctx) => {
      const r = await ctx.race([ctx.startAwait<string>("go"), ctx.startSleep("deadline", 600)]);
      return typeof r === "string" ? `got:${r}` : "timed-out";
    });
    const worker = createWorker({
      nc,
      workflows: [wf],
      prefix: "e2e23",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e23" });
      const t0 = Date.now();
      const handle = await client.start(wf, {});
      expect(await handle.result(20_000)).toBe("timed-out");
      expect(Date.now() - t0).toBeGreaterThanOrEqual(500); // actually waited for the ~600ms deadline
    } finally {
      await worker.stop();
    }
  });

  it("FAN-OUT: Promise.all over two startAwait signals completes when both are sent", async () => {
    // A run parked on a fan-out of two signals is re-driven by the work queue on EACH signal until both
    // have arrived — proves multi-signal fan-out end-to-end (not just children/timers).
    const wf = defineWorkflow("fansig_e2e", async (ctx) => {
      const [a, b] = await Promise.all([
        ctx.startAwait<string>("alpha"),
        ctx.startAwait<string>("beta"),
      ]);
      return `${a}+${b}`;
    });
    const worker = createWorker({
      nc,
      workflows: [wf],
      prefix: "e2e24",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e24" });
      const handle = await client.start(wf, {});
      await client.signal(handle.runId, "alpha", "A");
      await client.signal(handle.runId, "beta", "B");
      expect(await handle.result(15_000)).toBe("A+B");
    } finally {
      await worker.stop();
    }
  });

  it("FAN-OUT: Promise.allSettled CAPTURES a failing child and completes with mixed results e2e", async () => {
    // The complement of Promise.all (which fails the parent on any child error): allSettled lets the
    // parent collect a failed child's rejection alongside a sibling's value and keep running. Proven
    // over the real work queue — both children are driven to their terminals, then the parent resumes.
    const okChild = defineWorkflow("settle_ok_e2e", (_ctx, n: number) => Promise.resolve(n * 2));
    const badChild = defineWorkflow("settle_bad_e2e", () => Promise.reject(new Error("nope")));
    const parent = defineWorkflow("settle_parent_e2e", async (ctx) => {
      const r = await Promise.allSettled([
        ctx.startCall(okChild, 21),
        ctx.startCall(badChild, undefined),
      ]);
      return r.map((x) => (x.status === "fulfilled" ? `ok:${x.value}` : "rejected"));
    });
    const worker = createWorker({
      nc,
      workflows: [parent, okChild, badChild],
      prefix: "e2e36",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e36" });
      const handle = await client.start(parent, {});
      expect(await handle.result(15_000)).toEqual(["ok:42", "rejected"]);
      expect((await handle.status()).state).toBe("completed");
    } finally {
      await worker.stop();
    }
  });

  it("CANCEL: a run parked on an await is cancelled — result() rejects, status() is cancelled", async () => {
    const wf = defineWorkflow("cancel_await_e2e", async (ctx) => {
      await ctx.run("before", () => "b");
      return ctx.await<string>("never");
    });
    const worker = createWorker({
      nc,
      workflows: [wf],
      prefix: "e2e25",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e25" });
      const handle = await client.start(wf, {});
      await waitUntil(async () => (await handle.status()).state === "suspended", 10_000);
      await client.cancel(handle.runId, { reason: "user bailed" });
      await expect(handle.result(15_000)).rejects.toThrow(CancelledError);
      expect((await handle.status()).state).toBe("cancelled");
    } finally {
      await worker.stop();
    }
  });

  it("CANCEL: a run parked on a long sleep is cancelled before the timer fires", async () => {
    const wf = defineWorkflow("cancel_sleep_e2e", async (ctx) => {
      await ctx.run("before", () => "b");
      await ctx.sleep("long", 60_000); // ~1 min; we cancel well before it fires
      return "woke";
    });
    const worker = createWorker({
      nc,
      workflows: [wf],
      prefix: "e2e26",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e26" });
      const handle = await client.start(wf, {});
      await waitUntil(async () => (await handle.status()).state === "suspended", 10_000);
      await client.cancel(handle.runId);
      await expect(handle.result(15_000)).rejects.toThrow(CancelledError);
    } finally {
      await worker.stop();
    }
  });

  it("CANCEL: cancelling an already-completed run is a no-op", async () => {
    const wf = defineWorkflow("cancel_done_e2e", (ctx) => ctx.run("s", () => "done"));
    const worker = createWorker({
      nc,
      workflows: [wf],
      prefix: "e2e27",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e27" });
      const handle = await client.start(wf, {});
      expect(await handle.result(15_000)).toBe("done"); // completes (no suspend point)
      await client.cancel(handle.runId); // cancel arrives AFTER the terminal
      await new Promise((r) => setTimeout(r, 300)); // let the worker consume the cancel work item
      expect((await handle.status()).state).toBe("completed"); // still completed — the cancel was a no-op
    } finally {
      await worker.stop();
    }
  });

  it("CANCEL: a double-cancel is idempotent (exactly one cancel-requested entry)", async () => {
    const wf = defineWorkflow("cancel_dbl_e2e", async (ctx) => {
      await ctx.run("before", () => "b");
      return ctx.await<string>("never");
    });
    const worker = createWorker({
      nc,
      workflows: [wf],
      prefix: "e2e28",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e28" });
      const handle = await client.start(wf, {});
      await waitUntil(async () => (await handle.status()).state === "suspended", 10_000);
      await client.cancel(handle.runId);
      await client.cancel(handle.runId); // second cancel — must be a no-op
      await expect(handle.result(15_000)).rejects.toThrow(CancelledError);
      expect((await handle.status()).state).toBe("cancelled");
      const n = names("e2e28");
      const journal = new NatsJournal(jetstream(nc), await jetstreamManager(nc), n, jsonCodec);
      const reqs = (await journal.load(handle.runId)).filter(
        (l) => l.entry.kind === "cancel-requested",
      );
      expect(reqs).toHaveLength(1); // collapsed to one cancel-requested
    } finally {
      await worker.stop();
    }
  });

  it("CANCEL: cancelling an unknown run throws (not found)", async () => {
    const client = createClient({ nc, prefix: "e2e29", streamMaxBytes: 64 * 1024 * 1024 });
    await expect(client.cancel("no-such-run")).rejects.toThrow(/not found/);
  });

  it("CANCEL: ctx.onCancel compensation runs end-to-end when a parked run is cancelled", async () => {
    const refunded: string[] = [];
    const wf = defineWorkflow("cancel_cleanup_e2e", async (ctx) => {
      ctx.onCancel(() => ctx.run("refund", () => void refunded.push("refund")));
      await ctx.run("charge", () => "charged");
      return ctx.await<string>("never");
    });
    const worker = createWorker({
      nc,
      workflows: [wf],
      prefix: "e2e30",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e30" });
      const handle = await client.start(wf, {});
      await waitUntil(async () => (await handle.status()).state === "suspended", 10_000);
      await client.cancel(handle.runId);
      await expect(handle.result(15_000)).rejects.toThrow(CancelledError);
      expect(refunded).toEqual(["refund"]); // compensation ran (exactly once) during cancellation
    } finally {
      await worker.stop();
    }
  });

  it("CANCEL: cascades to an in-flight child (cancelling the parent cancels the child)", async () => {
    const child = defineWorkflow("cascade_child_e2e", (ctx) => ctx.await<string>("never"));
    const parent = defineWorkflow("cascade_parent_e2e", async (ctx) => {
      await ctx.call(child, undefined);
      return "done";
    });
    const worker = createWorker({
      nc,
      workflows: [parent, child],
      prefix: "e2e31",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e31" });
      const handle = await client.start(parent, {});
      const childRunId = `${handle.runId}_0`; // deterministic child runId (${parent}_${seq})
      await waitUntil(
        async () => (await client.get(childRunId).status()).state === "suspended",
        10_000,
      );
      await client.cancel(handle.runId);
      await waitUntil(
        async () => (await client.get(childRunId).status()).state === "cancelled",
        10_000,
      );
      expect((await handle.status()).state).toBe("cancelled");
    } finally {
      await worker.stop();
    }
  });

  it("CANCEL: forwards through a continue-as-new chain to the running generation", async () => {
    // gen 0 rolls over to gen 1, which parks on an await. Cancelling the ORIGINAL runId must reach gen 1.
    const wf = defineWorkflow("can_cancel_e2e", (ctx, input: { n: number }) =>
      input.n === 0 ? ctx.continueAsNew({ n: 1 }) : ctx.await<string>("never"),
    );
    const worker = createWorker({
      nc,
      workflows: [wf],
      prefix: "e2e32",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e32" });
      const handle = await client.start(wf, { n: 0 });
      const gen1 = `${handle.runId}_g1`;
      await waitUntil(async () => (await client.get(gen1).status()).state === "suspended", 10_000);
      await client.cancel(handle.runId); // cancel the original (gen 0, already `continued`)
      await waitUntil(async () => (await client.get(gen1).status()).state === "cancelled", 10_000);
    } finally {
      await worker.stop();
    }
  });

  it("CANCEL: cascade recurses (grandparent → parent → child)", async () => {
    const child = defineWorkflow("gc_child_e2e", (ctx) => ctx.await<string>("never"));
    const parent = defineWorkflow("gc_parent_e2e", async (ctx) => {
      await ctx.call(child, undefined);
      return "p";
    });
    const grand = defineWorkflow("gc_grand_e2e", async (ctx) => {
      await ctx.call(parent, undefined);
      return "g";
    });
    const worker = createWorker({
      nc,
      workflows: [grand, parent, child],
      prefix: "e2e33",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e33" });
      const handle = await client.start(grand, {});
      const parentRun = `${handle.runId}_0`;
      const childRun = `${parentRun}_0`;
      await waitUntil(
        async () => (await client.get(childRun).status()).state === "suspended",
        10_000,
      );
      await client.cancel(handle.runId);
      // cascade must reach two levels down to the grandchild
      await waitUntil(
        async () => (await client.get(childRun).status()).state === "cancelled",
        10_000,
      );
      expect((await client.get(parentRun).status()).state).toBe("cancelled");
      expect((await handle.status()).state).toBe("cancelled");
    } finally {
      await worker.stop();
    }
  });

  it("CANCEL: cascade only cancels in-flight children (a finished child is untouched)", async () => {
    const fast = defineWorkflow("cc_fast_e2e", (_ctx, n: number) => Promise.resolve(n));
    const slow = defineWorkflow("cc_slow_e2e", (ctx) => ctx.await<string>("never"));
    const parent = defineWorkflow("cc_parent_e2e", async (ctx) => {
      await ctx.call(fast, 1); // completes
      await ctx.call(slow, undefined); // parks
      return "done";
    });
    const worker = createWorker({
      nc,
      workflows: [parent, fast, slow],
      prefix: "e2e34",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e34" });
      const handle = await client.start(parent, {});
      const fastRun = `${handle.runId}_0`;
      const slowRun = `${handle.runId}_1`;
      await waitUntil(
        async () => (await client.get(slowRun).status()).state === "suspended",
        10_000,
      );
      expect((await client.get(fastRun).status()).state).toBe("completed");
      await client.cancel(handle.runId);
      await waitUntil(
        async () => (await client.get(slowRun).status()).state === "cancelled",
        10_000,
      );
      expect((await client.get(fastRun).status()).state).toBe("completed"); // finished child untouched
    } finally {
      await worker.stop();
    }
  });

  it("CANCEL: once cancel is journaled, a resumed step does NOT re-fire and NO new child starts", async () => {
    // Cancel is a hard-stop at the next COMMAND boundary. Staged here: the parked op resolved
    // BEFORE the cancel (so replay proceeds past it), and a step sits begun-but-unfinished (a slow
    // incarnation mid-step). The cancel replay must halt AT that step: its fn must not re-fire, and
    // the ctx.call after it must never journal or start a child. (Under the old next-pending-op
    // semantics this exact staging executed the step AND started a child during the cancel replay —
    // the child-escape hole; the fresh-load cascade remains as belt-and-suspenders for journals
    // written by older workers.)
    const n = names("e2ecasc");
    await provision(nc, n, { streamMaxBytes: 64 * 1024 * 1024 });
    const journal = new NatsJournal(jetstream(nc), await jetstreamManager(nc), n, jsonCodec);
    const runId = `cascfresh-${Date.now()}`;

    const child = defineWorkflow("casc_child", async (ctx) => {
      await ctx.sleep("long", 120_000);
      return "woke";
    });
    const parentOf = (pause: () => Promise<string>) =>
      defineWorkflow("casc_parent", async (ctx) => {
        await ctx.await("gate");
        await ctx.run("pause", pause);
        await ctx.call(child, {});
        return "done";
      });

    // Stage the journal: start + an early gate signal, then an incarnation that hangs INSIDE the
    // "pause" step (a slow worker mid-step) — leaving step-begin journaled without step-end.
    await journal.append(runId, {
      entry: stamp({ kind: "start", seq: 0, wf: "casc_parent", wfVersion: 1, input: {} }, Date.now()), // prettier-ignore
      msgID: msgId.start(runId),
      expectLastSeq: 0,
    });
    let last = (await journal.load(runId)).at(-1)!.streamSeq;
    await journal.append(runId, {
      entry: stamp(
        { kind: "signal", seq: NON_COMMAND_SEQ, name: "gate", data: {}, signalId: "sig1" },
        Date.now(),
      ),
      msgID: msgId.signal(runId, "sig1"),
      expectLastSeq: last,
    });
    void executeRun(runId, {
      workflow: parentOf(() => new Promise<string>(() => {})), // hangs mid-step, abandoned
      journal,
      loaded: await journal.load(runId),
    });
    await waitUntil(
      async () =>
        (await journal.load(runId)).some(
          (l) => l.entry.kind === "step-begin" && l.entry.name === "pause",
        ),
      10_000,
    );

    // Cancellation is requested while the run sits mid-step (begin, no end).
    last = (await journal.load(runId)).at(-1)!.streamSeq;
    await journal.append(runId, {
      entry: stamp({ kind: "cancel-requested", seq: NON_COMMAND_SEQ }, Date.now()),
      msgID: msgId.cancel(runId),
      expectLastSeq: last,
    });

    // A worker now drives the cancel replay: gate resolves (signal < cancel), then the run must
    // hard-stop AT the resumed "pause" step — its fn never re-fires, no call is journaled, no child
    // process ever exists.
    let pauseRuns = 0;
    const worker = createWorker({
      nc,
      workflows: [
        parentOf(() => {
          pauseRuns += 1;
          return Promise.resolve("paused");
        }),
        child,
      ],
      prefix: "e2ecasc",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2ecasc" });
      await client.cancel(runId);
      await expect(client.get(runId).result(15_000)).rejects.toThrow(CancelledError);

      expect(pauseRuns).toBe(0); // the begun-but-unfinished step did NOT re-fire
      const loaded = await journal.load(runId);
      expect(loaded.some((l) => l.entry.kind === "call")).toBe(false); // no new child journaled
      expect(loaded.some((l) => l.entry.kind === "step-end" && l.entry.name === "pause")).toBe(
        false,
      ); // the step stays unfinished; the cancelled terminal ends the run
    } finally {
      await worker.stop();
    }
  });

  it("CANCEL: re-driving a cancelled terminal re-cascades to in-flight children", async () => {
    // Simulates a worker that appended the parent's cancelled terminal, then crashed BEFORE the
    // cascade. The redelivered wake takes the terminal short-circuit — which must re-cascade, or
    // the in-flight child keeps running unowned forever.
    const n = names("e2ecascrd");
    await provision(nc, n, { streamMaxBytes: 64 * 1024 * 1024 });
    const journal = new NatsJournal(jetstream(nc), await jetstreamManager(nc), n, jsonCodec);
    const child = defineWorkflow("cascrd_child", async (ctx) => {
      await ctx.sleep("long", 120_000);
      return "woke";
    });
    const parentWf = defineWorkflow("cascrd_parent", () => Promise.resolve("never-runs"));
    const worker = createWorker({
      nc,
      workflows: [parentWf, child],
      prefix: "e2ecascrd",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2ecascrd" });
      const parentRunId = `cascrd-${Date.now()}`;
      const childRunId = `${parentRunId}_0`;
      await client.start(child, {}, { runId: childRunId }); // the in-flight child, parked on its sleep
      await waitUntil(
        async () => (await client.get(childRunId).status()).state === "suspended",
        15_000,
      );

      // Parent journal as the crashed worker left it: started, child called, cancelled — no cascade.
      let last = 0;
      for (const [entry, mid] of [
        [stamp({ kind: "start", seq: 0, wf: "cascrd_parent", wfVersion: 1, input: {} }, Date.now()), msgId.start(parentRunId)], // prettier-ignore
        [stamp({ kind: "call", seq: 0, name: "cascrd_child", childRunId, wf: "cascrd_child", input: {} }, Date.now()), msgId.command(parentRunId, 0)], // prettier-ignore
        [stamp({ kind: "cancelled", seq: NON_COMMAND_SEQ }, Date.now()), msgId.terminal(parentRunId)], // prettier-ignore
      ] as const) {
        const ack = await journal.append(parentRunId, { entry, msgID: mid, expectLastSeq: last });
        last = ack.streamSeq;
      }

      // The late/redelivered wake for the finalized parent must re-drive the cascade.
      const js = jetstream(nc);
      await js.publish(
        n.workSubject("cascrd_parent"),
        new TextEncoder().encode(JSON.stringify({ runId: parentRunId, kind: "start" })),
      );
      await waitUntil(
        async () => (await client.get(childRunId).status()).state === "cancelled",
        15_000,
      );
    } finally {
      await worker.stop();
    }
  });

  it("CANCEL: forwards through a multi-generation continue-as-new chain", async () => {
    const wf = defineWorkflow("can3_e2e", (ctx, input: { n: number }) =>
      input.n < 2 ? ctx.continueAsNew({ n: input.n + 1 }) : ctx.await<string>("never"),
    );
    const worker = createWorker({
      nc,
      workflows: [wf],
      prefix: "e2e35",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e35" });
      const handle = await client.start(wf, { n: 0 });
      const gen2 = `${handle.runId}_g2`;
      await waitUntil(async () => (await client.get(gen2).status()).state === "suspended", 10_000);
      await client.cancel(handle.runId); // cancel gen 0 → must forward g0 → g1 → g2
      await waitUntil(async () => (await client.get(gen2).status()).state === "cancelled", 10_000);
    } finally {
      await worker.stop();
    }
  });

  it("PRAXIS: now/random/uuid are captured once and replay IDENTICALLY from the real journal", async () => {
    // The pure suite proves the engine; this proves the REAL journal round-trips a value-capture, so a
    // replay under a DIFFERENT clock still returns the recorded now/random/uuid (real storage + decode).
    const n = names("e2e37");
    await provision(nc, n, { streamMaxBytes: 64 * 1024 * 1024 });
    const journal = new NatsJournal(jetstream(nc), await jetstreamManager(nc), n, jsonCodec);
    const runId = `vc-${Date.now()}`;
    await journal.append(runId, {
      entry: stamp({ kind: "start", seq: 0, wf: "vcwf", wfVersion: 1, input: {} }, Date.now()),
      msgID: msgId.start(runId),
      expectLastSeq: 0,
    });
    const wf = defineWorkflow("vcwf", async (ctx) => ({
      t: await ctx.now(),
      r: await ctx.random(),
      id: await ctx.uuid(),
    }));
    // Incarnation 1: capture under clock = 1000, journaled to the real stream.
    const first = await executeRun(runId, {
      workflow: wf,
      journal,
      loaded: await journal.load(runId),
      clock: () => 1000,
    });
    expect(first.kind).toBe("completed");
    // Incarnation 2 (replay): a wildly different clock — the values MUST come from the journal.
    const second = await executeRun(runId, {
      workflow: wf,
      journal,
      loaded: await journal.load(runId),
      clock: () => 999_999,
    });
    expect(second).toEqual(first);
    const v = (first as { value: { t: number; r: number; id: string } }).value;
    expect(v.t).toBe(1000); // the recorded clock, NOT the replay clock 999_999
    expect(v.id).toMatch(/^[0-9a-f-]{36}$/i);
  });

  it("PRAXIS: a step RetryPolicy retries inline against the real server, then succeeds", async () => {
    let calls = 0;
    const wf = defineWorkflow("retry_e2e", (ctx) =>
      ctx.run(
        "flaky",
        () => {
          calls += 1;
          if (calls < 3) throw new Error(`transient ${calls}`);
          return `ok@${calls}`;
        },
        { retry: { kind: "fixed", attempts: 3, delayMs: 10 } },
      ),
    );
    const worker = createWorker({
      nc,
      workflows: [wf],
      prefix: "e2e38",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e38" });
      const handle = await client.start(wf, {});
      expect(await handle.result(15_000)).toBe("ok@3");
      expect(calls).toBe(3); // failed twice, succeeded on the 3rd — all within ONE delivery (inline retry)
    } finally {
      await worker.stop();
    }
  });

  it("PRAXIS: a signal sent BEFORE the await is reached is journaled and consumed on arrival", async () => {
    // The await isn't reached for ~800ms (a durable sleep barrier); the signal is sent ~100ms in, so it
    // lands while the run is parked on the sleep. When the await is finally reached, the already-journaled
    // signal resolves it immediately — proving an early signal is not lost (real signalId dedup + match).
    const wf = defineWorkflow("earlysig_e2e", async (ctx) => {
      await ctx.sleep("barrier", 800);
      return ctx.await<string>("go");
    });
    const worker = createWorker({
      nc,
      workflows: [wf],
      prefix: "e2e39",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e39" });
      const handle = await client.start(wf, {});
      await new Promise((r) => setTimeout(r, 100)); // run is parked on the 800ms barrier...
      await client.signal(handle.runId, "go", "EARLY"); // ...signal lands well before the await
      expect(await handle.result(15_000)).toBe("EARLY");
    } finally {
      await worker.stop();
    }
  });

  it("PRAXIS: ctx.race where a real signal beats the timer resolves to the signal", async () => {
    // Complements the existing race-vs-timer (timer wins): here a real signal at ~300ms beats a 3s timer.
    const wf = defineWorkflow("racesig_e2e", async (ctx) => {
      const r = await ctx.race([ctx.startAwait<string>("go"), ctx.startSleep("deadline", 3000)]);
      return typeof r === "string" ? `got:${r}` : "timed-out";
    });
    const worker = createWorker({
      nc,
      workflows: [wf],
      prefix: "e2e40",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e40" });
      const handle = await client.start(wf, {});
      await new Promise((r) => setTimeout(r, 300)); // let the run park on the race (both branches armed)
      await client.signal(handle.runId, "go", "SIG");
      expect(await handle.result(15_000)).toBe("got:SIG");
    } finally {
      await worker.stop();
    }
  });

  it("ctx.call with opts.runId overrides the child's runId end-to-end", async () => {
    const child = defineWorkflow("ridchild_e2e", (_c, n: number) => Promise.resolve(n * 2));
    const customId = `custom-child-${Date.now()}`;
    const parent = defineWorkflow("ridparent_e2e", (ctx) =>
      ctx.call(child, 21, { runId: customId }),
    );
    const worker = createWorker({
      nc,
      workflows: [parent, child],
      prefix: "e2e41",
      streamMaxBytes: 64 * 1024 * 1024,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2e41" });
      const handle = await client.start(parent, {});
      expect(await handle.result(15_000)).toBe(42);
      // the child ran under the OVERRIDDEN runId (not the default `${parentRunId}_0`)
      expect((await client.get(customId).status()).state).toBe("completed");
    } finally {
      await worker.stop();
    }
  });
});
