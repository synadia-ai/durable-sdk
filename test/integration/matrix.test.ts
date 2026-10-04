// The feature matrix crossings that no single-feature suite reaches.
//
// Each feature has its own file, and each is well covered ON ITS OWN. What none of them cover is the
// feature under CONCURRENCY: a versioned run driven by two workers at once, a rollover raced at the
// boundary, a cancel arriving while another worker is mid-step, an offloaded payload under a signal
// storm. Those are exactly the situations the engine's one primitive exists for, and testing a
// feature only in isolation is how a bug survives — the 0.6.1 lost-event bug and this session's
// continue-as-new adopt bug were both invisible until two workers touched the same run.
//
// One prefix, sized small: a prefix reserves its max_bytes across six streams (test/harness).

import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { connect, type NatsConnection } from "@nats-io/transport-node";
import { afterAll, beforeAll, describe, expect, inject, it } from "vitest";
import { defineWorkflow } from "../../src/core/workflow.js";
import { encodeJson } from "../../src/internal/json.js";
import { type DurableEventFields, type DurableEventName } from "../../src/internal/metrics.js";
import { jsonCodec } from "../../src/journal/codec.js";
import { countOf, createClient, engineCounts } from "../harness/engine.js";
import { NatsJournal } from "../harness/shell/journal-store.js";
import { names } from "../harness/shell/names.js";
import { type WorkItem } from "../../src/core/work-item.js";
import { createWorker } from "../harness/engine.js";
import { over } from "../harness/payload.js";

const natsUrl = inject("natsUrl");
const MIB = 1024 * 1024;
const MX = "mtrx";
// Sized to hold what the offload crossing writes: two workers rewriting a 2 × max_payload blob.
const SIZED = { streamMaxBytes: 128 * MIB };

/**
 * Counts how many times each run was DRIVEN, across every worker sharing this sink.
 *
 * Without it these tests could pass having never raced: if a storm's items were all consumed before
 * the run parked, or one worker took every one, the assertions would hold for a plain sequential run
 * and prove nothing about contention. Mirrors the check the chaos suite already makes.
 *
 * durable-sdk: re-expressed through the engine (`info`), so the four `drives.piledUp` assertions
 * now read the engine's count. The library's workers drove every item of a storm, the ones that
 * arrived after the run ended included (a terminal re-drive each). The engine hands a run's items
 * to polls one at a time and acks an ended run's items itself, so most of a storm never reaches a
 * worker and `work.done` per run stays below 3 however hard the storm piled up. What piles up is
 * the engine's `de_engine_items_pulled_total` for the workflow: "driven ≥ 3" per run becomes "≥ 3
 * items pulled per run of the workflow". `perRun` (the workers' own count) stays, for the message.
 */
class Drives {
  readonly perRun = new Map<string, number>();
  private pulled = 0;
  constructor(
    private readonly nc: NatsConnection,
    private readonly workflow: string,
    private readonly runs = 1,
  ) {}
  readonly onEvent = (name: DurableEventName, fields: DurableEventFields): void => {
    if (name !== "work.done") return;
    const id = String(fields.runId);
    this.perRun.set(id, (this.perRun.get(id) ?? 0) + 1);
  };
  private async refresh(): Promise<boolean> {
    this.pulled = countOf(await engineCounts(this.nc, MX), "de_engine_items_pulled_total", { workflow: this.workflow }); // prettier-ignore
    return this.pulled >= 3 * this.runs;
  }
  /** Formatted so a failure prints the actual counts instead of just "false". */
  piledUp(runId: string): string {
    return `${runId} driven>=3: ${String(this.pulled >= 3 * this.runs)} of ${JSON.stringify({ pulled: this.pulled, runs: this.runs, workers: Object.fromEntries(this.perRun) })}`; // prettier-ignore
  }
  expected(runId: string): string {
    return `${runId} driven>=3: true of ${JSON.stringify({ pulled: this.pulled, runs: this.runs, workers: Object.fromEntries(this.perRun) })}`; // prettier-ignore
  }
  /** Wait for the storm to be PROCESSED (bounded: a storm that never piles up fails, with the counts). */
  async settled(runId: string): Promise<void> {
    await waitUntil(() => this.refresh(), 10_000, `the storm on ${runId} to be processed (${this.piledUp(runId)})`); // prettier-ignore
  }
}

async function waitUntil(cond: () => boolean | Promise<boolean>, timeoutMs: number, what: string) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await cond()) return;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 50));
  }
}

/** A run's journal, raw — refs and transforms unresolved, so the audit sees what is really stored. */
async function raw(nc: NatsConnection, runId: string) {
  return new NatsJournal(jetstream(nc), await jetstreamManager(nc), names(MX), jsonCodec).loadRaw(
    runId,
  );
}

/** The invariants that must hold whatever raced: one terminal, no command position written twice. */
async function assertJournalSane(nc: NatsConnection, runId: string): Promise<void> {
  const entries = (await raw(nc, runId)).map((l) => l.entry);
  const terminals = entries.filter((e) =>
    ["result", "fail", "cancelled", "continued"].includes(e.kind),
  );
  expect(terminals).toHaveLength(1);
  const commands = new Map<string, number>();
  for (const e of entries) {
    if (["step-end", "now", "random", "uuid", "sleep", "await", "call"].includes(e.kind)) {
      const key = `${e.kind}:${String(e.seq)}`;
      commands.set(key, (commands.get(key) ?? 0) + 1);
    }
  }
  expect([...commands].filter(([, n]) => n > 1)).toEqual([]);
}

describe.skipIf(!natsUrl)("feature × concurrency crossings", () => {
  let nc: NatsConnection;
  beforeAll(async () => {
    nc = await connect({ servers: natsUrl! });
  });
  afterAll(async () => {
    await nc.drain();
  });

  it("VERSIONING × fence: two workers racing ONE pinned run keep it on its own version", async () => {
    // The mixed-fleet test has a v1-only worker PARKING a v2 run — the workers never contend. Here
    // both can run it and both are woken repeatedly, so the version-pinned dispatch has to hold
    // under an actual race rather than under an orderly handoff. A v2 body that ran on v1's code
    // would still produce a plausible result; the journal audit is what would catch it.
    const executions: string[] = [];
    const v1 = defineWorkflow(
      "mx_ver",
      async (ctx) => {
        await ctx.await("go");
        return ctx.run("s", () => {
          executions.push("v1");
          return "v1";
        });
      },
      { version: 1 },
    );
    const v2 = defineWorkflow(
      "mx_ver",
      async (ctx) => {
        await ctx.await("go");
        return ctx.run("s", () => {
          executions.push("v2");
          return "v2";
        });
      },
      { version: 2 },
    );
    const drives = new Drives(nc, "mx_ver", 2); // durable-sdk: the engine's count, two runs of mx_ver
    const opts = { nc, workflows: [v1, v2], prefix: MX, lease: false, metrics: drives, ...SIZED };
    const a = createWorker(opts);
    const b = createWorker(opts);
    await a.start();
    await b.start();
    try {
      const client = createClient({ nc, prefix: MX, ...SIZED });
      const h1 = await client.start(v1, {});
      const h2 = await client.start(v2, {});
      await new Promise((r) => setTimeout(r, 400));
      await Promise.all(
        [h1, h2].flatMap((h) => Array.from({ length: 4 }, () => client.signal(h.runId, "go", {}))),
      );

      expect(await h1.result(30_000)).toBe("v1");
      expect(await h2.result(30_000)).toBe("v2");
      expect((await h1.status()).version).toBe(1);
      expect((await h2.status()).version).toBe(2);
      // Neither run was ever executed by the other version's code, however the wakes interleaved.
      expect(executions.filter((e) => e === "v1").length).toBeGreaterThanOrEqual(1);
      await assertJournalSane(nc, h1.runId);
      await assertJournalSane(nc, h2.runId);
      for (const h of [h1, h2]) {
        await drives.settled(h.runId);
        expect(drives.piledUp(h.runId)).toBe(drives.expected(h.runId));
      }
    } finally {
      await a.stop();
      await b.stop();
    }
  }, 90_000);

  it("CONTINUE-AS-NEW × crash: a rollover interrupted before the next generation starts is repaired", async () => {
    // The crash window peculiar to a rollover: the `continued` terminal is journaled, and then the
    // process dies before `handleContinue` starts generation n+1. Nothing is left to drive the
    // chain — no work item exists for a run that was never started — so the repair has to come from
    // re-driving the terminal generation. If it did not, the chain would simply stop, with a
    // healthy-looking `continued` at the end and no error anywhere.
    const seen: number[] = [];
    const wf = defineWorkflow("mx_can_crash", async (ctx, input: { n: number }) => {
      seen.push(input.n);
      await ctx.run(`work${String(input.n)}`, () => input.n);
      if (input.n < 1) await ctx.continueAsNew({ n: input.n + 1 });
      return `done:${String(input.n)}`;
    });
    const worker = createWorker({ nc, workflows: [wf], prefix: MX, ...SIZED });
    await worker.start();
    const client = createClient({ nc, prefix: MX, ...SIZED });
    const h = await client.start(wf, { n: 0 });
    expect(await h.result(25_000)).toBe("done:1");
    await worker.stop();

    // Sabotage: the next generation exists, so re-drive gen 0 the way a redelivery after a crash
    // would. The repair must be IDEMPOTENT — it must not start a third generation or re-run gen 1.
    const before = seen.length;
    // durable-sdk: the re-drive is the engine's now; the baseline of its count, before the item.
    const redrives = async (): Promise<number> =>
      countOf(await engineCounts(nc, MX), "de_engine_acks_total", { outcome: "terminal-redrive" });
    const redrivesBefore = await redrives();
    const item: WorkItem = { runId: h.runId, kind: "resume" };
    await jetstream(nc).publish(names(MX).workSubject("mx_can_crash"), encodeJson(item), {
      msgID: `${h.runId}:mx-redrive`,
    });

    const outcomes: string[] = [];
    const w2 = createWorker({
      nc,
      workflows: [wf],
      prefix: MX,
      ...SIZED,
      metrics: {
        onEvent: (n: DurableEventName, f: DurableEventFields) => {
          if (n === "work.done" && f.runId === h.runId) outcomes.push(String(f.outcome));
        },
      },
    });
    await w2.start();
    try {
      // durable-sdk: re-expressed through the engine (`info`). The library's worker reported the
      // re-drive as its own `work.done` outcome `terminal-redrive`; the engine re-drives a terminal
      // run itself and acks the item without a poll, so it never reaches w2 (`outcomes` stays
      // empty). Waiting on the engine's `de_engine_acks_total{outcome="terminal-redrive"}` instead.
      await waitUntil(async () => (await redrives()) > redrivesBefore || outcomes.includes("terminal-redrive"), 20_000, "the re-drive"); // prettier-ignore
      expect((await h.status()).state).toBe("continued");
      expect((await client.get(`${h.runId}_g1`).status()).state).toBe("completed");
      expect(seen.length).toBe(before); // nothing re-executed
      await assertJournalSane(nc, h.runId);
      await assertJournalSane(nc, `${h.runId}_g1`);
    } finally {
      await w2.stop();
    }
  }, 90_000);

  it("CONTINUE-AS-NEW × multi-worker: a raced rollover starts ONE next generation", async () => {
    // The integration twin of the adopt unit test. Two workers drive the same run through a
    // rollover under a wake storm; whichever loses the terminal CAS must adopt the journaled spec,
    // so exactly one `continued` exists and exactly one generation 1 is started. Before the adopt
    // fix the loser handed its own spec to `handleContinue`, and the next generation's input was
    // decided by whoever won a second, unordered race.
    const wf = defineWorkflow("mx_can_race", async (ctx, input: { n: number }) => {
      await ctx.await("go");
      if (input.n < 1) await ctx.continueAsNew({ n: input.n + 1 });
      return `end:${String(input.n)}`;
    });
    const drives = new Drives(nc, "mx_can_race"); // durable-sdk: the engine's count
    const opts = { nc, workflows: [wf], prefix: MX, lease: false, metrics: drives, ...SIZED };
    const a = createWorker(opts);
    const b = createWorker(opts);
    await a.start();
    await b.start();
    try {
      const client = createClient({ nc, prefix: MX, ...SIZED });
      const h = await client.start(wf, { n: 0 });
      await new Promise((r) => setTimeout(r, 400));
      await Promise.all(Array.from({ length: 6 }, () => client.signal(h.runId, "go", {})));

      await waitUntil(
        async () => (await client.get(`${h.runId}_g1`).status()).state !== "pending",
        30_000,
        "generation 1 to exist",
      );
      // Generation 1 parks on its own `go`, so nudge it and let the chain finish.
      await Promise.all(Array.from({ length: 3 }, () => client.signal(`${h.runId}_g1`, "go", {})));
      expect(await client.get(`${h.runId}_g1`).result(30_000)).toBe("end:1");

      const gen0 = (await raw(nc, h.runId)).map((l) => l.entry);
      expect(gen0.filter((e) => e.kind === "continued")).toHaveLength(1);
      expect((await client.get(`${h.runId}_g2`).status()).state).toBe("pending"); // no third gen
      await assertJournalSane(nc, h.runId);
      await assertJournalSane(nc, `${h.runId}_g1`);
      await drives.settled(h.runId);
      expect(drives.piledUp(h.runId)).toBe(drives.expected(h.runId));
    } finally {
      await a.stop();
      await b.stop();
    }
  }, 120_000);

  // durable-sdk: docs/findings.md F1, answered by the engine's F25 (put and get) and the SDK's
  // chunked transport (src/engine/blobs.ts).
  it("OFFLOAD × multi-worker: a storm over blob-backed payloads journals each step once", async () => {
    // Offload is proven against crash, fence and redelivery, but never under contention. What is
    // specific here: two workers re-running the same step both WRITE a blob, and the names are
    // content-addressed — so identical values converge on one object and the loser's write is
    // harmless. A design that deleted a fenced writer's blob would corrupt the winner's entry.
    const BIG = over(nc, 2); // twice this server's max_payload: it cannot ride inline
    const wf = defineWorkflow("mx_blob", async (ctx) => {
      await ctx.await("go");
      const big = await ctx.run("render", () => "z".repeat(BIG));
      return big.length;
    });
    const drives = new Drives(nc, "mx_blob"); // durable-sdk: the engine's count
    const opts = { nc, workflows: [wf], prefix: MX, lease: false, metrics: drives, ...SIZED };
    const a = createWorker(opts);
    const b = createWorker(opts);
    await a.start();
    await b.start();
    try {
      const client = createClient({ nc, prefix: MX, ...SIZED });
      const h = await client.start(wf, {});
      await new Promise((r) => setTimeout(r, 400));
      await Promise.all(Array.from({ length: 5 }, () => client.signal(h.runId, "go", {})));

      expect(await h.result(45_000)).toBe(BIG);
      await assertJournalSane(nc, h.runId);
      // The value really did ride as a ref — otherwise this proved nothing about offload.
      const stepEnd = (await raw(nc, h.runId)).find((l) => l.entry.kind === "step-end")?.entry as
        { valueRef?: { name: string } } | undefined;
      expect(stepEnd?.valueRef?.name).toMatch(new RegExp(`^${h.runId}/sha256:`));
      await drives.settled(h.runId);
      expect(drives.piledUp(h.runId)).toBe(drives.expected(h.runId));
    } finally {
      await a.stop();
      await b.stop();
    }
  }, 120_000);

  it("CANCELLATION × multi-worker: a cancel racing two drivers still lands exactly one terminal", async () => {
    // A cancel is deliberately UNGATED by the lease so the kill switch always works, which means it
    // races execution by design. Under two workers the run can be mid-step on one while the other
    // records the cancel — and the only acceptable outcomes are a single `cancelled` terminal or a
    // single `result` (a cancel that arrives after completion is a no-op). What must never happen
    // is two terminals, or a run that ends up with neither.
    const wf = defineWorkflow("mx_cancel", async (ctx) => {
      await ctx.await("go");
      await ctx.run("slow", () => new Promise((r) => setTimeout(() => r("a"), 400)));
      await ctx.sleep("gap", 300);
      await ctx.run("after", () => "b");
      return "finished";
    });
    const drives = new Drives(nc, "mx_cancel"); // durable-sdk: the engine's count
    const opts = { nc, workflows: [wf], prefix: MX, lease: false, metrics: drives, ...SIZED };
    const a = createWorker(opts);
    const b = createWorker(opts);
    await a.start();
    await b.start();
    try {
      const client = createClient({ nc, prefix: MX, ...SIZED });
      const h = await client.start(wf, {});
      await new Promise((r) => setTimeout(r, 400));
      // Wake it from both sides and cancel in the middle of the storm.
      await Promise.all([
        ...Array.from({ length: 3 }, () => client.signal(h.runId, "go", {})),
        client.cancel(h.runId, { reason: "raced" }),
        client.cancel(h.runId, { reason: "raced again" }),
      ]);

      await waitUntil(
        async () => ["cancelled", "completed"].includes((await h.status()).state),
        30_000,
        "a terminal state",
      );
      await assertJournalSane(nc, h.runId); // exactly one terminal, whichever it is
      const kinds = (await raw(nc, h.runId)).map((l) => l.entry.kind);
      expect(kinds.filter((k) => k === "cancelled").length).toBeLessThanOrEqual(1);
      expect(kinds.filter((k) => k === "cancel-requested")).toHaveLength(1); // double-cancel collapses
      await drives.settled(h.runId);
      expect(drives.piledUp(h.runId)).toBe(drives.expected(h.runId));
    } finally {
      await a.stop();
      await b.stop();
    }
  }, 90_000);
});
