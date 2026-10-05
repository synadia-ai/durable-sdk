// The worker's recovery paths, against a real nats-server.
//
// `worker.ts` is the least-covered file in the tree, and what is dark in it is not a random slice —
// it is almost entirely the code that runs when something has ALREADY gone wrong: a poison item, a
// work item for a run that no longer exists, an event append that keeps losing its CAS, a crash
// between the journal terminal and the KV finalize, a broken timers stream, a lease taken away
// mid-drive. Every one of those is invisible while the system is healthy, which is exactly when a
// bug in it costs the most.
//
// The assertions here are on the ACK DECISION (PROTOCOL §6.4) rather than on a workflow's return
// value: ack vs nak-with-backoff vs term is what decides whether a run is retried forever, dropped,
// or quietly poisons a consumer, and several different ack choices can produce the same result().

import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import { type NatsConnection } from "@nats-io/transport-node";
import { connect } from "../harness/existing.js";
import { afterAll, beforeAll, describe, expect, inject, it } from "vitest";
import { defineWorkflow } from "../../src/core/workflow.js";
import { encodeJson } from "../../src/internal/json.js";
import { type DurableEventFields, type DurableEventName } from "../../src/internal/metrics.js";
import { jsonCodec } from "../../src/journal/codec.js";
import {
  countOf,
  createClient,
  engineCounts,
  engineLog,
  restartEngine,
} from "../harness/engine.js";
import { NatsJournal } from "../harness/shell/journal-store.js";
import { names, skipAsTenant } from "../harness/shell/names.js";
import { type WorkItem } from "../../src/core/work-item.js";
import { createWorker } from "../harness/engine.js";
import { over } from "../harness/payload.js";
import { DELETES_STREAMS, describeUnlessExisting, WAN } from "../harness/mode.js";

const natsUrl = inject("natsUrl");
// durable-sdk: deleting a stream of the prefix is not for a server the run does not own (harness/mode.ts).
const brokenTimers = describeUnlessExisting(DELETES_STREAMS, !!natsUrl);
const MIB = 1024 * 1024;

// Two prefixes: one shared by the well-behaved tests, one for the test that DELETES a stream out
// from under a running worker. Each is sized small on purpose — a prefix reserves its max_bytes
// across six streams against the harness's budget (see test/harness/nats-server.ts).
const REC = "wrec";
const BROKEN = "wrecx";
// Sized to hold the oversized failure message twice over (the child's entry and the parent's copy).
const SIZED = { streamMaxBytes: 128 * MIB };

class Sink {
  readonly events: Array<{ name: DurableEventName; fields: DurableEventFields }> = [];
  readonly onEvent = (name: DurableEventName, fields: DurableEventFields): void => {
    this.events.push({ name, fields });
  };
  outcomes(runId?: string): string[] {
    return this.events
      .filter((e) => e.name === "work.done" && (!runId || e.fields.runId === runId))
      .map((e) => String(e.fields.outcome));
  }
}

class Logs {
  readonly warns: Array<{ msg: string; fields?: Record<string, unknown> }> = [];
  readonly debug = (): void => {};
  readonly info = (): void => {};
  readonly warn = (msg: string, fields?: Record<string, unknown>): void => {
    this.warns.push({ msg, ...(fields ? { fields } : {}) });
  };
  readonly error = (msg: string, fields?: Record<string, unknown>): void => {
    this.warns.push({ msg, ...(fields ? { fields } : {}) });
  };
  saw(fragment: string): boolean {
    return this.warns.some((w) => w.msg.includes(fragment));
  }
}

async function waitUntil(cond: () => boolean | Promise<boolean>, timeoutMs: number, what: string) {
  timeoutMs *= WAN; // durable-sdk: × the round-trip factor (harness/mode.ts, F29)
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await cond()) return;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 50));
  }
}

/**
 * durable-sdk: the engine's ack decisions on `prefix` by outcome label, as a list (each label
 * repeated by its count), for the assertions that read the library worker's `work.done`
 * outcomes. A terminal re-drive and a missing journal are decided in the engine now and never
 * reach a worker; the engine counts them in `de_engine_acks_total{outcome, ack}` (`info`).
 * Order is not observable through counts.
 */
async function engineOutcomes(
  nc: NatsConnection,
  prefix: string,
  labels: string[],
): Promise<string[]> {
  const counts = await engineCounts(nc, prefix);
  return labels.flatMap((l) => Array.from({ length: countOf(counts, "de_engine_acks_total", { outcome: l }) }, () => l)); // prettier-ignore
}

describe.skipIf(!natsUrl)("worker recovery paths (real nats-server)", () => {
  let nc: NatsConnection;
  beforeAll(async () => {
    nc = await connect({ servers: natsUrl! });
  });
  afterAll(async () => {
    await nc.drain();
  });

  // prettier-ignore
  it("an UNDECODABLE work item is TERMed — a poison message cannot block a consumer forever", async () => {
    // DE_WORK is a WorkQueue stream with `max_deliver: -1`, so a NAK'd item comes back forever.
    // Bytes that are not JSON can never become decodable, so redelivering one is an infinite loop
    // at the head of a workflow's queue — every other run of that workflow stops. `term` is the
    // only correct answer, and on a workqueue stream it also removes the message.
    const wf = defineWorkflow("rec_poison", async (ctx) => ctx.run("s", () => "ok"));
    const worker = createWorker({ nc, workflows: [wf], prefix: REC, ...SIZED });
    await worker.start();
    try {
      const n = names(REC);
      const jsm = await jetstreamManager(nc);
      await jetstream(nc).publish(n.workSubject("rec_poison"), new TextEncoder().encode("{ not"));

      // Termed → gone from the stream. A NAK-loop would leave it pending indefinitely instead.
      await waitUntil(
        async () => (await jsm.streams.info(n.workStream)).state.messages === 0,
        15_000,
        "the poison item to be dropped",
      );

      // …and the pump is still serving: the very next run goes through.
      const client = createClient({ nc, prefix: REC, ...SIZED });
      const h = await client.start(wf, {});
      expect(await h.result(20_000)).toBe("ok");
    } finally {
      await worker.stop();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // prettier-ignore
  it("a work item for a run with NO journal is retried, then dropped — never looped on forever", async () => {
    // A forged item, or one whose journal was GC'd. Dropping immediately would strand a run whose
    // journal was merely slow to read (a fired timer wake is one-shot and would be gone), so the
    // worker retries with backoff first and only terms once the journal stays missing.
    const sink = new Sink();
    const logs = new Logs();
    const wf = defineWorkflow("rec_ghost", async (ctx) => ctx.run("s", () => 1));
    const worker = createWorker({
      nc,
      workflows: [wf],
      prefix: REC,
      ...SIZED,
      metrics: sink,
      logger: logs,
    });
    await worker.start();
    try {
      const item: WorkItem = { runId: "rec-ghost-nobody", kind: "resume" };
      await jetstream(nc).publish(names(REC).workSubject("rec_ghost"), encodeJson(item), {
        msgID: "ghost-1",
      });

      // durable-sdk: re-expressed through the engine. The no-journal policy (nak with backoff,
      // term on the fifth delivery) is the engine's, and the item never reaches the worker: the
      // outcomes are the engine's ack counts (`engineOutcomes`, retries listed before the drop,
      // which the engine guarantees by counting deliveries) and the log line is the engine's.
      const ghost = (): Promise<string[]> => engineOutcomes(nc, REC, ["retry-no-journal", "dropped-no-journal"]); // prettier-ignore
      await waitUntil(
        async () => (await ghost()).includes("dropped-no-journal"),
        25_000,
        "the ghost item to be dropped",
      );
      // The retries came FIRST — dropping on the first delivery is the bug this guards.
      const outcomes = await ghost();
      expect(outcomes.filter((o) => o === "retry-no-journal").length).toBeGreaterThanOrEqual(4);
      expect(outcomes.at(-1)).toBe("dropped-no-journal");
      expect((await engineLog(REC)).some((l) => l.includes("no journal for this run after retries"))).toBe(true); // prettier-ignore
      expect(sink.outcomes("rec-ghost-nobody")).toEqual([]); // never handed to the worker
      void logs;
    } finally {
      await worker.stop();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // durable-sdk: "an event append that keeps losing its CAS NAKs — the event is never acked away"
  // is dropped. It injected the fences by patching NatsJournal.prototype.append inside the worker;
  // recording the signal is the engine's now (journaled before the poll hands the item out, nak'd
  // when every attempt is fenced: durable-engine core.RecordInbound, engine.TestRecordInbound*),
  // and no SDK request carries the signal entry.
  // prettier-ignore
  it("a terminal RE-DRIVE repairs a status hint the crash window left stale", async () => {
    // A crash between the journal terminal and the KV finalize leaves status() reporting
    // running/suspended forever while result() (journal truth) already resolves. The re-drive
    // branch exists to repair exactly that, and it has to repair the FAILED case too — the arm that
    // is easy to leave out, because the happy path is the one anybody tests by hand.
    const sink = new Sink();
    const wf = defineWorkflow("rec_redrive", async (ctx) => {
      await ctx.run("s", () => 1);
      throw new Error("declined");
    });
    const worker = createWorker({
      nc,
      workflows: [wf],
      prefix: REC,
      ...SIZED,
      metrics: sink,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: REC, ...SIZED });
      const h = await client.start(wf, {});
      await expect(h.result(20_000)).rejects.toMatchObject({ name: "WorkflowFailure" });
      expect((await h.status()).state).toBe("failed");

      // Simulate the crash window: put the hint back to `running` behind the engine's back.
      const kv = await new Kvm(nc).open(names(REC).runsKv);
      await kv.put(h.runId, encodeJson({ status: "running", wf: "rec_redrive", version: 1 }));
      expect((await h.status()).state).toBe("running"); // the stale state a user would see

      const redrives = (await engineOutcomes(nc, REC, ["terminal-redrive"])).length;
      const item: WorkItem = { runId: h.runId, kind: "resume" };
      await jetstream(nc).publish(names(REC).workSubject("rec_redrive"), encodeJson(item), {
        msgID: `${h.runId}:redrive`,
      });

      // durable-sdk: re-expressed through the engine — the re-drive is the engine's, acked
      // without a poll; its count, not the worker's `work.done`, says it happened.
      await waitUntil(
        async () => sink.outcomes(h.runId).includes("terminal-redrive") || (await engineOutcomes(nc, REC, ["terminal-redrive"])).length > redrives, // prettier-ignore
        20_000,
        "the re-drive",
      );
      await waitUntil(async () => (await h.status()).state === "failed", 10_000, "the repair");
    } finally {
      await worker.stop();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // prettier-ignore
  it("re-driving an already-CANCELLED child re-notifies its parent, idempotently", async () => {
    // The other re-drive arm: a crash could have written the child's `cancelled` terminal and died
    // before telling the parent, which would park the parent forever on a child that is gone. The
    // re-drive must re-send the outcome — and must be safe to do twice, since the parent may
    // already have it.
    const sink = new Sink();
    const child = defineWorkflow("rec_kid", async (ctx) => {
      await ctx.await("never");
      return "unreachable";
    });
    const parent = defineWorkflow("rec_dad", async (ctx) => {
      try {
        await ctx.call(child, {});
        return "child-ok";
      } catch (e) {
        // A cancelled child reaches its parent as a child-done carrying a CancelledError shape,
        // which `ctx.call` surfaces the way it surfaces any child failure: a WorkflowFailure whose
        // message names the underlying error. The parent is NOT itself cancelled.
        const err = e as Error;
        return `${err.name}/${err.message.includes("CancelledError") ? "cancelled" : "other"}`;
      }
    });
    const worker = createWorker({
      nc,
      workflows: [parent, child],
      prefix: REC,
      ...SIZED,
      metrics: sink,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: REC, ...SIZED });
      const h = await client.start(parent, {});
      const childId = `${h.runId}_0`;
      await waitUntil(async () => (await client.get(childId).status()).state === "suspended", 20_000, "the child to park"); // prettier-ignore

      await client.cancel(childId, { reason: "operator killed the child" });
      expect(await h.result(25_000)).toBe("WorkflowFailure/cancelled");

      // Now re-drive the finished child. The parent is already terminal, so the re-notification
      // must be a harmless no-op rather than a second resolve or a crash.
      const redrives = (await engineOutcomes(nc, REC, ["terminal-redrive"])).length;
      const item: WorkItem = { runId: childId, kind: "resume" };
      await jetstream(nc).publish(names(REC).workSubject("rec_kid"), encodeJson(item), {
        msgID: `${childId}:redrive`,
      });
      // durable-sdk: re-expressed through the engine (its re-drive count), as above.
      await waitUntil(
        async () => sink.outcomes(childId).includes("terminal-redrive") || (await engineOutcomes(nc, REC, ["terminal-redrive"])).length > redrives, // prettier-ignore
        20_000,
        "the child re-drive",
      );

      expect((await client.get(childId).status()).state).toBe("cancelled");
      expect(await h.result(5_000)).toBe("WorkflowFailure/cancelled"); // unchanged
      const raw = await new NatsJournal(
        jetstream(nc),
        await jetstreamManager(nc),
        names(REC),
        jsonCodec,
      ).loadRaw(h.runId);
      expect(raw.filter((l) => l.entry.kind === "resolve")).toHaveLength(1); // no second resolve
    } finally {
      await worker.stop();
    }
  }, 90_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // durable-sdk: docs/findings.md F8, answered by the engine's F21: the gating run lease is the
  // engine's, so the "lease lost" line is the engine's log, read below.
  // prettier-ignore
  it("the run lease being taken away mid-drive is survivable — it is advisory, not correctness", async () => {
    // The lease narrows the at-least-once window; it never carries correctness (the journal CAS
    // does). So when a renewal loses its CAS and the key now names somebody else, the right
    // behaviour is to notice, stop renewing, and CARRY ON — not to abort a run that is perfectly
    // safe to finish.
    const logs = new Logs();
    const wf = defineWorkflow("rec_lease", async (ctx) => {
      const a = await ctx.run("slow", async () => {
        await new Promise((r) => setTimeout(r, 2_000));
        return "done";
      });
      return a;
    });
    const worker = createWorker({
      nc,
      workflows: [wf],
      prefix: REC,
      ...SIZED,
      leaseTtlMs: 1_000, // renews ~3x/second, so the theft is noticed inside the slow step
      logger: logs,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: REC, ...SIZED });
      const h = await client.start(wf, {});

      // Steal the lease out from under the running worker: a different owner, written directly.
      const kv = await new Kvm(nc).open(names(REC).leasesKv);
      await waitUntil(async () => (await kv.get(h.runId)) !== null, 15_000, "the lease to appear");
      await kv.put(
        h.runId,
        encodeJson({ owner: "some-other-worker", deadlineMs: Date.now() + 60_000 }),
      );

      // The run still finishes — losing an advisory lease must never lose work.
      expect(await h.result(30_000)).toBe("done");
      // durable-sdk: the engine's log (`run lease lost mid-drive; continuing under the journal CAS`).
      expect(logs.saw("run lease lost mid-drive") || (await engineLog(REC)).some((l) => l.includes("run lease lost mid-drive"))).toBe(true); // prettier-ignore
    } finally {
      await worker.stop();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)
});

brokenTimers("worker recovery — a broken timers stream", () => {
  let nc: NatsConnection;
  beforeAll(async () => {
    nc = await connect({ servers: natsUrl! });
  });
  afterAll(async () => {
    await nc.drain();
  });

  // durable-sdk: docs/findings.md F7, answered by the engine's F19 (the work durable is bound on
  // its own, so polls keep being served with DE_TIMERS gone).
  // prettier-ignore
  it("the maintenance pass and the pumps survive DE_TIMERS disappearing, and heal on restart", async () => {
    // Its own prefix, because this test destroys a stream. The scenario is real: a server upgraded
    // across nats-server 2.14.2 / 2.12.10 does not bring DE_TIMERS back after a restart
    // (LEARNINGS #40), so the worker must keep serving — loudly — rather than dying in a background
    // interval where nobody would see it.
    const logs = new Logs();
    const wf = defineWorkflow("rec_timers", async (ctx) => ctx.run("s", () => "ok"));
    const worker = createWorker({
      nc,
      workflows: [wf],
      prefix: BROKEN,
      ...SIZED,
      maintenanceIntervalMs: 1_000,
      logger: logs,
    });
    await worker.start();
    try {
      const jsm = await jetstreamManager(nc);
      await jsm.streams.delete(names(BROKEN).timersStream);

      // Both halves of the hygiene pass fail against a stream that is not there, and both must be
      // caught and retried next tick rather than escaping into an unhandled rejection.
      // durable-sdk: re-expressed through the engine — the maintenance pass is the engine's, and
      // so is its log (`maintenanceIntervalMs` is its `--maintenance-interval`).
      const engineSaw = async (f: string): Promise<boolean> => (await engineLog(BROKEN)).some((l) => l.includes(f)); // prettier-ignore
      await waitUntil(
        async () => logs.saw("timer-wake purge failed") || (await engineSaw("timer-wake purge failed")) || (await engineSaw("timers headroom check failed")), // prettier-ignore
        20_000,
        "the maintenance pass to report the broken stream",
      );

      // The work pump is untouched by the timers stream being gone: runs that do not sleep still go
      // through. This is the part that matters operationally — a broken timer stream degrades
      // timers, it does not stop the worker.
      const client = createClient({ nc, prefix: BROKEN, ...SIZED });
      const h = await client.start(wf, {});
      expect(await h.result(20_000)).toBe("ok");

      const health = worker.health();
      expect(health.running).toBe(true);
      // durable-sdk: assertion removed — `health.pumps.some((p) => p.stream === timersStream)`.
      // The worker has no consumer pumps (one poll per slot); the timers durable is the engine's.
    } finally {
      await worker.stop();
    }

    // …and a fresh worker re-provisions the stream, so the outage is recoverable by a restart.
    // durable-sdk: provisioning is the engine's, at its start: the restart is the engine's.
    await restartEngine(BROKEN);
    const healed = createWorker({ nc, workflows: [wf], prefix: BROKEN, ...SIZED });
    await healed.start();
    try {
      const jsm = await jetstreamManager(nc);
      expect((await jsm.streams.info(names(BROKEN).timersStream)).config.allow_msg_schedules).toBe(
        true,
      );
      const client = createClient({ nc, prefix: BROKEN, ...SIZED });
      const slept = defineWorkflow("rec_timers_sleep", async (ctx) => {
        await ctx.sleep("nap", 200);
        return "woke";
      });
      const w2 = createWorker({ nc, workflows: [slept], prefix: BROKEN, ...SIZED });
      await w2.start();
      try {
        const h = await client.start(slept, {});
        expect(await h.result(20_000)).toBe("woke"); // timers work again
      } finally {
        await w2.stop();
      }
    } finally {
      await healed.stop();
    }
  }, 90_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)
});

// The paths that only exist because something unusual is being asked of the engine: killing a run
// whose code is not deployed, cancelling a chain that has already rolled over, a child failing with
// a payload too large to inline. Each is a place where the easy implementation strands somebody.
describe.skipIf(!natsUrl)("worker recovery — kills, chains and oversized failures", () => {
  let nc: NatsConnection;
  beforeAll(async () => {
    nc = await connect({ servers: natsUrl! });
  });
  afterAll(async () => {
    await nc.drain();
  });

  // prettier-ignore
  it("a cancel REASON survives the forward across a continue-as-new rollover", async () => {
    // Cancelling the original runId of a chain has to reach whichever generation is live, and the
    // operator's reason has to travel with it — that reason is the only record of WHY a run was
    // killed, and it is deliberately kept in the clear for exactly that purpose. Forwarding the
    // cancel but dropping the reason would leave a `cancelled` entry that explains nothing.
    const wf = defineWorkflow("rec_chain", (ctx, input: { n: number }) =>
      input.n === 0 ? ctx.continueAsNew({ n: 1 }) : ctx.await<string>("never"),
    );
    const worker = createWorker({ nc, workflows: [wf], prefix: REC, ...SIZED });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: REC, ...SIZED });
      const h = await client.start(wf, { n: 0 });
      const gen1 = `${h.runId}_g1`;
      await waitUntil(
        async () => (await client.get(gen1).status()).state === "suspended",
        20_000,
        "generation 1 to park",
      );

      await client.cancel(h.runId, { reason: "budget revoked" });
      await waitUntil(
        async () => (await client.get(gen1).status()).state === "cancelled",
        20_000,
        "the cancel to reach generation 1",
      );

      skipAsTenant(); // the test reads the journal itself (shell/names.ts)
      const jsm = await jetstreamManager(nc);
      const entries = await new NatsJournal(jetstream(nc), jsm, names(REC), jsonCodec).loadRaw(
        gen1,
      );
      const cancelled = entries.find((l) => l.entry.kind === "cancelled")?.entry as
        { reason?: string } | undefined;
      expect(cancelled?.reason).toBe("budget revoked");
    } finally {
      await worker.stop();
    }
  }, 90_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // prettier-ignore
  it("cancelling a BLOCKED child notifies its parent — a kill must not strand the caller", async () => {
    // The nastiest shape in the versioning design: a child pinned to a version nobody deploys parks
    // forever, and its parent waits on it forever. An operator kills the child — and there is no
    // code to run its onCancel handlers, so the worker writes the `cancelled` terminal directly.
    // If that path forgot to notify the parent, the kill would swap one stranded run for another.
    const childV2 = defineWorkflow("rec_blocked_kid", (ctx) => ctx.await<string>("never"), {
      version: 2,
    });
    const childV1 = defineWorkflow("rec_blocked_kid", (ctx) => ctx.run("s", () => "v1"), {
      version: 1,
    });
    const parent = defineWorkflow("rec_blocked_dad", async (ctx) => {
      try {
        await ctx.call(childV2, {});
        return "child-ok";
      } catch (e) {
        return `${(e as Error).name}/${(e as Error).message.includes("CancelledError") ? "cancelled" : "other"}`;
      }
    });
    // v2 is NOT registered: the child parks blocked the moment the parent starts it.
    const worker = createWorker({
      nc,
      workflows: [parent, childV1],
      prefix: REC,
      ...SIZED,
      versionWaitMs: 250,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: REC, ...SIZED });
      const h = await client.start(parent, {});
      const childId = `${h.runId}_0`;
      await waitUntil(
        async () => (await client.get(childId).status()).state === "blocked",
        25_000,
        "the child to park as blocked",
      );

      await client.cancel(childId, { reason: "v2 will never ship" });

      // Both halves: the child is tombstoned WITH the reason, and the parent is released.
      await waitUntil(
        async () => (await client.get(childId).status()).state === "cancelled",
        25_000,
        "the blocked child to be tombstoned",
      );
      expect(await h.result(25_000)).toBe("WorkflowFailure/cancelled");
      skipAsTenant(); // the test reads the journal itself (shell/names.ts)
      const jsm = await jetstreamManager(nc);
      const entries = await new NatsJournal(jetstream(nc), jsm, names(REC), jsonCodec).loadRaw(
        childId,
      );
      const cancelled = entries.find((l) => l.entry.kind === "cancelled")?.entry as
        { reason?: string } | undefined;
      expect(cancelled?.reason).toBe("v2 will never ship");
      expect(entries.some((l) => l.entry.kind === "step-begin")).toBe(false); // no code ever ran
    } finally {
      await worker.stop();
    }
  }, 90_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // durable-sdk: docs/findings.md F1, answered by the engine's F25 (put and get) and the SDK's
  // chunked transport (src/engine/blobs.ts).
  // prettier-ignore
  it("a child failing with an OVERSIZED message resolves the parent through a blob ref", async () => {
    // The child-done shrink path's error arm. A big failure message is not exotic — an exception
    // that embeds its input is the usual way one appears — and it cannot ride the work queue
    // inline. The parent's `resolve` entry therefore carries an errorRef, and `result()` has to
    // inflate it back into a real failure rather than surfacing an empty error.
    const boom = `declined: ${"reason ".repeat(Math.ceil(over(nc) / 7))}`; // > max_payload
    const child = defineWorkflow("rec_bigfail_kid", (_ctx): Promise<never> => {
      throw new Error(boom);
    });
    const parent = defineWorkflow("rec_bigfail_dad", async (ctx) => {
      try {
        await ctx.call(child, {});
        return "no";
      } catch (e) {
        return (e as Error).message.length;
      }
    });
    const worker = createWorker({ nc, workflows: [parent, child], prefix: REC, ...SIZED });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: REC, ...SIZED });
      const h = await client.start(parent, {});
      expect(await h.result(30_000)).toBeGreaterThan(over(nc));

      skipAsTenant(); // the test reads the journal itself (shell/names.ts)
      const jsm = await jetstreamManager(nc);
      const entries = await new NatsJournal(jetstream(nc), jsm, names(REC), jsonCodec).loadRaw(
        h.runId,
      );
      const resolve = entries.find((l) => l.entry.kind === "resolve")?.entry as
        { error?: unknown; errorRef?: { name: string } } | undefined;
      expect(resolve?.errorRef?.name).toMatch(new RegExp(`^${h.runId}/sha256:`));
      expect(resolve?.error).toBeUndefined();
    } finally {
      await worker.stop();
    }
  }, 90_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // prettier-ignore
  it("re-driving a COMPLETED child re-delivers its value to the parent, idempotently", async () => {
    // The success arm of the same repair the cancelled test covers: a crash between a child's
    // `result` and the child-done delivery would park the parent on a child that finished happily.
    const sink = new Sink();
    const child = defineWorkflow("rec_done_kid", (ctx) => ctx.run("s", () => "value"));
    const parent = defineWorkflow(
      "rec_done_dad",
      async (ctx) => `got:${await ctx.call(child, {})}`,
    );
    const worker = createWorker({
      nc,
      workflows: [parent, child],
      prefix: REC,
      ...SIZED,
      metrics: sink,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: REC, ...SIZED });
      const h = await client.start(parent, {});
      expect(await h.result(25_000)).toBe("got:value");
      const childId = `${h.runId}_0`;

      const redrives = (await engineOutcomes(nc, REC, ["terminal-redrive"])).length;
      const item: WorkItem = { runId: childId, kind: "resume" };
      await jetstream(nc).publish(names(REC).workSubject("rec_done_kid"), encodeJson(item), {
        msgID: `${childId}:redrive-done`,
      });
      // durable-sdk: re-expressed through the engine (its re-drive count), as above.
      await waitUntil(
        async () => sink.outcomes(childId).includes("terminal-redrive") || (await engineOutcomes(nc, REC, ["terminal-redrive"])).length > redrives, // prettier-ignore
        20_000,
        "the child re-drive",
      );

      expect(await h.result(5_000)).toBe("got:value"); // unchanged
      skipAsTenant(); // the test reads the journal itself (shell/names.ts)
      const jsm = await jetstreamManager(nc);
      const entries = await new NatsJournal(jetstream(nc), jsm, names(REC), jsonCodec).loadRaw(
        h.runId,
      );
      expect(entries.filter((l) => l.entry.kind === "resolve")).toHaveLength(1);
      expect(entries.filter((l) => l.entry.kind === "result")).toHaveLength(1);
    } finally {
      await worker.stop();
    }
  }, 90_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)

  // prettier-ignore
  it("stop() hands back an item its poll was handed as it stopped — another worker has it at once, never after an ack wait", async () => {
    // The engine's F51 (the SDK's part): a poll answered with an item in the instant before the
    // withdrawal reaches the worker after the `cancelPoll` was answered. A stop() that returned
    // then, with the process exiting right after, left the item to a lease nobody held, until its
    // ack wait (30 s). stop() now returns only once every poll has returned and that item has gone
    // back (`done retry`). The worker's connection holds the item's poll reply until the
    // withdrawal is answered, and a little longer, as the wire would; it is closed the moment
    // stop() resolves, as process.exit would.
    const wf = defineWorkflow("rec_stop_handback", (ctx) => ctx.run("s", () => "ran"));
    const dec = new TextDecoder();
    const holder = await connect({ servers: natsUrl! });
    let withdrawn!: () => void;
    const withdrawal = new Promise<void>((r) => (withdrawn = r));
    let handed!: () => void;
    const handedOff = new Promise<void>((r) => (handed = r));
    const dones: string[] = [];
    const held = new Proxy(holder, {
      get(t, k) {
        if (k === "request") {
          return async (subject: string, data?: Uint8Array, o?: { timeout: number }) => {
            const body = JSON.parse(dec.decode(data ?? new Uint8Array())) as { cancelPoll?: boolean; outcome?: { kind: string } }; // prettier-ignore
            const reply = await t.request(subject, data, o);
            if (subject.endsWith(".api.v1.poll") && body.cancelPoll === true) withdrawn();
            else if (subject.endsWith(".api.v1.poll") && "runId" in (JSON.parse(dec.decode(reply.data)) as object)) {
              handed();
              await withdrawal;
              await new Promise((r) => setTimeout(r, 200));
            } else if (subject.endsWith(".api.v1.done")) dones.push(body.outcome?.kind ?? "?");
            return reply;
          };
        }
        const v: unknown = Reflect.get(t, k);
        return typeof v === "function" ? (v as (...a: unknown[]) => unknown).bind(t) : v;
      },
    });
    const stopping = createWorker({ nc: held, workflows: [wf], prefix: REC, ...SIZED, concurrency: 1 });
    let other: ReturnType<typeof createWorker> | undefined;
    try {
      await stopping.start();
      const client = createClient({ nc, prefix: REC, ...SIZED });
      const h = await client.start(wf, {});
      await Promise.race([handedOff, new Promise((_, no) => setTimeout(() => no(new Error("the item never reached the poll")), 10_000 * WAN))]); // prettier-ignore
      await stopping.stop();
      expect(dones).toEqual(["retry"]); // handed back before stop() resolved
      await holder.close(); // the process exits

      other = createWorker({ nc, workflows: [wf], prefix: REC, ...SIZED });
      await other.start();
      const t0 = Date.now();
      expect(await h.result(15_000)).toBe("ran");
      expect(Date.now() - t0).toBeLessThan(15_000 * WAN); // the ack wait is 30 s
    } finally {
      await stopping.stop();
      await other?.stop();
      if (!holder.isClosed()) await holder.close();
    }
  }, 60_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)
});
