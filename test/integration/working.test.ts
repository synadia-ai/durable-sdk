// Ack-progress heartbeats (audit P2: `working()`): a step that legitimately runs longer than
// AckWait must NOT be redelivered mid-execution (redelivery during a live step is the biggest
// at-least-once effect amplifier — a second worker re-runs the frontier fn). And the inverse edge:
// the extension MUST be capped, so a genuinely hung step eventually lets the item redeliver instead
// of holding the run hostage forever. Observed via the metrics seam: work.dispatch counts deliveries
// into this worker; work.done carries deliveryCount.

import { type NatsConnection } from "@nats-io/transport-node";
import { connect } from "../harness/existing.js";
import { afterAll, beforeAll, describe, expect, inject, it } from "vitest";
import { defineWorkflow } from "../../src/core/workflow.js";
import {
  type DurableEventFields,
  type DurableEventName,
  type Metrics,
} from "../../src/internal/metrics.js";
import { createClient } from "../harness/engine.js";
import { createWorker } from "../harness/engine.js";
import { WAN } from "../harness/mode.js";

const natsUrl = inject("natsUrl");
const MIB = 1024 * 1024;
const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

type Recorded = { name: DurableEventName; fields: DurableEventFields };
function recorder(): { events: Recorded[]; sink: Metrics } {
  const events: Recorded[] = [];
  return {
    events,
    sink: {
      onEvent(name, fields) {
        events.push({ name, fields });
      },
    },
  };
}

async function waitUntil(cond: () => boolean, timeoutMs: number): Promise<void> {
  timeoutMs *= WAN; // durable-sdk: × the round-trip factor (harness/mode.ts, F29)
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (cond()) return;
    if (Date.now() > deadline) throw new Error("waitUntil timed out");
    await delay(50);
  }
}

describe.skipIf(!natsUrl)("ack-progress heartbeats for long steps (real nats-server)", () => {
  let nc: NatsConnection;
  beforeAll(async () => {
    nc = await connect({ servers: natsUrl! });
  });
  afterAll(async () => {
    await nc.drain();
  });

  it("a step much longer than AckWait completes on ONE delivery with ONE execution", async () => {
    const prefix = "workx1";
    let executions = 0;
    const wf = defineWorkflow("wx_slow_step", (ctx) =>
      ctx.run("slow", async () => {
        executions += 1;
        await delay(4_000); // ~2.7 × ackWait: without heartbeats this redelivers mid-step
        return "done";
      }),
    );
    const r = recorder();
    const worker = createWorker({
      nc,
      workflows: [wf],
      prefix,
      streamMaxBytes: 16 * MIB,
      ackWaitMs: 1_500,
      metrics: r.sink,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix });
      const handle = await client.start(wf, undefined);
      expect(await handle.result(20_000)).toBe("done");
      // Give a straggling redelivery a moment to surface before asserting there was none.
      await delay(2_000);
      const deliveries = r.events.filter(
        (e) => e.name === "work.dispatch" && e.fields.runId === handle.runId,
      );
      const dones = r.events.filter(
        (e) => e.name === "work.done" && e.fields.runId === handle.runId,
      );
      expect(executions).toBe(1);
      expect(deliveries.length).toBe(1); // no AckWait redelivery ever reached this worker
      expect(dones.length).toBe(1);
      expect(dones[0]?.fields.outcome).toBe("completed");
      expect(dones[0]?.fields.deliveryCount).toBe(1);
    } finally {
      await worker.stop();
    }
  });

  it("the heartbeat also covers LOCAL QUEUE wait: an item chained behind a slow same-run step never redelivers", async () => {
    const prefix = "workx2";
    const wf = defineWorkflow("wx_chain", (ctx) =>
      ctx.run("slow", async () => {
        await delay(2_500);
        return "done";
      }),
    );
    const r = recorder();
    const worker = createWorker({
      nc,
      workflows: [wf],
      prefix,
      streamMaxBytes: 16 * MIB,
      ackWaitMs: 1_500,
      metrics: r.sink,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix });
      const handle = await client.start(wf, undefined);
      await waitUntil(
        () => r.events.some((e) => e.name === "work.dispatch" && e.fields.runId === handle.runId),
        5_000,
      );
      await delay(200); // the slow step is now executing
      // A signal for the SAME run: delivered immediately, but per-run serialization chains it
      // behind the executing step for ~2.3s > ackWait. The heartbeat must start at DISPATCH, not at
      // execution start, or this queued item redelivers while we still intend to process it.
      await client.signal(handle.runId, "poke", { n: 1 });
      expect(await handle.result(20_000)).toBe("done");
      await delay(2_000); // let any straggling redelivery surface
      const signalDispatches = r.events.filter(
        (e) =>
          e.name === "work.dispatch" &&
          e.fields.runId === handle.runId &&
          e.fields.kind === "signal",
      );
      // durable-sdk: assertion touched, `toBe(1)` → `toBeLessThanOrEqual(1)`. The queued signal
      // item now waits in the engine, behind the run's held item, with its ack wait kept there;
      // when it comes up the run has ended, and the engine never hands an ended run's item to a
      // poll (it acks it itself: the engine's API.md, "poll"). So it reaches this worker 0
      // times, never twice — the claim of the test, "never redelivers", is what still holds.
      expect(signalDispatches.length).toBeLessThanOrEqual(1); // at most one delivery of the queued signal item
    } finally {
      await worker.stop();
    }
  });

  it("ESCAPE HATCH: past maxWorkMs the heartbeat stops and a hung step's item redelivers", async () => {
    const prefix = "workx3";
    let release!: () => void;
    const gate = new Promise<string>((resolve) => {
      release = () => resolve("released");
    });
    let executions = 0;
    const wf = defineWorkflow("wx_hung", (ctx) =>
      ctx.run("hang", () => {
        executions += 1;
        return gate; // first (and only local) execution hangs until the test releases it
      }),
    );
    const r = recorder();
    const worker = createWorker({
      nc,
      workflows: [wf],
      prefix,
      streamMaxBytes: 16 * MIB,
      ackWaitMs: 800,
      maxWorkMs: 1_600, // extend at most twice, then let AckWait expire
      metrics: r.sink,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix });
      const handle = await client.start(wf, undefined);
      // The redelivered item lands back on this (single) worker as a SECOND dispatch — proving the
      // server considered the item abandoned once the cap cut the heartbeat off. (It chains behind
      // the hung first execution locally; on a multi-worker fleet another worker would take over.)
      await waitUntil(
        () =>
          r.events.filter((e) => e.name === "work.dispatch" && e.fields.runId === handle.runId)
            .length >= 2,
        15_000,
      );
      expect(executions).toBe(1); // per-run chain: the redelivery never double-executes LOCALLY
      release(); // un-hang: the first execution finishes, the redelivered copy re-drives the terminal
      expect(await handle.result(20_000)).toBe("released");
    } finally {
      await worker.stop();
    }
  });

  // prettier-ignore
  it("ESCAPE HATCH across WORKERS: past maxWorkMs the lease lapses too, so a peer takes the run over", async () => {
    // The bound has to release BOTH grips this worker holds on the run. Releasing only the ack
    // extension makes the item redeliverable and nothing more: the peer that receives it is refused
    // by the lease the hung holder is still renewing, so the escape hatch is defeated by the
    // optimization sitting next to it. Here the same deadline stops the renewals, and the takeover
    // actually happens. (Found while porting this to Python; the control — unbounded renewal —
    // never takes the run over at all.)
    const prefix = "workx4";
    let release!: () => void;
    const gate = new Promise<string>((resolve) => {
      release = () => resolve("released");
    });
    const attempts: number[] = [];
    const wf = defineWorkflow("wx_takeover", async (ctx) => {
      attempts.push(ctx.attempt); // ctx.attempt IS the delivery count
      return ctx.run("hang", () => gate);
    });
    const opts = {
      nc,
      workflows: [wf],
      prefix,
      streamMaxBytes: 16 * MIB,
      ackWaitMs: 800,
      maxWorkMs: 1_600, // extend at most twice, then let go of everything
      leaseTtlMs: 1_000, // …and let the abandoned lease expire quickly
    };
    const a = createWorker(opts);
    const b = createWorker(opts); // a peer, idle, waiting for the redelivery
    await a.start();
    await b.start();
    try {
      const client = createClient({ nc, prefix });
      const handle = await client.start(wf, undefined);
      await waitUntil(() => attempts.length >= 2, 25_000);
      expect(attempts[0]).toBe(1);
      expect(attempts[1]).toBeGreaterThanOrEqual(2); // a genuine redelivery, not a fresh start
      release();
      expect(await handle.result(20_000)).toBe("released");
    } finally {
      await a.stop();
      await b.stop();
    }
  }, 40_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)
});
