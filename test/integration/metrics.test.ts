// durable-sdk: the twin of python/tests/integration/test_metrics.py's "one ordinary run reports the
// whole path" (F21): the TypeScript worker emits `journal.load` once per item and
// the real size of every appended entry, as the Python worker does, so a dashboard reads one number
// from both. The library's TypeScript tree has no metrics suite of its own to copy.

import { type NatsConnection } from "@nats-io/transport-node";
import { connect } from "../harness/existing.js";
import { afterAll, beforeAll, describe, expect, inject, it } from "vitest";
import { defineWorkflow } from "../../src/core/workflow.js";
import { type DurableEventFields, type DurableEventName } from "../../src/internal/metrics.js";
import { createClient, createWorker } from "../harness/engine.js";

const natsUrl = inject("natsUrl");

class Recorder {
  readonly events: Array<{ name: DurableEventName; fields: DurableEventFields }> = [];
  onEvent(name: DurableEventName, fields: DurableEventFields): void {
    this.events.push({ name, fields });
  }
  named(name: DurableEventName): DurableEventFields[] {
    return this.events.filter((e) => e.name === name).map((e) => e.fields);
  }
}

describe.skipIf(!natsUrl)("metrics (the Python worker's twin)", () => {
  let nc: NatsConnection;
  beforeAll(async () => {
    nc = await connect({ servers: natsUrl! });
  });
  afterAll(async () => {
    await nc.drain();
  });

  it("one ordinary run reports the whole path", async () => {
    const prefix = "metrics1";
    const sink = new Recorder();
    const wf = defineWorkflow("tsmetrics1", async (ctx) => {
      await ctx.run("step", () => "s");
      await ctx.sleep("brief", 50);
      return "done";
    });
    const worker = createWorker({ nc, workflows: [wf], prefix, metrics: sink });
    await worker.start();
    try {
      const client = createClient({ nc, prefix });
      const h = await client.start(wf, undefined);
      expect(await h.result(30_000)).toBe("done");
      const deadline = Date.now() + 10_000;
      while (!sink.named("work.done").some((d) => d.outcome === "completed")) {
        if (Date.now() > deadline) throw new Error("the worker never reported the completed item");
        await new Promise((r) => setTimeout(r, 50));
      }

      const appends = sink.named("journal.append");
      expect(appends.length).toBeGreaterThan(0);
      expect(new Set(appends.map((a) => a.outcome))).toEqual(new Set(["ok"]));
      const kinds = new Set(appends.map((a) => a.kind));
      for (const k of ["step-begin", "step-end", "sleep", "result"]) expect(kinds).toContain(k);
      expect(appends.every((a) => typeof a.ms === "number" && (a.bytes as number) > 0)).toBe(true);

      // One journal.load per item, with the journal's size.
      const loads = sink.named("journal.load");
      expect(loads.length).toBeGreaterThanOrEqual(sink.named("work.dispatch").length);
      expect(new Set(loads.map((l) => l.outcome))).toEqual(new Set(["ok"]));
      expect(loads.every((l) => (l.entries as number) > 0 && (l.bytes as number) > 0)).toBe(true);

      const dispatched = new Set(sink.named("work.dispatch").map((d) => d.kind));
      expect(dispatched).toContain("start");
      expect(dispatched).toContain("timer");
      const done = sink.named("work.done");
      const outcomes = new Set(done.map((d) => d.outcome));
      expect(outcomes).toContain("suspended");
      expect(outcomes).toContain("completed");
      expect(done.every((d) => "waitMs" in d && "deliveryCount" in d)).toBe(true);
    } finally {
      await worker.stop();
    }
  });
});
