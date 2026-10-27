// durable-sdk: two tests that FAIL ON PURPOSE, run only under DE_INJECT_FAILURE=1, to prove the
// existing-server mode's cleanup (F33): a test that throws with its worker still
// running and putting blobs, and one that times out while its worker is putting blobs (vitest
// moves on; the body keeps running), then a passing test while those workers would keep writing:
// the shape of a run against a remote server (a failed test's worker polled on for minutes). The run is expected to
// report the two as failed and to leave nothing behind on the prefix: no message in any of its
// eight, no durable but the engine's pair (it goes with the streams at the run's end).
// The Python twin is python/tests/integration/test_injected_failure.py.

import { describe, inject, it } from "vitest";
import { defineWorkflow } from "../../src/core/workflow.js";
import { createClient, createWorker } from "../harness/engine.js";
import { connect } from "../harness/existing.js";
import { maxPayload } from "../harness/payload.js";

const natsUrl = inject("natsUrl");
const injected = process.env.DE_INJECT_FAILURE === "1";

describe.skipIf(!natsUrl || !injected)("injected failures (DE_INJECT_FAILURE=1)", () => {
  it("throws with its worker left running, still writing blobs", async () => {
    const nc = await connect({ servers: natsUrl! });
    const P = maxPayload(nc);
    const wf = defineWorkflow("inj_throw", async (ctx) => {
      let n = 0;
      for (let i = 0; i < 40; i++) {
        n += (await ctx.run(`big-${String(i)}`, () => `${String(i)}`.repeat(Math.ceil(1.5 * P)))).length; // prettier-ignore
        await ctx.sleep(`nap-${String(i)}`, 50);
      }
      return n;
    });
    const worker = createWorker({ nc, workflows: [wf], prefix: "inj1" });
    await worker.start();
    const client = createClient({ nc, prefix: "inj1" });
    await client.start(wf, {});
    await new Promise((r) => setTimeout(r, 1_000));
    throw new Error("injected: the test fails with its worker running and writing blobs");
  });

  it("times out while its worker is still putting blobs", async () => {
    const nc = await connect({ servers: natsUrl! });
    const P = maxPayload(nc);
    const wf = defineWorkflow("inj_timeout", async (ctx) => {
      let n = 0;
      for (let i = 0; i < 40; i++) {
        n += (await ctx.run(`big-${String(i)}`, () => `${String(i)}`.repeat(Math.ceil(1.5 * P)))).length; // prettier-ignore
        await ctx.sleep(`nap-${String(i)}`, 50);
      }
      return n;
    });
    const worker = createWorker({ nc, workflows: [wf], prefix: "inj2" });
    await worker.start();
    const client = createClient({ nc, prefix: "inj2" });
    const h = await client.start(wf, {});
    await h.result(120_000); // never within the test's 3 s
  }, 3_000);

  it("passes after them, while their workers would keep writing", async () => {
    await new Promise((r) => setTimeout(r, 5_000));
  });
});
