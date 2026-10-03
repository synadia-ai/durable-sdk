// Showcase: CANCELLATION (v0.2). client.cancel stops a run parked at a suspend point; result() then
// rejects with CancelledError and status() reports "cancelled". Cancellation is an uncatchable hard-stop
// at the next ctx.* suspend point — a workflow's try/catch cannot swallow it (use ctx.onCancel to clean
// up; see example:saga-cancel).
//
//   1. start a local nats-server:  nats-server -js
//   2. run:                        NATS_URL=nats://127.0.0.1:4222 npm run example:cancel

import { connect } from "@nats-io/transport-node";
import { createClient, createWorker, defineWorkflow, type Ctx } from "../src/index.js";
import { CancelledError } from "../src/errors.js";

const longJob = defineWorkflow("long-job", async (ctx: Ctx) => {
  await ctx.run("step-1", () => console.log("  [long-job] step 1 done"));
  console.log("  [long-job] now waiting up to an hour for approval…");
  await ctx.sleep("wait", 60 * 60 * 1000); // an hour — we cancel well before it fires
  return "finished";
});

async function main(): Promise<void> {
  const url = process.env.NATS_URL ?? "nats://127.0.0.1:4222";
  const nc = await connect({ servers: url });
  console.log(`connected to ${url}`);
  const worker = createWorker({ nc, workflows: [longJob] });
  await worker.start();
  const client = createClient({ nc });

  const handle = await client.start(longJob, {});
  console.log(`started ${handle.runId}`);

  await new Promise((r) => setTimeout(r, 500)); // let it park on the sleep
  console.log("status before cancel:", (await handle.status()).state);
  console.log("cancelling…");
  await client.cancel(handle.runId, { reason: "changed my mind" });

  try {
    await handle.result(15_000);
  } catch (e) {
    console.log("result() rejected with:", e instanceof CancelledError ? "CancelledError" : e);
  }
  console.log("status after cancel:", (await handle.status()).state); // "cancelled"

  await worker.stop();
  await nc.drain();
}

main().catch((e: unknown) => {
  console.error(e);
  process.exit(1);
});
