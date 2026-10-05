// Showcase: durable FAN-OUT (v0.2). Start several sub-workflows concurrently with ctx.startCall and
// gather them with native Promise.all; race a child against a durable timer with ctx.race.
//
//   1. start a local nats-server:  nats-server -js
//   2. run:                        NATS_URL=nats://127.0.0.1:4222 npm run example:fan-out   (on the fabric: NATS_URL=<the fabric> NATS_CREDS=<your creds file>)
//
// All children START in one pass (before any await), then the run suspends once and each child-done
// re-drives it until every branch has resolved — true parallel fan-out over the work queue.

import { readFileSync } from "node:fs";
import { connect, credsAuthenticator } from "@nats-io/transport-node";
import { createClient, createWorker, defineWorkflow, type Ctx } from "../src/index.js";

// A child "task" — an ordinary durable workflow, invoked below as a fan-out branch.
const task = defineWorkflow("task", (ctx: Ctx, n: number) => ctx.run("compute", () => n * n));

// Scatter-gather: one child per item, started concurrently, gathered with Promise.all.
const batch = defineWorkflow("batch", async (ctx: Ctx, input: { items: number[] }) => {
  const futures = input.items.map((n) => ctx.startCall(task, n)); // every child starts HERE
  const squares = await Promise.all(futures); // suspend until ALL have resolved
  const total = squares.reduce((a, b) => a + b, 0);
  console.log(
    `  [batch] ${input.items.join(",")} → squares [${squares.join(",")}] → total ${total}`,
  );
  return total;
});

// Timeout pattern: race a child against a durable timer — whichever settles first (by journal order) wins.
const raced = defineWorkflow("raced", async (ctx: Ctx, n: number) => {
  const winner = await ctx.race([ctx.startCall(task, n), ctx.startSleep("timeout", 5_000)]);
  return typeof winner === "number" ? `task won (${winner})` : "timed out";
});

async function main(): Promise<void> {
  const url = process.env.NATS_URL ?? "nats://127.0.0.1:4222";
  // NATS_CREDS: a creds file, for the fabric (docs/FABRIC.md); absent, a plain local server.
  const creds = process.env.NATS_CREDS;
  const auth = creds ? { authenticator: credsAuthenticator(readFileSync(creds)) } : {};
  const nc = await connect({ servers: url, ...auth });
  console.log(`connected to ${url}`);
  const worker = createWorker({ nc, workflows: [batch, task, raced] });
  await worker.start();
  const client = createClient({ nc });

  const batchHandle = await client.start(batch, { items: [1, 2, 3, 4, 5] });
  console.log(`started batch ${batchHandle.runId}`);
  console.log("batch result:", await batchHandle.result(20_000)); // 1+4+9+16+25 = 55

  const raceHandle = await client.start(raced, 7);
  console.log("race result:", await raceHandle.result(20_000)); // "task won (49)"

  await worker.stop();
  await nc.drain();
}

main().catch((e: unknown) => {
  console.error(e);
  process.exit(1);
});
