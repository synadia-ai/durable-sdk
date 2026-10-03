// Showcase: a durable "order" workflow exercising the core v0.1 ctx surface —
//   ctx.run   (durable steps)        ctx.sleep (a durable timer, native scheduling)
//   ctx.await (an external signal)   ctx.call  (a sub-workflow)
// plus client.start / signal / result / status. (ctx.continueAsNew and ctx.await's
// timeoutMs are also part of v0.1 — see DESIGN.md §2 — but aren't exercised here.)
//
//   1. start a local nats-server:  nats-server -js
//   2. run:                        NATS_URL=nats://127.0.0.1:4222 npm run example:charge
//
// Durability: while it's parked at "await payment" or "sleep", kill the process and re-run with the
// SAME runId (DE_RUN_ID=<id>) — it resumes from the journal instead of re-running completed steps.

import { connect } from "@nats-io/transport-node";
import {
  createClient,
  createWorker,
  defineWorkflow,
  withReconnectDefaults,
  type Ctx,
  type Logger,
} from "../src/index.js";

// A sub-workflow — an ordinary durable workflow in its own right, invoked below via ctx.call.
const ship = defineWorkflow("ship", async (ctx: Ctx, input: { orderId: string }) => {
  const label = await ctx.run("print-label", () => `LBL-${input.orderId}`);
  const tracking = await ctx.run("dispatch", () => `TRK-${input.orderId.slice(-4)}`);
  console.log(`    [ship] ${label} dispatched as ${tracking}`);
  return { label, tracking };
});

interface Cart {
  cartId: string;
  amount: number;
}

const order = defineWorkflow(
  "order",
  async (
    ctx: Ctx,
    input: Cart,
  ): Promise<{ status: string; orderId: string; tracking?: string }> => {
    // 1. a durable step — its result is journaled and never re-run on replay
    const orderId = await ctx.run("reserve-inventory", () => {
      console.log(`  [order] reserved inventory for ${input.cartId}`);
      return `ord-${input.cartId}`;
    });

    // 2. block until an external signal arrives (e.g. a payment webhook).
    // NOTE: this console.log lives in the workflow body (not a ctx.run), so it re-prints on every
    // replay — which is exactly why durable side effects belong inside ctx.run (run-once), not the body.
    console.log(`  [order] awaiting payment approval — send client.signal(runId, "payment", …)`);
    const payment = await ctx.await<{ approved: boolean }>("payment");
    if (!payment.approved) return { status: "abandoned", orderId };

    // 3. a durable timer (native JetStream scheduling) — survives restarts
    await ctx.sleep("settle-delay", 500);

    // 4. invoke a sub-workflow and wait for its result
    const shipment = await ctx.call(ship, { orderId });

    return { status: "shipped", orderId, tracking: shipment.tracking };
  },
);

const consoleLogger: Logger = {
  debug() {},
  info() {},
  warn: (m, f) => console.warn(m, f ?? ""),
  error: (m, f) => console.error(m, f ?? ""),
};

async function main(): Promise<void> {
  const url = process.env.NATS_URL ?? "nats://127.0.0.1:4222";
  const nc = await connect(withReconnectDefaults({ servers: url }));
  console.log(`connected to ${url}`);

  // one worker process can host many workflows; register the order and its sub-workflow
  const worker = createWorker({ nc, workflows: [order, ship], logger: consoleLogger });
  await worker.start();

  const client = createClient({ nc });
  const runId = process.env.DE_RUN_ID;
  const handle = await client.start(
    order,
    { cartId: "c-42", amount: 19.99 },
    runId ? { runId } : undefined,
  );
  console.log(`started order ${handle.runId}`);

  // simulate the payment webhook firing a moment later
  await new Promise((r) => setTimeout(r, 400));
  console.log("approving payment via client.signal(...)");
  await client.signal(handle.runId, "payment", { approved: true });

  const result = await handle.result(20_000);
  console.log("result:", result);
  console.log("status:", await handle.status());

  await worker.stop();
  await nc.drain();
}

main().catch((e: unknown) => {
  console.error(e);
  process.exit(1);
});
