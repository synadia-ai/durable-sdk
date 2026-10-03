// Crash & replay — the headline of durable execution, in two commands.
//
// A `provision` workflow with three steps. Each step logs ONLY when its function actually executes, so
// the console proves what replayed vs. what re-ran. Run it in two phases against the SAME runId:
//
//   nats-server -js &
//   # phase 1 — a worker runs two steps, then "crashes" (process.exit) before the third:
//   CRASH=1 DE_RUN_ID=crash-demo NATS_URL=nats://127.0.0.1:4222 npm run example:crash-replay
//   # phase 2 — a FRESH worker picks the run back up and finishes it:
//           DE_RUN_ID=crash-demo NATS_URL=nats://127.0.0.1:4222 npm run example:crash-replay
//
// In phase 2 you'll see the two completed steps are *replayed* (their functions never run again — no
// log) while only `attach` executes. The final result stitches together values produced in two
// different processes, each side effect having fired exactly once. See docs/walkthroughs/crash-replay.md.

import { connect } from "@nats-io/transport-node";
import {
  createClient,
  createWorker,
  defineWorkflow,
  withReconnectDefaults,
  type Ctx,
  type Logger,
} from "../src/index.js";

const provision = defineWorkflow(
  "provision",
  async (
    ctx: Ctx,
    input: { vm: string },
  ): Promise<{ ip: string; disk: string; status: string }> => {
    // The body runs top-to-bottom on EVERY incarnation (including each resume) — it's just control
    // flow. The durable side effects live inside ctx.run, so they run once and replay thereafter.
    console.log(`→ workflow entered for ${input.vm}`);

    const ip = await ctx.run("allocate-ip", () => {
      console.log("   ⚡ allocate-ip   EXECUTED");
      return "10.0.0.7";
    });
    const disk = await ctx.run("create-disk", () => {
      console.log("   ⚡ create-disk   EXECUTED");
      return "disk-42";
    });

    // Simulate a worker crash: two steps are journaled, the third never starts.
    if (process.env.CRASH === "1") {
      console.log("   ✗ CRASH — the worker dies here, before 'attach'");
      process.exit(1);
    }

    const status = await ctx.run("attach", () => {
      console.log("   ⚡ attach        EXECUTED");
      return "attached";
    });

    return { ip, disk, status };
  },
);

const logger: Logger = {
  debug() {},
  info() {},
  warn: (m, f) => console.warn(m, f ?? ""),
  error: (m, f) => console.error(m, f ?? ""),
};

async function main(): Promise<void> {
  const url = process.env.NATS_URL ?? "nats://127.0.0.1:4222";
  const nc = await connect(withReconnectDefaults({ servers: url }));
  // A low AckWait so a fresh worker re-picks up the in-flight (crashed) work item in ~2s instead of 30.
  const worker = createWorker({ nc, workflows: [provision], ackWaitMs: 2_000, logger });
  await worker.start();

  const client = createClient({ nc });
  const runId = process.env.DE_RUN_ID ?? "crash-demo";
  const handle = await client.start(provision, { vm: "web-1" }, { runId });
  console.log(`started provision ${handle.runId}\n`);

  if (process.env.CRASH === "1") {
    // A finished run is immutable — the worker won't re-execute it, so on an already-completed runId the
    // crash line is never reached. Detect that and exit cleanly (otherwise we'd hang on the open NATS
    // connection); on a fresh run the workflow calls process.exit(1) mid-run so we never get past the wait.
    const { state } = await handle.status();
    if (
      state === "completed" ||
      state === "failed" ||
      state === "cancelled" ||
      state === "continued"
    ) {
      console.log(`run ${runId} is already '${state}' — nothing to crash. Use a fresh DE_RUN_ID.`);
    } else {
      await new Promise((r) => setTimeout(r, 8_000));
    }
    await worker.stop();
    await nc.drain();
  } else {
    console.log("waiting for the resumed run to finish…\n");
    console.log("result:", await handle.result(20_000));
    console.log("status:", (await handle.status()).state);
    await worker.stop();
    await nc.drain();
  }
}

main().catch((e: unknown) => {
  console.error(e);
  process.exit(1);
});
