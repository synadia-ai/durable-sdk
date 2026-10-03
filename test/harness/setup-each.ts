// vitest setupFiles: after EVERY test, the test's engines are stopped (engine.ts), then two things
// on the shared server.
//
// 1. The leftover check, attributed: any consumer without a durable name fails the test that made
//    it, by name (leftovers.ts). The end-of-run check still exists; this one says WHO.
// 2. The owner of the test's streams: every stream on the server is deleted. The server runs under
//    an account profile (nats-server.ts) whose stream cap is 16 — a prefix is seven — so a test's
//    provisioning must be gone before the next test starts. Deleting everything rather than the
//    prefixes a test declared means an undeclared prefix cannot accumulate either. Tests that
//    reuse a prefix name across cases simply re-provision (provisioning is idempotent, and a
//    worker's start() provisions).
//
// A test that leaves a worker RUNNING sees its pumps lose their streams here and retry with
// backoff until its file's afterAll drains the connection — the test was already at fault.

import { connect, type NatsConnection } from "@nats-io/transport-node";
import { afterAll, afterEach, expect, inject } from "vitest";
import { setEngineServer, stopEngines } from "./engine.js";
import { deleteAllStreams, leftoverMessage, listEphemeralConsumersOn } from "./leftovers.js";

// The server itself, never the delay proxy (`DE_TEST_RTT_MS`): hygiene is the harness's own
// traffic, and listing every consumer of every stream at WAN latency would dominate the run.
const url = inject("natsDirectUrl");
let nc: NatsConnection | undefined;

async function conn(): Promise<NatsConnection> {
  nc ??= await connect({ servers: url!, name: "test-harness-hygiene" });
  return nc;
}

afterEach(async () => {
  // The engines of the test first (a graceful stop naks what they hold), then the streams.
  await stopEngines();
  setEngineServer(undefined);
  if (!url) return;
  const c = await conn();
  try {
    let left = await listEphemeralConsumersOn(c);
    if (left.length > 0) {
      // A consumer mid-teardown (a test's own helper unwinding) clears in well under this; a leak
      // does not.
      await new Promise((r) => setTimeout(r, 500));
      left = await listEphemeralConsumersOn(c);
    }
    if (left.length > 0) {
      const who = expect.getState().currentTestName ?? "(unknown test)";
      throw new Error(leftoverMessage(left, `after "${who}"`));
    }
  } finally {
    await deleteAllStreams(c);
  }
});

afterAll(async () => {
  if (nc) await nc.drain();
  nc = undefined;
});
