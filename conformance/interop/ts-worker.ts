// @synadia-ai/durable — a minimal TypeScript worker, for the cross-language interop test.
//
// The Python suite spawns this (`python/tests/integration/test_interop.py`), then drives it with a
// PYTHON client: start, result, status, signal, cancel. Nothing is shared between the two sides
// except the NATS streams and the protocol (durable-engine docs/protocol/PROTOCOL.md) — which is
// the entire point.
//
//   NATS_URL=… DE_PREFIX=… npx tsx conformance/interop/ts-worker.ts
//
// Prints "READY" on stdout once its consumers are bound, so the test can stop guessing.

import { readFileSync } from "node:fs";
import { credsAuthenticator } from "@nats-io/nats-core";
import { connect } from "@nats-io/transport-node";
import { aesGcmPayloadCodec, createWorker, defineWorkflow } from "../../src/index.js";

/** run + now + a returned object: proves values survive the round trip in both directions. */
const greet = defineWorkflow("greet", async (ctx, input: { name: string }) => {
  const shouted = await ctx.run("shout", () => input.name.toUpperCase());
  const at = await ctx.now();
  return { greeting: `HELLO ${shouted}`, at, from: "typescript" };
});

/**
 * Parks on a signal and reports what it received. With an oversized payload the signal's data
 * arrives as a blob ref the OTHER SDK wrote, so this is what proves the two agree on the offloaded
 * (and, under a payload codec, transformed) work-item form rather than only on inline values.
 */
const echoSignal = defineWorkflow("echo_signal", async (ctx) => {
  const data = await ctx.await<string>("card");
  return { len: data.length, head: data.slice(0, 24) };
});

/** Parks on a signal, so the Python client can drive `signal` and `cancel` against a live run. */
const approve = defineWorkflow("approve", async (ctx, input: { order: string }) => {
  const decision = await ctx.await<{ ok: boolean }>("decision");
  return { order: input.order, approved: decision.ok };
});

/** Fails, so the Python client sees a terminal `fail` surface as WorkflowFailure. */
const boom = defineWorkflow("boom", (_ctx, _input: unknown): Promise<never> => {
  throw new Error("the carrier rejected the shipment");
});

// NATS_CREDS: a creds file, when the server needs one (the harness's existing-server mode). A
// `user:password@` in NATS_URL is the connection's user and password: nats.js ignores it in a URL,
// and a config-file server's tenant users are names and passwords (docs/findings.md F38).
const creds = process.env.NATS_CREDS;
const url = new URL(process.env.NATS_URL ?? "nats://127.0.0.1:4222");
const user = decodeURIComponent(url.username);
const pass = decodeURIComponent(url.password);
url.username = "";
url.password = "";
const nc = await connect({
  servers: user === "" ? (process.env.NATS_URL ?? "nats://127.0.0.1:4222") : url.toString(),
  ...(user !== "" ? { user, pass } : {}),
  ...(creds ? { authenticator: credsAuthenticator(readFileSync(creds)) } : {}),
});
// DE_PAYLOAD_KEY (hex) turns on encryption at rest, mirroring ts-client.ts — so the Python suite can
// drive a TypeScript worker that encrypts and check what actually landed on the streams.
const keyHex = process.env.DE_PAYLOAD_KEY;
const worker = createWorker({
  nc,
  workflows: [greet, approve, boom, echoSignal],
  ...(process.env.DE_PREFIX !== undefined ? { prefix: process.env.DE_PREFIX } : {}),
  ...(keyHex !== undefined
    ? { payloadCodec: aesGcmPayloadCodec(Uint8Array.from(Buffer.from(keyHex, "hex"))) }
    : {}),
});
await worker.start();
console.log("READY");

const shutdown = async (): Promise<void> => {
  await worker.stop();
  await nc.close();
  process.exit(0);
};
process.on("SIGTERM", () => void shutdown());
process.on("SIGINT", () => void shutdown());
