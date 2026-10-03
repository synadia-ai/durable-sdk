// @synadia-ai/durable — a minimal TypeScript client, for the reverse cross-language interop test.
//
// The mirror image of ts-worker.ts: here the PYTHON side runs the workflow and this drives it, which
// is what proves the Python worker is conformant as seen by the reference implementation rather than
// only as seen by its own tests.
//
//   NATS_URL=… DE_PREFIX=… npx tsx conformance/interop/ts-client.ts <workflow> <runId> <inputJson>
//
// Prints one line: `RESULT <json>`, `FAILED <message>`, or `CANCELLED`.

import { connect } from "@nats-io/transport-node";
import { aesGcmPayloadCodec, createClient, defineWorkflow } from "../../src/index.js";
import { CancelledError, WorkflowFailure } from "../../src/errors.js";

const [workflowName, runId, inputJson] = process.argv.slice(2);
if (!workflowName || !runId) {
  console.error("usage: ts-client.ts <workflow> <runId> [inputJson]");
  process.exit(2);
}

// A client addresses a workflow by (name, version) — it never needs the body, which is the whole
// reason this file can exist. This one lives in Python.
const wf = defineWorkflow(workflowName, (): Promise<never> => {
  throw new Error("this workflow's body is implemented in the Python worker");
});

const nc = await connect({ servers: process.env.NATS_URL ?? "nats://127.0.0.1:4222" });
// DE_PAYLOAD_KEY (hex) turns on encryption at rest, so the reverse-interop test can prove the two
// SDKs' AES-GCM payload codecs are wire-compatible: PYTHON encrypts, this reads it back.
const keyHex = process.env.DE_PAYLOAD_KEY;
const client = createClient({
  nc,
  ...(process.env.DE_PREFIX !== undefined ? { prefix: process.env.DE_PREFIX } : {}),
  ...(keyHex !== undefined
    ? { payloadCodec: aesGcmPayloadCodec(Uint8Array.from(Buffer.from(keyHex, "hex"))) }
    : {}),
});

try {
  const handle = await client.start(wf, inputJson ? JSON.parse(inputJson) : {}, { runId });

  if (process.env.DE_SIGNAL !== undefined) {
    // Give the Python worker a moment to park on its ctx.wait, then deliver the signal. Not needed
    // for correctness (the signal is journaled either way) — it just keeps the test's intent clear.
    await new Promise((r) => setTimeout(r, 750));
    let data: unknown = JSON.parse(process.env.DE_SIGNAL_DATA ?? "{}");
    // DE_SIGNAL_PAD pads a string payload out to N bytes. An environment variable cannot carry the
    // megabytes it takes to force a work item over max_payload, and that oversized case is the one
    // where the SDKs have to agree on a blob ref rather than on an inline value.
    const pad = Number(process.env.DE_SIGNAL_PAD ?? 0);
    if (pad > 0 && typeof data === "string") data = data.padEnd(pad, "s");
    await client.signal(runId, process.env.DE_SIGNAL, data);
  }

  const result = await handle.result(20_000);
  console.log(`RESULT ${JSON.stringify(result ?? null)}`);
} catch (e) {
  if (e instanceof CancelledError) console.log("CANCELLED");
  else if (e instanceof WorkflowFailure) console.log(`FAILED ${e.message}`);
  else {
    console.error(String(e));
    process.exitCode = 1;
  }
} finally {
  await nc.close();
}
