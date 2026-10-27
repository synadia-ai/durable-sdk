// vitest setupFiles: after EVERY test, the test's engines are stopped (engine.ts), then two things
// on the shared server.
//
// 1. The leftover check, attributed: any consumer without a durable name fails the test that made
//    it, by name (leftovers.ts). The end-of-run check still exists; this one says WHO.
// 2. The owner of the test's streams: every stream on the server is deleted. The server runs under
//    an account profile (nats-server.ts) whose stream cap is 16 — a prefix is eight, so two prefixes — so a test's
//    provisioning must be gone before the next test starts. Deleting everything rather than the
//    prefixes a test declared means an undeclared prefix cannot accumulate either. Tests that
//    reuse a prefix name across cases simply re-provision (provisioning is idempotent, and a
//    worker's start() provisions).
//
// A test that leaves a worker RUNNING sees its pumps lose their streams here and retry with
// backoff until its file's afterAll drains the connection — the test was already at fault.

import { type NatsConnection } from "@nats-io/transport-node";
import { afterAll, afterEach, beforeEach, expect, inject, vi } from "vitest";
import { appendFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { beginTest, endTestWorkers, reportRunIds, setEngineServer, stopEngines, testBlobBytes } from "./engine.js"; // prettier-ignore
import {
  adminFromEnv,
  connect,
  connectAdmin,
  deleteRunBlobs,
  prefixEphemeralConsumers,
  purgeSince,
  runIdsSince,
  scopeOf,
  snapshot,
  type Snapshot,
} from "./existing.js";
import { deleteAllStreams, leftoverMessage, listEphemeralConsumersOn } from "./leftovers.js";

// Against an existing server (existing.ts): the run's one engine stays up, the leftovers check
// looks at the prefix's streams only, and in place of deleting every stream the test's writes
// under the prefix are purged (what changed since the snapshot taken before it). Every wait is
// scaled by the round trip measured at the start: the test and hook timeouts here, a handle's
// result() in engine.ts.
const runPrefix = inject("runPrefix");
const factor = inject("rttFactor");
if (factor !== 1) vi.setConfig({ testTimeout: 30_000 * factor, hookTimeout: 30_000 * factor });
let before: Snapshot | undefined;
/** Every run id this file's tests touched: each cleanup deletes all their blobs by name, so a
 * blob a timed-out test's run wrote after its own cleanup goes at the next one. */
const fileRunIds = new Set<string>();
// The conformance workers and clients the interop suite spawns read NATS_CREDS.
if (runPrefix != null && process.env.DE_NATS_CREDS) process.env.NATS_CREDS = process.env.DE_NATS_CREDS; // prettier-ignore

// The server itself, never the delay proxy (`DE_TEST_RTT_MS`): hygiene is the harness's own
// traffic, and listing every consumer of every stream at WAN latency would dominate the run.
const url = inject("natsDirectUrl");
let nc: NatsConnection | undefined;
let anc: NatsConnection | undefined;

/** The tests' identity on the server: the engine's API (`info`, a `cancelPoll`). */
async function conn(): Promise<NatsConnection> {
  nc ??= await connect({ servers: url!, name: "test-harness-hygiene" });
  return nc;
}

/** The streams: in the mode the admin connection (DE_ADMIN_URL, the tests' own by default; as a
 * tenant the engine's account), elsewhere the spawned server. */
async function admin(): Promise<NatsConnection> {
  if (runPrefix == null) return conn();
  const a = adminFromEnv();
  if (a.adminUrl === url && a.adminCreds === (process.env.DE_NATS_CREDS || undefined))
    return conn();
  anc ??= await connectAdmin({ name: "test-harness-admin" });
  return anc;
}

beforeEach(async () => {
  if (runPrefix == null || !url) return;
  // A test skipped part-way (`context.skip()`: as a tenant, a test that reads the engine's streams;
  // under DE_ENGINE_URL, one that needs other flags or a restart) never reaches afterEach, so what
  // it wrote before the skip is cleaned here, before the next snapshot would take it as given.
  if (before !== undefined) await afterEachExisting(runPrefix, { skipped: true });
  before = await snapshot(await admin(), scopeOf(runPrefix));
  await beginTest(await conn());
});

afterEach(async () => {
  if (runPrefix != null) return afterEachExisting(runPrefix);
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

// A test that failed or timed out is cleaned like any other (F33): vitest runs
// this hook after both, and everything below the leftover check is in its `finally`. Its workers
// are stopped (bounded) and its polls withdrawn first, so neither a worker it left running nor a
// fetch the engine still holds re-creates what the cleanup deletes.
async function afterEachExisting(prefix: string, o: { skipped?: boolean } = {}): Promise<void> {
  setEngineServer(undefined);
  if (!url) return;
  const c = await conn();
  const a = await admin();
  const scope = scopeOf(prefix);
  if (o.skipped !== true) tallyBlobBytes(prefix);
  try {
    await endTestWorkers(c);
    // A skipped test's leftovers are not checked: the next test's name would be on them.
    if (o.skipped === true) return;
    let left = await prefixEphemeralConsumers(a, scope, before?.consumers);
    if (left.length > 0) {
      await new Promise((r) => setTimeout(r, 500 * factor));
      left = await prefixEphemeralConsumers(a, scope, before?.consumers);
    }
    if (left.length > 0) {
      const who = expect.getState().currentTestName ?? "(unknown test)";
      throw new Error(leftoverMessage(left, `after "${who}"`));
    }
  } finally {
    if (before !== undefined) {
      const runIds = await runIdsSince(a, scope, before);
      runIds.forEach((r) => fileRunIds.add(r));
      await purgeSince(a, scope, before);
      await deleteRunBlobs(a, scope, fileRunIds);
      await reportRunIds(runIds);
    }
    before = undefined;
  }
}

/** The bytes the test's clients and workers moved through `put` and `get`, a line of
 * `.de-run/<prefix>/blob-bytes.tsv` (test, bytes up, bytes down, max_payload): what a large-value
 * test's bandwidth term counts (mode.ts `moving`). */
function tallyBlobBytes(prefix: string): void {
  const { put, get } = testBlobBytes();
  if (put + get === 0) return;
  const dir = join(fileURLToPath(new URL("../..", import.meta.url)), ".de-run", prefix);
  mkdirSync(dir, { recursive: true });
  const who = expect.getState().currentTestName ?? "(unknown test)";
  appendFileSync(join(dir, "blob-bytes.tsv"), `${who}\t${String(put)}\t${String(get)}\t${String(inject("natsMaxPayload"))}\n`); // prettier-ignore
}

afterAll(async () => {
  if (runPrefix != null && before !== undefined) await afterEachExisting(runPrefix, { skipped: true }); // prettier-ignore
  if (anc) await anc.drain();
  anc = undefined;
  if (nc) await nc.drain();
  nc = undefined;
});
