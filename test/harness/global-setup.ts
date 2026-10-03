// vitest globalSetup: start one shared nats-server for the whole run; inject its URL. If the binary
// is absent, integration suites self-skip via describe.skipIf(!inject("natsUrl")).
//
// The server runs under an ACCOUNT-LIMITS profile (nats-server.ts; `DE_TEST_PROFILE`, default
// `ngs`): the managed service's per-stream consumer cap, stream count, storage and payload limits.
// Every test therefore runs under the quotas that bit the first NGS load run, not on the limitless
// default server that had hidden five consumer leaks. setup-each.ts is the per-test companion.
//
// `DE_TEST_RTT_MS=<n>` (opt-in) puts a delay proxy (proxy.ts) between every test and the server:
// `natsUrl` is then the PROXY, so the whole suite runs at that round-trip time without a test
// knowing. `natsDirectUrl` is always the server itself — for the harness's own hygiene, and for a
// test that brings its own proxy and must not stack two.

import { EngineNotAvailableError, enginePin, resolveEngineBinary } from "./engine-bin.js";
import { assertNoEphemeralConsumers } from "./leftovers.js";
import { NatsProxy } from "./proxy.js";
import {
  NatsServerNotAvailableError,
  NatsServerProcess,
  profileFromEnv,
  type ServerProfile,
} from "./nats-server.js";

interface SetupContext {
  provide(key: string, value: unknown): void;
}

export default async function setup(ctx: SetupContext): Promise<() => Promise<void>> {
  const profile = profileFromEnv();
  // The engine first: without it no suite can run, and a wrong checkout must fail the run loud.
  let engineBin: string | null = null;
  try {
    engineBin = resolveEngineBinary();
    console.warn(`\nengine: ${engineBin} (engine.pin ${enginePin()})\n`);
  } catch (e) {
    if (!(e instanceof EngineNotAvailableError)) throw e;
    console.warn(`\n⚠ durable-engine unavailable (${e.message}) — integration tests will be skipped.\n`); // prettier-ignore
    ctx.provide("engineBin", null);
    ctx.provide("natsUrl", null);
    ctx.provide("natsDirectUrl", null);
    ctx.provide("natsProfile", profile);
    ctx.provide("natsRttMs", 0);
    return () => Promise.resolve();
  }
  ctx.provide("engineBin", engineBin);
  const server = new NatsServerProcess({ profile });
  try {
    await server.start();
  } catch (e) {
    if (e instanceof NatsServerNotAvailableError) {
      console.warn("\n⚠ nats-server not found on PATH — integration tests will be skipped.\n");
      ctx.provide("natsUrl", null);
      ctx.provide("natsDirectUrl", null);
      ctx.provide("natsProfile", profile);
      ctx.provide("natsRttMs", 0);
      return () => Promise.resolve();
    }
    throw e;
  }
  const rttMs = rttFromEnv();
  const proxy = rttMs > 0 ? new NatsProxy(server.url, { rttMs }) : undefined;
  await proxy?.start();
  if (proxy) console.warn(`\n⏱  DE_TEST_RTT_MS=${String(rttMs)}: every test runs through a delay proxy.\n`); // prettier-ignore
  ctx.provide("natsUrl", proxy?.url ?? server.url);
  ctx.provide("natsDirectUrl", server.url);
  ctx.provide("natsProfile", profile);
  ctx.provide("natsRttMs", rttMs);
  return async () => {
    // The footprint check (leftovers.ts): a suite that leaked an ephemeral consumer anywhere fails
    // here, by name, rather than passing green on a server that never counts them. setup-each.ts
    // already checked after every test; this catches anything created after the last one.
    try {
      await assertNoEphemeralConsumers(server.url);
    } finally {
      await proxy?.stop();
      await server.stop();
    }
  };
}

/** `DE_TEST_RTT_MS`, default 0 (no proxy); a value that is not a number fails loud. */
function rttFromEnv(): number {
  const raw = process.env.DE_TEST_RTT_MS;
  if (raw === undefined || raw === "") return 0;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0)
    throw new Error(`DE_TEST_RTT_MS="${raw}": expected a number ≥ 0`);
  return n;
}

declare module "vitest" {
  interface ProvidedContext {
    natsUrl: string | null;
    natsDirectUrl: string | null;
    natsProfile: ServerProfile;
    natsRttMs: number;
  }
}
