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

import { mkdirSync, writeFileSync } from "node:fs";
import net from "node:net";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { EngineNotAvailableError, enginePin, resolveEngineBinary } from "./engine-bin.js";
import {
  connect,
  deletePrefixStreams,
  existingFromEnv,
  measureRtt,
  prefixEphemeralConsumers,
  prefixStreams,
  rttFactor,
  snapshot,
  type ExistingServer,
} from "./existing.js";
import { RunEngine } from "./run-engine.js";
import { assertNoEphemeralConsumers, leftoverMessage } from "./leftovers.js";
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
  ctx.provide("runPrefix", null);
  ctx.provide("rttFactor", 1);
  ctx.provide("engineControl", null);
  const existing = existingFromEnv();
  if (existing) return setupExisting(ctx, existing);
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

const freePort = (): Promise<number> =>
  new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.on("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const a = srv.address();
      srv.close(() => resolve(typeof a === "object" && a ? a.port : 0));
    });
  });

/**
 * The mode for an existing server (existing.ts): no spawn, no profile, one prefix and one engine
 * for the run, the waits scaled by the measured round trip. `natsDirectUrl` is the server itself
 * (the harness's hygiene), `natsUrl` the server or, under `DE_TEST_RTT_MS`, the delay proxy in
 * front of it — and so is the engine's: it runs where the harness runs, so its round trip to the
 * server is the tests' (from a laptop to NGS, the WAN's; locally, the proxy's).
 */
async function setupExisting(ctx: SetupContext, ex: ExistingServer): Promise<() => Promise<void>> {
  const engineBin = ex.engineUrl === undefined ? resolveEngineBinary() : null;
  const nc = await connect({ servers: ex.url, name: "test-harness-run" });
  // JetStream first: a server or an account without it fails here, by name, not in every test.
  const { jetstreamManager } = await import("@nats-io/jetstream");
  await (await jetstreamManager(nc)).getAccountInfo();
  const serverRtt = await measureRtt(nc);
  const proxyRtt = rttFromEnv();
  const proxy = proxyRtt > 0 ? new NatsProxy(ex.url, { rttMs: proxyRtt }) : undefined;
  await proxy?.start();
  let rttMs = serverRtt;
  if (proxy) {
    const pnc = await connect({ servers: proxy.url, name: "test-harness-rtt" });
    rttMs = await measureRtt(pnc);
    await pnc.close();
  }
  const factor = rttFactor(rttMs);
  const run = new RunEngine(engineBin, proxy?.url ?? ex.url, ex.prefix, {
    ...(ex.creds !== undefined ? { creds: ex.creds } : {}),
    metricsAddr: `127.0.0.1:${String(await freePort())}`,
    startTimeoutMs: Math.round(30_000 * factor),
  });
  console.warn(
    `\nexisting server ${ex.url}${ex.creds !== undefined ? " (creds)" : ""}, prefix ${ex.prefix}, ` +
      `round trip ${rttMs.toFixed(1)} ms${proxy ? ` (the server ${serverRtt.toFixed(1)} ms, the proxy ${String(proxyRtt)} ms)` : ""} → every wait × ${String(factor)}\n` + // prettier-ignore
      (engineBin !== null
        ? `engine: ${engineBin} (engine.pin ${enginePin()}), one for the run, metrics on http://${run.extras.metricsAddr!}\n`
        : `engine: DE_ENGINE_URL ${ex.engineUrl!} (not started by the harness)\n`),
  );
  if (engineBin !== null) {
    // The run's engine provisions the seven of the prefix, or says why not (a tier too small).
    try {
      await run.ensure({});
    } catch (e) {
      await nc.close();
      await proxy?.stop();
      throw e;
    }
  } else {
    const r = await fetch(new URL("/healthz", ex.engineUrl));
    if (!r.ok) throw new Error(`DE_ENGINE_URL ${ex.engineUrl!}: /healthz answered ${String(r.status)}`); // prettier-ignore
  }
  await run.listen();
  // What the prefix held before the run: another client's consumers on a shared prefix are not
  // this run's leftovers.
  const atStart = await snapshot(nc, ex.prefix);
  ctx.provide("engineBin", engineBin ?? "external");
  ctx.provide("natsUrl", proxy?.url ?? ex.url);
  ctx.provide("natsDirectUrl", ex.url);
  ctx.provide("natsProfile", "none");
  ctx.provide("natsRttMs", Math.round(rttMs));
  ctx.provide("runPrefix", ex.prefix);
  ctx.provide("rttFactor", factor);
  ctx.provide("engineControl", run.controlUrl);
  return async () => {
    try {
      // The footprint check, on the prefix's streams only.
      const left = await prefixEphemeralConsumers(nc, ex.prefix, atStart.consumers);
      if (left.length > 0) throw new Error(leftoverMessage(left, "after the suite"));
    } finally {
      await report(nc, ex, run);
      await run.stop();
      if (ex.deleteAtEnd) {
        const gone = await deletePrefixStreams(nc, ex.prefix);
        console.warn(`DE_CLEANUP=delete: deleted ${gone.join(", ") || "nothing"}`);
      }
      await nc.close();
      await proxy?.stop();
    }
  };
}

/** What the run leaves for the reader: the prefix's leftovers and the engine's /metrics. */
async function report(
  nc: Awaited<ReturnType<typeof connect>>,
  ex: ExistingServer,
  run: RunEngine,
): Promise<void> {
  const { jetstreamManager } = await import("@nats-io/jetstream");
  const jsm = await jetstreamManager(nc);
  const lines: string[] = [];
  for (const s of prefixStreams(ex.prefix)) {
    try {
      const si = await jsm.streams.info(s);
      const durables: string[] = [];
      for await (const c of jsm.consumers.list(s)) durables.push(c.name);
      lines.push(`${s}: ${String(si.state.messages)} messages, ${String(si.state.bytes)} bytes, consumers [${durables.join(", ")}]`); // prettier-ignore
    } catch {
      lines.push(`${s}: absent`);
    }
  }
  const dir = join(fileURLToPath(new URL("../..", import.meta.url)), ".de-run", ex.prefix);
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "leftovers.txt"), lines.join("\n") + "\n");
  if (run.extras.metricsAddr !== undefined || ex.engineUrl !== undefined) {
    const base = ex.engineUrl ?? `http://${run.extras.metricsAddr!}`;
    try {
      writeFileSync(
        join(dir, "metrics.txt"),
        await (await fetch(new URL("/metrics", base))).text(),
      );
    } catch (e) {
      lines.push(`/metrics: ${e instanceof Error ? e.message : String(e)}`);
    }
  }
  if (!run.external) writeFileSync(join(dir, "engine.log"), run.log.join("\n") + "\n");
  console.warn(`\nprefix ${ex.prefix} after the run (also in ${dir}: leftovers.txt, metrics.txt, engine.log; the engine started ${String(run.starts)}×):\n  ${lines.join("\n  ")}\n`); // prettier-ignore
}

declare module "vitest" {
  interface ProvidedContext {
    runPrefix: string | null;
    rttFactor: number;
    engineControl: string | null;
    natsUrl: string | null;
    natsDirectUrl: string | null;
    natsProfile: ServerProfile;
    natsRttMs: number;
  }
}
