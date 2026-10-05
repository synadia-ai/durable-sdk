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

import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { EngineNotAvailableError, enginePin, resolveEngineBinary, runLicence } from "./engine-bin.js"; // prettier-ignore
import { randomBytes } from "node:crypto";
import {
  connect,
  connectAdmin,
  deleteRunBlobs,
  deletePrefixStreams,
  durableOf,
  measureThroughput,
  sweepRun,
  existingFromEnv,
  measureRtt,
  prefixEphemeralConsumers,
  prefixStreams,
  rttFactor,
  scopeOf,
  snapshot,
  type ExistingServer,
} from "./existing.js";
import { HEADROOM as HEADROOM_NOTE } from "./budget.js";
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
  ctx.provide("throughputBps", 0);
  ctx.provide("natsMaxPayload", 0);
  ctx.provide("runTenant", null);
  ctx.provide("licenceFile", null);
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
  // A standalone engine starts only with a licence (F42): one for the run, minted here.
  const licence = provideLicence(ctx, engineBin);
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
      licence.remove();
    }
  };
}

/**
 * The run's licence for its standalone engines (F42): the caller's `DE_LICENCE`, or a test licence
 * minted once for the run (`durable-licence mint -test -name durable-sdk-tests`), removed at the end.
 */
function provideLicence(ctx: SetupContext, engineBin: string): { file: string; remove(): void } {
  const lic = runLicence(engineBin);
  console.warn(lic.minted ? `licence: a test licence minted for the run, ${lic.file}\n` : `licence: DE_LICENCE ${lic.file}\n`); // prettier-ignore
  ctx.provide("licenceFile", lic.file);
  return {
    file: lic.file,
    remove: () => {
      if (lic.minted) rmSync(dirname(lic.file), { recursive: true, force: true });
    },
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
  const licence = engineBin !== null ? provideLicence(ctx, engineBin) : undefined;
  const scope = scopeOf(ex.prefix);
  // The tests' identity, for the round trip, the probe and the engine's API; the admin connection
  // (DE_ADMIN_URL, the tests' own by default) for the streams: as a tenant the tests see none.
  const tnc = await connect({ servers: ex.url, name: "test-harness-run" });
  const nc = ex.adminUrl === ex.url && ex.adminCreds === ex.creds ? tnc : await connectAdmin({ name: "test-harness-admin" }); // prettier-ignore
  // JetStream first: a server or an account without it fails here, by name, not in every test.
  const { jetstreamManager } = await import("@nats-io/jetstream");
  await (await jetstreamManager(nc)).getAccountInfo();
  const serverRtt = await measureRtt(tnc);
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
    ...(licence !== undefined ? { licence: licence.file } : {}),
  });
  console.warn(
    `\nexisting server ${ex.url}${ex.creds !== undefined ? " (creds)" : ""}, prefix ${ex.prefix}${ex.tenant !== undefined ? `, as tenant ${ex.tenant} (the streams through ${ex.adminUrl}${ex.adminCreds !== undefined ? ", creds" : ""})` : ""}, ` +
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
      if (tnc !== nc) await tnc.close();
      await proxy?.stop();
      throw e;
    }
  } else {
    const r = await fetch(new URL("/healthz", ex.engineUrl));
    if (!r.ok) throw new Error(`DE_ENGINE_URL ${ex.engineUrl!}: /healthz answered ${String(r.status)}`); // prettier-ignore
  }
  await run.listen();
  // The throughput the large values meet, through the engine on the tests' link (the proxy's
  // under DE_TEST_RTT_MS), once: the large-value tests' budgets carry its term (harness/mode.ts).
  const probeRun = `probe-${randomBytes(4).toString("hex")}`;
  run.runIds.add(probeRun);
  const pnc = proxy ? await connect({ servers: proxy.url, name: "test-harness-probe" }) : tnc;
  const tp = await measureThroughput(pnc, ex.prefix, probeRun);
  const maxPayload = pnc.info?.max_payload ?? 0;
  if (pnc !== tnc) await pnc.close();
  await deleteRunBlobs(nc, scope, [probeRun]);
  const mib = (b: number): string => (b / (1024 * 1024)).toFixed(2);
  console.warn(
    `round trip ${rttMs.toFixed(1)} ms, throughput ${mib(tp.bps)} MiB/s (a ${mib(tp.bytes)} MiB put ${tp.putMs.toFixed(0)} ms and get ${tp.getMs.toFixed(0)} ms through the engine), max_payload ${mib(maxPayload)} MiB → a large value's budget adds its bytes over the throughput × ${String(HEADROOM_NOTE)}\n`, // prettier-ignore
  );
  // What the prefix held before the run: another client's consumers on a shared prefix are not
  // this run's leftovers.
  const atStart = await snapshot(nc, scope);
  ctx.provide("engineBin", engineBin ?? "external");
  ctx.provide("natsUrl", proxy?.url ?? ex.url);
  ctx.provide("natsDirectUrl", ex.url);
  ctx.provide("natsProfile", "none");
  ctx.provide("natsRttMs", Math.round(rttMs));
  ctx.provide("runPrefix", ex.prefix);
  ctx.provide("rttFactor", factor);
  ctx.provide("engineControl", run.controlUrl);
  ctx.provide("throughputBps", Math.round(tp.bps));
  ctx.provide("natsMaxPayload", maxPayload);
  ctx.provide("runTenant", ex.tenant ?? null);
  return async () => {
    try {
      // The footprint check, on the prefix's streams only.
      const left = await prefixEphemeralConsumers(nc, scope, atStart.consumers);
      if (left.length > 0) throw new Error(leftoverMessage(left, "after the suite"));
    } finally {
      const metrics = await engineMetrics(ex, run);
      await run.stop();
      licence?.remove();
      // The engine stopped, nothing re-creates a durable or writes a blob: what a test that failed
      // or timed out left after its own cleanup goes now (docs/findings.md F33).
      const swept = await sweepRun(nc, scope, run.runIds, atStart);
      // Under DE_ENGINE_URL the engine still runs: a poll a late-stopped worker had in flight can
      // bind a durable after the sweep (a tenant run left four so). Again after a pause, until a
      // round finds nothing (docs/findings.md F38).
      for (let round = 0; run.external && round < 3; round++) {
        await new Promise((r) => setTimeout(r, 2_000 * factor));
        const again = await sweepRun(nc, scope, run.runIds, atStart);
        swept.blobs += again.blobs;
        swept.durables += again.durables;
        if (again.blobs + again.durables === 0) break;
      }
      console.warn(`the run's end: ${String(swept.blobs)} blobs of its ${String(run.runIds.size)} runs and ${String(swept.durables)} durables deleted`); // prettier-ignore
      await report(nc, ex, run, metrics);
      if (ex.deleteAtEnd) {
        const gone = await deletePrefixStreams(nc, ex.prefix);
        console.warn(`DE_CLEANUP=${process.env.DE_CLEANUP || "delete (the default for a generated prefix)"}: deleted ${gone.join(", ") || "nothing"}`); // prettier-ignore
      }
      await nc.close();
      if (tnc !== nc) await tnc.close();
      await proxy?.stop();
    }
  };
}

/** What the run leaves for the reader: the prefix's leftovers and the engine's /metrics. */
/** The engine's /metrics text (or why not), read while it still runs. */
async function engineMetrics(
  ex: ExistingServer,
  run: RunEngine,
): Promise<string | Error | undefined> {
  // prettier-ignore
  if (run.extras.metricsAddr === undefined && ex.engineUrl === undefined) return undefined;
  const base = ex.engineUrl ?? `http://${run.extras.metricsAddr!}`;
  try {
    return await (await fetch(new URL("/metrics", base))).text();
  } catch (e) {
    return e instanceof Error ? e : new Error(String(e));
  }
}

async function report(
  nc: Awaited<ReturnType<typeof connect>>,
  ex: ExistingServer,
  run: RunEngine,
  metrics: string | Error | undefined,
): Promise<void> {
  const { jetstreamManager } = await import("@nats-io/jetstream");
  const jsm = await jetstreamManager(nc);
  const lines: string[] = [];
  for (const s of prefixStreams(ex.prefix)) {
    try {
      const si = await jsm.streams.info(s);
      const durables: string[] = [];
      for await (const c of jsm.consumers.list(s)) if (durableOf(scopeOf(ex.prefix), c.name)) durables.push(c.name); // prettier-ignore
      lines.push(`${s}: ${String(si.state.messages)} messages, ${String(si.state.bytes)} bytes, consumers [${durables.join(", ")}]`); // prettier-ignore
    } catch {
      lines.push(`${s}: absent`);
    }
  }
  const dir = join(fileURLToPath(new URL("../..", import.meta.url)), ".de-run", ex.prefix);
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "leftovers.txt"), lines.join("\n") + "\n");
  if (typeof metrics === "string") writeFileSync(join(dir, "metrics.txt"), metrics);
  else if (metrics !== undefined) lines.push(`/metrics: ${metrics.message}`);
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
    /** Against an existing server: the probe's bytes per second (0 elsewhere: not measured). */
    throughputBps: number;
    /** Against an existing server: the tests' connection's max_payload (0 elsewhere). */
    natsMaxPayload: number;
    /** DE_TENANT, or null. */
    runTenant: string | null;
    /** The licence of the run's standalone engines (F42), or null without an engine. */
    licenceFile: string | null;
  }
}
