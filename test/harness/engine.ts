// The one thing this harness adds to the library's: `durable-engine serve`, next to the spawned
// nats-server. The library's suites build a worker or a client on a prefix of their own and expect
// the prefix to work; here the prefix needs an engine, so the suites import `createWorker` and
// `createClient` from this module, which start one `serve` per (server, prefix) on first use and
// hand the call on to the SDK's. The library's per-worker options that are the engine's flags now
// (`ackWaitMs` → `--ack-wait`, `lease: false` → `--no-lease`, …) become the engine's command line.
// setup-each.ts stops every engine after each test, before it deletes the streams.
//
// The binary: `DE_ENGINE` (a path), or built with `go build -o` from the engine checkout
// (`DE_ENGINE_REPO`, default ../durable-engine) at the commit `engine.pin` names. A checkout at
// another commit, or with local changes, is refused unless `DE_ENGINE_ANY=1`. Without either the
// integration suites skip, saying why (global-setup.ts). Nothing reaches beyond 127.0.0.1 unless
// DE_NATS_URL says so.
//
// Against an existing server (existing.ts, `DE_NATS_URL`) there is one prefix and one engine for
// the run instead: every prefix a test names is the run's (`mapPrefix`), the engine is
// run-engine.ts's in the main process, and a test's flags restart it only when they differ from
// the ones it runs with. Its counters (`engineCounts`) are the test's own: the difference from
// the test's start, as a fresh engine per prefix gave them.

import { expect, inject } from "vitest";
import { getCurrentTest } from "vitest/suite";
import {
  createClient as sdkCreateClient,
  type Client,
  type ClientOptions,
} from "../../src/engine/client.js";
import {
  createWorker as sdkCreateWorker,
  type Worker,
  type WorkerOptions,
} from "../../src/engine/worker.js";
import { type Handle } from "../../src/types.js";
import { type DurableEventFields, type DurableEventName, type Metrics } from "../../src/internal/metrics.js"; // prettier-ignore

import { EngineNotAvailableError } from "./engine-bin.js";
import { EngineProcess, type EngineFlags } from "./engine-process.js";
export { EngineNotAvailableError, enginePin, resolveEngineBinary } from "./engine-bin.js";
export { EngineProcess, type EngineFlags } from "./engine-process.js";

/** The run's prefix against an existing server; elsewhere the prefix the test named. */
export function mapPrefix(prefix: string): string {
  return inject("runPrefix") ?? prefix;
}

/** True against an existing server (existing.ts). */
export const existingMode = (): boolean => inject("runPrefix") != null;

/** Skip the running test, saying why: what the mode cannot do. */
function skipInMode(why: string): never {
  console.warn(`skipped in the existing-server mode: ${expect.getState().currentTestName ?? "?"} — ${why}`); // prettier-ignore
  getCurrentTest()?.context.skip();
  throw new Error(why); // unreachable: skip() throws
}

async function control<T>(path: string, body?: unknown): Promise<T> {
  const base = inject("engineControl")!;
  const r = await fetch(new URL(path, base), body === undefined ? {} : { method: "POST", body: JSON.stringify(body) }); // prettier-ignore
  const v = (await r.json()) as T & { error?: string };
  if (r.status === 409 && v.error?.includes("DE_ENGINE_URL") === true) skipInMode(v.error);
  if (!r.ok) throw new Error(`the run's engine: ${v.error ?? String(r.status)}`);
  return v;
}

// The current test against an existing server: its flags are fixed by its first call, as the
// first worker or client on a prefix fixed a spawned engine's.
let testFlagsFixed = false;
let testCounts: { instance: string; counts: Record<string, number> } | undefined;
let testLogFrom = 0;

/** setup-each.ts: the run ids a test touched, for the run's end to sweep (existing.ts `sweepRun`). */
export async function reportRunIds(runIds: string[]): Promise<void> {
  if (runIds.length > 0) await control("/runs", { runIds });
}

/** setup-each.ts, before each test in the mode: the counters the test's are measured from. */
export async function beginTest(nc: {
  request(
    subject: string,
    data?: Uint8Array,
    opts?: { timeout: number },
  ): Promise<{ json<T>(): T }>;
}): Promise<void> {
  // prettier-ignore
  testFlagsFixed = false;
  testCounts = undefined;
  blobBytes = { put: 0, get: 0 };
  testLogFrom = (await control<{ next: number }>(`/log?since=${String(Number.MAX_SAFE_INTEGER)}`)).next; // prettier-ignore
  try {
    const r = await nc.request(`${inject("runPrefix")!}.api.v1.info`, new TextEncoder().encode("{}"), { timeout: 5_000 * inject("rttFactor") }); // prettier-ignore
    const info = r.json<{ instance?: string; counts?: Record<string, number> }>();
    testCounts = { instance: info.instance ?? "", counts: info.counts ?? {} };
  } catch {
    /* no engine running right now (a test before stopped it): the next one starts at zero */
  }
}

async function ensureRunEngine(flags: EngineFlags): Promise<void> {
  if (testFlagsFixed) return;
  await control("/ensure", { flags });
  testFlagsFixed = true;
}

const engines = new Map<string, Promise<EngineProcess>>();
let serverOverride: string | undefined;

/**
 * The server the engines of the current test connect to: the suite's shared server by default
 * (never the delay proxy: the engine sits next to NATS), or the one a suite spawned itself.
 */
export function setEngineServer(url: string | undefined): void {
  serverOverride = url;
}

function serverUrl(): string {
  const url = serverOverride ?? inject("natsDirectUrl");
  if (!url) throw new EngineNotAvailableError("no nats-server for the engine");
  return url;
}

/** The engine serving `prefix` on the current server, started on first use with `flags`. */
export function ensureEngine(prefix: string, flags: EngineFlags = {}): Promise<unknown> {
  if (existingMode()) return ensureRunEngine(flags);
  const url = serverUrl();
  const key = `${url}|${prefix}`;
  let p = engines.get(key);
  if (p === undefined) {
    const bin = inject("engineBin");
    if (!bin) return Promise.reject(new EngineNotAvailableError("no engine binary"));
    const licence = inject("licenceFile");
    const e = new EngineProcess(bin, url, prefix, flags, licence !== null ? { licence } : {});
    p = e.start().then(() => e);
    engines.set(key, p);
    p.catch(() => engines.delete(key));
  }
  return p;
}

/** Every engine started so far on `prefix` (the current server). */
export async function engineFor(prefix: string): Promise<EngineProcess | undefined> {
  return engines.get(`${serverUrl()}|${prefix}`);
}

/** Stop every engine (setup-each.ts, after each test). */
export async function stopEngines(): Promise<void> {
  const all = [...engines.values()];
  engines.clear();
  await Promise.allSettled(all.map(async (p) => (await p).stop()));
}

/** `DE_SDK_LOG=1`: a worker without a logger of its own logs to the console (debugging a suite). */
const consoleLogger = {
  debug: (m: string, f?: Record<string, unknown>) => console.error(`[sdk debug] ${m}`, f ?? ""),
  info: (m: string, f?: Record<string, unknown>) => console.error(`[sdk info] ${m}`, f ?? ""),
  warn: (m: string, f?: Record<string, unknown>) => console.error(`[sdk warn] ${m}`, f ?? ""),
  error: (m: string, f?: Record<string, unknown>) => console.error(`[sdk error] ${m}`, f ?? ""),
};

function flagsOf(o: EngineFlags): EngineFlags {
  const f: EngineFlags = {};
  if (o.streamMaxBytes !== undefined) f.streamMaxBytes = o.streamMaxBytes;
  if (o.ackWaitMs !== undefined) f.ackWaitMs = o.ackWaitMs;
  if (o.maxWorkMs !== undefined) f.maxWorkMs = o.maxWorkMs;
  if (o.lease !== undefined) f.lease = o.lease;
  if (o.leaseTtlMs !== undefined) f.leaseTtlMs = o.leaseTtlMs;
  if (o.maintenanceIntervalMs !== undefined) f.maintenanceIntervalMs = o.maintenanceIntervalMs;
  if (o.reconcileGraceMs !== undefined) f.reconcileGraceMs = o.reconcileGraceMs;
  if (o.offloadThreshold !== undefined) f.offloadThreshold = o.offloadThreshold;
  return f;
}

/** The bytes the current test's clients and workers moved through `put` and `get` (setup-each.ts
 * writes them down after each test in the mode: what a large-value budget counts). */
let blobBytes = { put: 0, get: 0 };

export function testBlobBytes(): { put: number; get: number } {
  const b = blobBytes;
  blobBytes = { put: 0, get: 0 };
  return b;
}

/** The test's metrics sink, if any, with the blob tally in front of it. */
function tallied<O extends { metrics?: Metrics }>(opts: O): O {
  const inner = opts.metrics;
  return {
    ...opts,
    metrics: {
      onEvent(name: DurableEventName, fields: DurableEventFields): void {
        if ((name === "blob.put" || name === "blob.get") && typeof fields.bytes === "number") {
          if (name === "blob.put") blobBytes.put += fields.bytes;
          else blobBytes.get += fields.bytes;
        }
        inner?.onEvent(name, fields);
      },
    },
  };
}

/** The workers the current test started and did not stop, with their workflows' names. */
const testWorkers = new Set<{ stop(): Promise<void>; names: string[] }>();
/** The workflow-name sets of every worker the current test started, stopped or not. */
let testWorkflowSets: string[][] = [];

/** The SDK's createWorker, with the prefix's engine started before the worker's first poll. */
export function createWorker(opts: WorkerOptions & EngineFlags): Worker {
  if (opts.prefix !== undefined || existingMode()) opts = { ...opts, prefix: mapPrefix(opts.prefix ?? "de") }; // prettier-ignore
  const w = sdkCreateWorker(tallied(process.env.DE_SDK_LOG === "1" && opts.logger === undefined ? { ...opts, logger: consoleLogger } : opts)); // prettier-ignore
  const live = { stop: () => w.stop(), names: [...new Set(opts.workflows.map((wf) => wf.name))].sort() }; // prettier-ignore
  return {
    async start(): Promise<void> {
      await ensureEngine(opts.prefix ?? "de", flagsOf(opts));
      testWorkers.add(live);
      testWorkflowSets.push(live.names);
      await w.start();
    },
    async stop(): Promise<void> {
      testWorkers.delete(live);
      await w.stop();
    },
    health: () => w.health(),
  };
}

/**
 * setup-each.ts, after each test in the mode, before its cleanup (docs/findings.md F33): a worker
 * the test left running (it failed, or timed out and its body still runs) is stopped, bounded; and
 * every waiting poll for each of the test's workflow sets is withdrawn (`poll {cancelPoll}`), so
 * the engine holds no pending fetch that would re-create the durables the cleanup deletes.
 */
export async function endTestWorkers(nc: {
  request(subject: string, data?: Uint8Array, opts?: { timeout: number }): Promise<unknown>;
}): Promise<void> {
  const factor = inject("rttFactor");
  const left = [...testWorkers];
  testWorkers.clear();
  const bounded = (p: Promise<unknown>): Promise<unknown> =>
    Promise.race([p.catch(() => undefined), new Promise((r) => setTimeout(r, 5_000 * factor))]);
  await Promise.all(left.map((w) => bounded(w.stop())));
  const sets = new Map(testWorkflowSets.map((n) => [n.join(","), n]));
  testWorkflowSets = [];
  const subject = `${inject("runPrefix")!}.api.v1.poll`;
  await Promise.all(
    [...sets.values()].map(
      (names) =>
      bounded(nc.request(subject, new TextEncoder().encode(JSON.stringify({ workflows: names.map((name) => ({ name, versions: [1] })), cancelPoll: true })), { timeout: 5_000 * factor })), // prettier-ignore
    ),
  );
}

/** The SDK's createClient, with the prefix's engine started before the client's first request. */
export function createClient(opts: ClientOptions & EngineFlags): Client {
  if (opts.prefix !== undefined || existingMode()) opts = { ...opts, prefix: mapPrefix(opts.prefix ?? "de") }; // prettier-ignore
  const c = sdkCreateClient(tallied(opts));
  // Against an existing server every wait is scaled by the measured round trip (existing.ts).
  const scale = (ms: number | undefined): number | undefined => (ms === undefined ? ms : ms * inject("rttFactor")); // prettier-ignore
  const ready = (): Promise<unknown> => ensureEngine(opts.prefix ?? "de", flagsOf(opts));
  const wrap = <O>(h: Handle<O>): Handle<O> => ({
    runId: h.runId,
    async result(timeoutMs?: number): Promise<O> {
      await ready();
      return h.result(scale(timeoutMs));
    },
    async status() {
      await ready();
      return h.status();
    },
  });
  return {
    async start(workflow, input, o) {
      await ready();
      return wrap(await c.start(workflow, input, o));
    },
    get: (runId) => wrap(c.get(runId)),
    async signal(runId, name, data) {
      await ready();
      return c.signal(runId, name, data);
    },
    async cancel(runId, o) {
      await ready();
      return c.cancel(runId, o);
    },
  };
}

/**
 * The library's `provision(nc, names, opts)`, re-expressed: provisioning is the engine's, at its
 * start. A suite that provisioned a prefix to stage a journal by hand starts the prefix's engine.
 */
export async function provision(
  _nc: unknown,
  n: { prefix: string },
  opts: { streamMaxBytes?: number } = {},
): Promise<void> {
  await ensureEngine(n.prefix, opts.streamMaxBytes !== undefined ? { streamMaxBytes: opts.streamMaxBytes } : {}); // prettier-ignore
}

declare module "vitest" {
  interface ProvidedContext {
    engineBin: string | null;
  }
}

/**
 * The engine's own counters (`info`'s `counts`, its /metrics numbers by series name), for a suite
 * whose assertion was about what the library's worker did and is now the engine's: a terminal
 * re-drive, an item pulled for a run that had already ended. One engine per prefix, so the
 * counts are the prefix's.
 */
export async function engineCounts(
  nc: { request(subject: string, data?: Uint8Array, opts?: { timeout: number }): Promise<{ json<T>(): T }> }, // prettier-ignore
  prefix: string,
): Promise<Record<string, number>> {
  const r = await nc.request(`${mapPrefix(prefix)}.api.v1.info`, new TextEncoder().encode("{}"), { timeout: 5_000 * inject("rttFactor") }); // prettier-ignore
  const info = r.json<{ instance?: string; counts?: Record<string, number> }>();
  const counts = info.counts ?? {};
  // One engine for the run: the test's own counts are the difference from its start (beginTest),
  // unless the engine was restarted since, when they start at zero as they did per prefix.
  if (!existingMode() || testCounts === undefined || testCounts.instance !== info.instance) return counts; // prettier-ignore
  const base = testCounts.counts;
  return Object.fromEntries(Object.entries(counts).map(([k, v]) => [k, v - (base[k] ?? 0)]));
}

/** The sum of every series of `name` whose labels include all of `labels`. */
export function countOf(
  counts: Record<string, number>,
  name: string,
  labels: Record<string, string> = {},
): number {
  // prettier-ignore
  let n = 0;
  for (const [k, v] of Object.entries(counts)) {
    if (k !== name && !k.startsWith(`${name}{`)) continue;
    if (Object.entries(labels).every(([l, val]) => k.includes(`${l}="${val}"`))) n += v;
  }
  return n;
}

/** The log lines of the engine serving `prefix` (its stderr, as `durable-engine serve` writes it). */
export async function engineLog(prefix: string): Promise<string[]> {
  // Under DE_ENGINE_URL the engine's log is its operator's, not the harness's: a test that reads
  // it is skipped, saying so (it was an empty log, and the assertion's failure, before).
  if (existingMode() && inject("engineBin") === "external") skipInMode("the engine is DE_ENGINE_URL's: its log is not the harness's to read"); // prettier-ignore
  // The run's one engine: the lines since the test began, as a test's own engine gave them.
  if (existingMode()) return (await control<{ lines: string[] }>(`/log?since=${String(testLogFrom)}`)).lines; // prettier-ignore
  const e = await engineFor(prefix);
  return e ? [...e.log] : [];
}

/**
 * Stop the engine serving `prefix` (gracefully, or `kill: true` for a crash); the next worker or
 * client on the prefix starts a fresh one, which provisions what is missing — the library's "a
 * restarted worker re-provisions", now the engine's.
 */
export async function restartEngine(prefix: string, opts: { kill?: boolean } = {}): Promise<void> {
  if (existingMode()) {
    await control("/halt", { kill: opts.kill === true });
    testFlagsFixed = false; // the next worker or client starts it again, with its flags
    return;
  }
  const key = `${serverUrl()}|${prefix}`;
  const p = engines.get(key);
  engines.delete(key);
  if (p === undefined) return;
  const e = await p;
  await (opts.kill === true ? e.kill() : e.stop());
}
