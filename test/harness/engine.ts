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
// integration suites skip, saying why (global-setup.ts). Nothing reaches beyond 127.0.0.1.

import { type ChildProcess, spawn } from "node:child_process";
import { inject } from "vitest";
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

import { EngineNotAvailableError } from "./engine-bin.js";
export { EngineNotAvailableError, enginePin, resolveEngineBinary } from "./engine-bin.js";

/** The library's worker/client options that are the engine's command line now. */
export interface EngineFlags {
  streamMaxBytes?: number;
  ackWaitMs?: number;
  maxWorkMs?: number;
  lease?: boolean;
  leaseTtlMs?: number;
  maintenanceIntervalMs?: number;
  reconcileGraceMs?: number;
}

function flagArgs(f: EngineFlags): string[] {
  const out: string[] = [];
  if (f.streamMaxBytes !== undefined) out.push("--stream-max-bytes", String(f.streamMaxBytes));
  if (f.ackWaitMs !== undefined) out.push("--ack-wait", `${f.ackWaitMs}ms`);
  if (f.maxWorkMs !== undefined) out.push("--max-work", `${f.maxWorkMs}ms`);
  if (f.lease === false) out.push("--no-lease");
  if (f.leaseTtlMs !== undefined) out.push("--lease-ttl", `${f.leaseTtlMs}ms`);
  if (f.maintenanceIntervalMs !== undefined) out.push("--maintenance-interval", `${f.maintenanceIntervalMs}ms`); // prettier-ignore
  if (f.reconcileGraceMs !== undefined) out.push("--reconcile-grace", `${f.reconcileGraceMs}ms`);
  return out;
}

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

let instances = 0;

/** One `durable-engine serve` process. */
export class EngineProcess {
  #proc: ChildProcess | undefined;
  readonly log: string[] = [];
  readonly instance: string;

  constructor(
    readonly bin: string,
    readonly url: string,
    readonly prefix: string,
    readonly flags: EngineFlags = {},
  ) {
    instances += 1;
    this.instance = `t${instances}`;
  }

  get args(): string[] {
    return ["serve", "--nats", this.url, "--prefix", this.prefix, "--instance", this.instance, ...flagArgs(this.flags)]; // prettier-ignore
  }

  /** Spawn and wait for the engine's `serving` line (it provisions the prefix first). */
  async start(): Promise<void> {
    const proc = spawn(this.bin, this.args, { stdio: ["ignore", "ignore", "pipe"] });
    this.#proc = proc;
    let buf = "";
    await new Promise<void>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error(`durable-engine serve did not come up in 30 s:\n${this.log.join("\n")}`)), 30_000); // prettier-ignore
      proc.stderr!.on("data", (d: Buffer) => {
        buf += d.toString("utf8");
        let i: number;
        while ((i = buf.indexOf("\n")) >= 0) {
          const line = buf.slice(0, i);
          buf = buf.slice(i + 1);
          this.log.push(line);
          if (this.log.length > 2_000) this.log.shift();
          if (process.env.DE_ENGINE_LOG === "1") console.error(`[engine ${this.prefix}] ${line}`);
          if (line.includes("msg=serving")) {
            clearTimeout(t);
            resolve();
          }
        }
      });
      proc.once("exit", (code) => {
        clearTimeout(t);
        reject(new Error(`durable-engine serve exited (${String(code)}):\n${this.log.join("\n")}`));
      });
    });
  }

  get running(): boolean {
    return this.#proc !== undefined && this.#proc.exitCode === null && this.#proc.signalCode === null; // prettier-ignore
  }

  async #signal(signal: "SIGTERM" | "SIGKILL"): Promise<void> {
    const proc = this.#proc;
    this.#proc = undefined;
    if (!proc || proc.exitCode !== null || proc.signalCode !== null) return;
    const exited = new Promise<void>((r) => proc.once("exit", () => r()));
    proc.kill(signal);
    await Promise.race([exited, delay(10_000)]);
    if (proc.exitCode === null && proc.signalCode === null) proc.kill("SIGKILL");
  }

  /** A graceful stop: the engine naks what it holds, so it redelivers at once. */
  stop(): Promise<void> {
    return this.#signal("SIGTERM");
  }

  /** A crash: its leases are lost, its items redeliver at the ack wait. */
  kill(): Promise<void> {
    return this.#signal("SIGKILL");
  }
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
export function ensureEngine(prefix: string, flags: EngineFlags = {}): Promise<EngineProcess> {
  const url = serverUrl();
  const key = `${url}|${prefix}`;
  let p = engines.get(key);
  if (p === undefined) {
    const bin = inject("engineBin");
    if (!bin) return Promise.reject(new EngineNotAvailableError("no engine binary"));
    const e = new EngineProcess(bin, url, prefix, flags);
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

function flagsOf(o: EngineFlags): EngineFlags {
  const f: EngineFlags = {};
  if (o.streamMaxBytes !== undefined) f.streamMaxBytes = o.streamMaxBytes;
  if (o.ackWaitMs !== undefined) f.ackWaitMs = o.ackWaitMs;
  if (o.maxWorkMs !== undefined) f.maxWorkMs = o.maxWorkMs;
  if (o.lease !== undefined) f.lease = o.lease;
  if (o.leaseTtlMs !== undefined) f.leaseTtlMs = o.leaseTtlMs;
  if (o.maintenanceIntervalMs !== undefined) f.maintenanceIntervalMs = o.maintenanceIntervalMs;
  if (o.reconcileGraceMs !== undefined) f.reconcileGraceMs = o.reconcileGraceMs;
  return f;
}

/** The SDK's createWorker, with the prefix's engine started before the worker's first poll. */
export function createWorker(opts: WorkerOptions): Worker {
  const w = sdkCreateWorker(opts);
  return {
    async start(): Promise<void> {
      await ensureEngine(opts.prefix ?? "de", flagsOf(opts));
      await w.start();
    },
    stop: () => w.stop(),
    health: () => w.health(),
  };
}

/** The SDK's createClient, with the prefix's engine started before the client's first request. */
export function createClient(opts: ClientOptions): Client {
  const c = sdkCreateClient(opts);
  const ready = (): Promise<EngineProcess> => ensureEngine(opts.prefix ?? "de", flagsOf(opts));
  const wrap = <O>(h: Handle<O>): Handle<O> => ({
    runId: h.runId,
    async result(timeoutMs?: number): Promise<O> {
      await ready();
      return h.result(timeoutMs);
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
