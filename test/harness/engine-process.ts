// One `durable-engine serve` process, vitest-free: engine.ts starts one per (server, prefix) for
// the spawned server; run-engine.ts (global-setup.ts) starts the one engine of a run against an
// existing server (existing.ts).

import { type ChildProcess, spawn } from "node:child_process";

/** The library's worker/client options that are the engine's command line now. */
export interface EngineFlags {
  streamMaxBytes?: number;
  ackWaitMs?: number;
  maxWorkMs?: number;
  lease?: boolean;
  leaseTtlMs?: number;
  maintenanceIntervalMs?: number;
  reconcileGraceMs?: number;
  /** `serve --offload-threshold` (the engine's F22): a test that wants the engine to offload small values. */
  offloadThreshold?: number;
}

export function flagArgs(f: EngineFlags): string[] {
  const out: string[] = [];
  if (f.streamMaxBytes !== undefined) out.push("--stream-max-bytes", String(f.streamMaxBytes));
  if (f.ackWaitMs !== undefined) out.push("--ack-wait", `${f.ackWaitMs}ms`);
  if (f.maxWorkMs !== undefined) out.push("--max-work", `${f.maxWorkMs}ms`);
  if (f.lease === false) out.push("--no-lease");
  if (f.leaseTtlMs !== undefined) out.push("--lease-ttl", `${f.leaseTtlMs}ms`);
  if (f.maintenanceIntervalMs !== undefined) out.push("--maintenance-interval", `${f.maintenanceIntervalMs}ms`); // prettier-ignore
  if (f.reconcileGraceMs !== undefined) out.push("--reconcile-grace", `${f.reconcileGraceMs}ms`);
  if (f.offloadThreshold !== undefined) out.push("--offload-threshold", String(f.offloadThreshold));
  return out;
}

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

let instances = 0;

/** What an engine of the existing-server mode adds to its command line. */
export interface EngineExtras {
  creds?: string;
  metricsAddr?: string;
  /** How long `serve` may take to provision and come up (default 30 s). */
  startTimeoutMs?: number;
  /** Every stderr line, as it comes. */
  onLine?: (line: string) => void;
  /**
   * `DE_LICENCE` for `serve` (a standalone engine starts only with a licence; F42): a file, or
   * `null` for none (the fabric mode needs none, and one given would bound its tenants). Absent:
   * the run's, which global-setup.ts put in the environment.
   */
  licence?: string | null;
  /** Arguments before `--prefix` (`--tenancy`, `--tenant-max-bytes 0`, …). */
  args?: string[];
}

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
    readonly extras: EngineExtras = {},
  ) {
    instances += 1;
    this.instance = `t${instances}`;
  }

  get args(): string[] {
    const extra: string[] = [];
    if (this.extras.creds !== undefined) extra.push("--creds", this.extras.creds);
    if (this.extras.metricsAddr !== undefined) extra.push("--metrics-addr", this.extras.metricsAddr); // prettier-ignore
    extra.push(...(this.extras.args ?? []));
    return ["serve", "--nats", this.url, ...extra, "--prefix", this.prefix, "--instance", this.instance, ...flagArgs(this.flags)]; // prettier-ignore
  }

  /** Spawn and wait for the engine's `serving` line (it provisions the prefix first). */
  async start(): Promise<void> {
    const env = { ...process.env };
    if (this.extras.licence === null) delete env.DE_LICENCE;
    else if (this.extras.licence !== undefined) env.DE_LICENCE = this.extras.licence;
    const proc = spawn(this.bin, this.args, { stdio: ["ignore", "ignore", "pipe"], env });
    this.#proc = proc;
    let buf = "";
    const limit = this.extras.startTimeoutMs ?? 30_000;
    await new Promise<void>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error(`durable-engine serve did not come up in ${String(limit / 1000)} s:\n${this.log.join("\n")}`)), limit); // prettier-ignore
      proc.stderr.on("data", (d: Buffer) => {
        buf += d.toString("utf8");
        let i: number;
        while ((i = buf.indexOf("\n")) >= 0) {
          const line = buf.slice(0, i);
          buf = buf.slice(i + 1);
          this.log.push(line);
          if (this.log.length > 2_000) this.log.shift();
          this.extras.onLine?.(line);
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
