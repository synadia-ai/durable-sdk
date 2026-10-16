// The ONE engine of a run against an existing server (existing.ts), owned by global-setup.ts in
// vitest's main process, so it outlives the per-file test workers. A test reaches it through a
// small HTTP control channel on 127.0.0.1 (engine.ts): `ensure` with the test's engine flags (the
// library's per-worker options that are `serve` flags now), which restarts the engine only when
// they differ from the ones it runs with; `stop` (graceful or a kill, a test's restartEngine);
// `log`. Under `DE_ENGINE_URL` the engine is not the harness's: it is never started, stopped or
// restarted, and a test that needs other flags or a restart is skipped, saying so.

import { createServer, type Server } from "node:http";
import { EngineProcess, type EngineFlags, type EngineExtras } from "./engine-process.js";

/** Flags as the run's engine takes them: `streamMaxBytes` dropped (the prefix's eight are sized
 * once, by the operator or the first provisioning, never resized by a test against a server the
 * run does not own), the rest in a stable order. */
export function runFlags(f: EngineFlags): EngineFlags {
  const out: EngineFlags = {};
  if (f.ackWaitMs !== undefined) out.ackWaitMs = f.ackWaitMs;
  if (f.maxWorkMs !== undefined) out.maxWorkMs = f.maxWorkMs;
  if (f.lease === false) out.lease = false;
  if (f.leaseTtlMs !== undefined) out.leaseTtlMs = f.leaseTtlMs;
  if (f.maintenanceIntervalMs !== undefined) out.maintenanceIntervalMs = f.maintenanceIntervalMs;
  if (f.reconcileGraceMs !== undefined) out.reconcileGraceMs = f.reconcileGraceMs;
  if (f.offloadThreshold !== undefined) out.offloadThreshold = f.offloadThreshold;
  if (f.noJournalMaxAgeMs !== undefined) out.noJournalMaxAgeMs = f.noJournalMaxAgeMs;
  return out;
}

const same = (a: EngineFlags, b: EngineFlags): boolean => JSON.stringify(a) === JSON.stringify(b);

export class RunEngine {
  #current: EngineProcess | undefined;
  #flags: EngineFlags = {};
  #log: string[] = [];
  #dropped = 0; // lines trimmed off the front of #log: a line's index is its position + this
  #queue: Promise<unknown> = Promise.resolve();
  #http: Server | undefined;
  #port = 0;
  starts = 0;
  /** Every run id the run's tests touched (setup-each.ts reports them): the end's sweep deletes
   * their blobs by name (existing.ts `sweepRun`). */
  readonly runIds = new Set<string>();

  constructor(
    readonly bin: string | null,
    readonly url: string,
    readonly prefix: string,
    readonly extras: EngineExtras,
  ) {
    this.extras = { ...extras, onLine: (l) => this.#line(l) };
  }

  #line(l: string): void {
    this.#log.push(l);
    if (this.#log.length > 20_000) {
      this.#log.splice(0, 10_000);
      this.#dropped += 10_000;
    }
  }

  /** The log lines from index `since` on (every engine of the run, in order), and the next index. */
  logSince(since: number): { lines: string[]; next: number } {
    return { lines: this.#log.slice(Math.max(0, since - this.#dropped)), next: this.#dropped + this.#log.length }; // prettier-ignore
  }

  get external(): boolean {
    return this.bin === null;
  }

  get controlUrl(): string {
    return `http://127.0.0.1:${String(this.#port)}`;
  }

  get log(): string[] {
    return [...this.#log];
  }

  /** One request at a time: a restart must not race another. */
  #serial<T>(fn: () => Promise<T>): Promise<T> {
    const p = this.#queue.then(fn, fn);
    this.#queue = p.catch(() => undefined);
    return p;
  }

  /** The engine running with `flags`: started if it is not, restarted if its flags differ. */
  ensure(flags: EngineFlags): Promise<{ instance: string; restarted: boolean }> {
    return this.#serial(async () => {
      const want = runFlags(flags);
      if (this.external) {
        if (Object.keys(want).length > 0) {
          throw new Error(`the engine is DE_ENGINE_URL's, not the harness's: it cannot take ${JSON.stringify(want)}`); // prettier-ignore
        }
        return { instance: "external", restarted: false };
      }
      const cur = this.#current;
      if (cur?.running && same(this.#flags, want)) return { instance: cur.instance, restarted: false }; // prettier-ignore
      if (cur) await cur.stop();
      const e = new EngineProcess(this.bin!, this.url, this.prefix, want, this.extras);
      this.#current = e;
      this.#flags = want;
      this.starts += 1;
      await e.start();
      return { instance: e.instance, restarted: cur !== undefined };
    });
  }

  /** A test's restartEngine: graceful, or a kill (a crash). The next `ensure` starts a fresh one. */
  halt(kill: boolean): Promise<void> {
    return this.#serial(async () => {
      if (this.external) throw new Error("the engine is DE_ENGINE_URL's, not the harness's: it cannot be restarted"); // prettier-ignore
      const cur = this.#current;
      if (!cur) return;
      await (kill ? cur.kill() : cur.stop());
      this.#current = undefined;
    });
  }

  async stop(): Promise<void> {
    await new Promise<void>((r) => (this.#http ? this.#http.close(() => r()) : r()));
    if (this.#current) await this.#current.stop();
    this.#current = undefined;
  }

  /** The control channel the test workers use (engine.ts). */
  listen(): Promise<void> {
    const server = createServer((req, res) => {
      let body = "";
      req.on("data", (d: Buffer) => (body += d.toString("utf8")));
      req.on("end", () => {
        const reply = (code: number, v: unknown): void => {
          res.writeHead(code, { "content-type": "application/json" });
          res.end(JSON.stringify(v));
        };
        const parsed = (body ? JSON.parse(body) : {}) as { flags?: EngineFlags; kill?: boolean; runIds?: string[] }; // prettier-ignore
        const route = `${req.method ?? ""} ${req.url ?? ""}`;
        const done =
          route === "POST /ensure"
            ? this.ensure(parsed.flags ?? {})
            : route === "POST /halt"
              ? this.halt(parsed.kill === true).then(() => ({}))
              : route === "POST /runs"
                ? Promise.resolve((parsed.runIds ?? []).forEach((r) => this.runIds.add(r))).then(() => ({})) // prettier-ignore
                : route.startsWith("GET /log")
                  ? Promise.resolve(this.logSince(Number(new URL(req.url ?? "", "http://x").searchParams.get("since") ?? 0))) // prettier-ignore
                  : Promise.reject(new Error(`no route ${route}`));
        done.then(
          (v) => reply(200, v),
          (e: unknown) => reply(409, { error: e instanceof Error ? e.message : String(e) }),
        );
      });
    });
    this.#http = server;
    return new Promise((resolve, reject) => {
      server.on("error", reject);
      server.listen(0, "127.0.0.1", () => {
        const a = server.address();
        this.#port = typeof a === "object" && a ? a.port : 0;
        resolve();
      });
    });
  }
}
