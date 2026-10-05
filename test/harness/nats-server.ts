// Spawn a local nats-server (with JetStream) off PATH for integration tests. Mirrors the
// synadia-agents harness: free port, wait-for-TCP, temp store dir, skip-if-absent.
//
// The server enforces ACCOUNT LIMITS shaped like the managed service's (server-profiles.json):
// consumers capped PER STREAM, a stream count, storage, `max_bytes` required, no memory storage,
// an 8 MiB max_payload. Every test used to run on a default local server with none of these, which
// is how five consumer leaks stayed invisible until the first run against NGS (LEARNINGS #56–#59).
// With `no_auth_user`, an unauthenticated client lands in the limited account, so the tests
// themselves are unchanged. `DE_TEST_PROFILE=ngs|tight|none` selects the profile (default ngs).

import { type ChildProcess, spawn } from "node:child_process";
import { accessSync, constants, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

export class NatsServerNotAvailableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NatsServerNotAvailableError";
  }
}

/** The account's JetStream limits, in the server's own config vocabulary. */
export interface AccountLimits {
  max_mem: number;
  max_file: number;
  max_streams: number;
  max_consumers: number;
  max_bytes_required: boolean;
}

export interface ServerLimits {
  maxPayload: number;
  account: AccountLimits;
}

export type ServerProfile = "ngs" | "tight" | "none";

const PROFILES = JSON.parse(
  readFileSync(new URL("./server-profiles.json", import.meta.url), "utf8"),
) as Record<string, ServerLimits | null | string>;

/** The limits a profile stands for; `null` = a default local server. */
export function limitsFor(profile: ServerProfile): ServerLimits | null {
  const p = PROFILES[profile];
  if (p === undefined || typeof p === "string") {
    throw new Error(`unknown server profile "${profile}" (server-profiles.json)`);
  }
  return p;
}

/** `DE_TEST_PROFILE`, defaulting to `ngs`; an unknown value fails loud rather than testing nothing. */
export function profileFromEnv(): ServerProfile {
  const raw = process.env.DE_TEST_PROFILE ?? "ngs";
  if (raw !== "ngs" && raw !== "tight" && raw !== "none") {
    throw new Error(`DE_TEST_PROFILE="${raw}": expected ngs, tight or none`);
  }
  return raw;
}

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

export class NatsServerProcess {
  #proc: ChildProcess | undefined;
  #bin = "";
  #port = 0;
  #store = "";
  readonly limits: ServerLimits | null;
  readonly #config: ((port: number, store: string) => string) | undefined;

  /**
   * `profile` picks a row of server-profiles.json (default: `DE_TEST_PROFILE`, then `ngs`);
   * `limits` overrides it outright — a test that asserts against a specific cap passes the number
   * it asserts against, so the assertion cannot drift from the table.
   */
  constructor(
    opts: {
      profile?: ServerProfile;
      limits?: ServerLimits | null;
      /** A config of the test's own instead (`renderTenancyConfig`). */
      config?: (port: number, store: string) => string;
    } = {},
  ) {
    this.limits =
      opts.limits !== undefined ? opts.limits : limitsFor(opts.profile ?? profileFromEnv());
    this.#config = opts.config;
  }

  get url(): string {
    return `nats://127.0.0.1:${this.#port}`;
  }

  async start(): Promise<void> {
    const bin = findBinary("nats-server");
    if (!bin) throw new NatsServerNotAvailableError("nats-server not found on PATH");
    this.#bin = bin;
    this.#port = await freePort();
    this.#store = mkdtempSync(join(tmpdir(), "de-nats-"));
    // Give JetStream a generous SERVER store budget — the account's `max_file` (below) is the one
    // that binds under a profile. It's a logical limit (no real allocation), so it can be large —
    // 1 TiB here. The arithmetic still matters without a profile, because overrunning it surfaces
    // as an opaque "insufficient storage resources" at provision time: a prefix RESERVES max_bytes
    // (1 GiB default, reserved not used on demand) on each of its streams, ~5 GiB per default-sized
    // prefix. Two defenses: this ceiling, and `streamMaxBytes` on test prefixes (16 MiB is ample)
    // so each costs ~0.2 GiB instead of ~5 — which under the `ngs` profile's 50 GiB account is the
    // difference between two default-sized prefixes fitting per test and twenty small ones.
    writeFileSync(
      join(this.#store, "nats.conf"),
      this.#config?.(this.#port, this.#store) ?? renderConfig(this.#port, this.#store, this.limits),
    );
    await this.#spawn();
  }

  async #spawn(): Promise<void> {
    this.#proc = spawn(this.#bin, ["-c", join(this.#store, "nats.conf")], { stdio: "ignore" });
    await waitForTcp("127.0.0.1", this.#port, 5_000);
  }

  async #kill(signal: "SIGTERM" | "SIGKILL"): Promise<void> {
    const proc = this.#proc;
    this.#proc = undefined;
    if (!proc || proc.exitCode !== null) return;
    const exited = new Promise<void>((r) => proc.once("exit", () => r()));
    proc.kill(signal);
    await Promise.race([exited, delay(5_000)]);
    if (proc.exitCode === null && proc.signalCode === null) proc.kill("SIGKILL");
  }

  /**
   * Take the server away and bring it back on the SAME port and store — a broker restart as a
   * client sees it: every connection drops, JetStream state (streams, durables, the journal)
   * comes back from disk. `kill: true` is SIGKILL, a crash with no lame-duck goodbye;
   * `downMs` is how long the port stays dark.
   */
  async restart(opts: { kill?: boolean; downMs?: number } = {}): Promise<void> {
    await this.#kill(opts.kill === true ? "SIGKILL" : "SIGTERM");
    if (opts.downMs !== undefined && opts.downMs > 0) await delay(opts.downMs);
    await this.#spawn();
  }

  async stop(): Promise<void> {
    await this.#kill("SIGTERM");
    if (this.#store) {
      try {
        rmSync(this.#store, { recursive: true, force: true });
      } catch {
        /* ignore */
      }
      this.#store = "";
    }
  }
}

/** The server config for a port, a store dir and (optionally) an account-limits profile. */
export function renderConfig(port: number, store: string, limits: ServerLimits | null): string {
  let conf =
    `host: 127.0.0.1\nport: ${String(port)}\n` +
    (limits ? `max_payload: ${String(limits.maxPayload)}\n` : "") +
    `jetstream { store_dir: "${store}", max_file_store: 1099511627776, max_mem_store: 268435456 }\n`;
  if (limits) {
    const a = limits.account;
    // One account carrying the limits; `no_auth_user` maps every unauthenticated connection into
    // it, so a test's plain `connect({ servers })` lands under the caps without knowing they exist.
    conf +=
      `accounts {\n  DE {\n    jetstream: { max_mem: ${String(a.max_mem)}, max_file: ${String(a.max_file)}, ` +
      `max_streams: ${String(a.max_streams)}, max_consumers: ${String(a.max_consumers)}, ` +
      `max_bytes_required: ${String(a.max_bytes_required)} }\n` +
      `    users: [ { user: de, password: de } ]\n  }\n}\nno_auth_user: de\n`;
  }
  return conf;
}

/**
 * A server for the engine's fabric mode, as the engine's tests set one up
 * (`internal/testserver.TenancyConfig`): the account `DE` (user `de`) exports `{prefix}.{T}.api.>`
 * to each tenant `T`, whose account imports it as `{prefix}.api.>` with `share: true`; users
 * `de`, `<t>-client` and `<t>-worker`, each password its name.
 */
export function renderTenancyConfig(
  port: number,
  store: string,
  prefix: string,
  tenants: string[],
): string {
  // prettier-ignore
  const exports = tenants.map((t) => `      { service: "${prefix}.${t}.api.>", accounts: [${t}] }\n`).join(""); // prettier-ignore
  const accounts = tenants
    .map((t) => {
      const u = t.toLowerCase();
      return (
        `  ${t} {\n    users: [ { user: ${u}-client, password: ${u}-client }, { user: ${u}-worker, password: ${u}-worker } ]\n` +
        `    imports: [ { service: { account: DE, subject: "${prefix}.${t}.api.>" }, to: "${prefix}.api.>", share: true } ]\n  }\n`
      );
    })
    .join("");
  return (
    `host: 127.0.0.1\nport: ${String(port)}\n` +
    `jetstream { store_dir: "${store}", max_file_store: 1099511627776, max_mem_store: 268435456 }\n` +
    `accounts {\n  DE {\n    jetstream: enabled\n    users: [ { user: de, password: de } ]\n    exports: [\n${exports}    ]\n  }\n${accounts}}\n`
  );
}

function findBinary(name: string): string | null {
  for (const dir of (process.env.PATH ?? "").split(":")) {
    if (!dir) continue;
    const full = join(dir, name);
    try {
      accessSync(full, constants.X_OK);
      return full;
    } catch {
      /* keep looking */
    }
  }
  return null;
}

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.unref();
    srv.on("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      const port = typeof addr === "object" && addr ? addr.port : 0;
      srv.close(() => resolve(port));
    });
  });
}

function waitForTcp(host: string, port: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  return new Promise((resolve, reject) => {
    const tryOnce = (): void => {
      const sock = net.connect(port, host);
      sock.once("connect", () => {
        sock.destroy();
        resolve();
      });
      sock.once("error", () => {
        sock.destroy();
        if (Date.now() > deadline) reject(new Error(`waitForTcp ${host}:${port} timed out`));
        else setTimeout(tryOnce, 100);
      });
    };
    tryOnce();
  });
}
