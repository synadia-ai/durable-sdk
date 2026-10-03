// Upgrading a broker across the discard-compat boundary (LEARNINGS #40), end to end.
//
// nats-server >= 2.14.2 / >= 2.12.10 reject `allow_msg_schedules` with `discard: new` — on stream
// RESTORE as well as on create. A deployment provisioned by 0.5.0 therefore loses DE_TIMERS the
// moment its broker is upgraded: the stream stops being restored (`stream not found`) while its
// files sit intact on disk, and every pending wake stops firing. Provisioning heals that by adopting
// the on-disk store — but an `add` does NOT rewrite the persisted config, so without an explicit
// `update` the stream disappears AGAIN on the next server restart, forever.
//
// This needs two server binaries, so it self-skips unless NATS_SERVER_OLD points at a <= 2.14.1
// (or <= 2.12.9) build. Verified on BOTH release lines — 2.14.1 → 2.14.4, and 2.12.9 → 2.12.14 via
// NATS_SERVER_NEW — and confirmed to FAIL at the final restart without the provisioning fix:
//
//   NATS_SERVER_OLD=/path/to/nats-server-2.14.1 bun run test -- test/integration/upgrade-compat.test.ts
//   NATS_SERVER_OLD=…2.12.9 NATS_SERVER_NEW=…2.12.14 bun run test -- test/integration/upgrade-compat.test.ts

import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DiscardPolicy, RetentionPolicy, StorageType, jetstreamManager } from "@nats-io/jetstream";
import { connect, type NatsConnection } from "@nats-io/transport-node";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { names } from "../harness/shell/names.js";
import { provision, setEngineServer } from "../harness/engine.js";
import { jsErrorCode } from "../harness/shell/resources.js";

const oldBin = process.env.NATS_SERVER_OLD;
const newBin = process.env.NATS_SERVER_NEW ?? "nats-server";

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.unref();
    srv.on("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      srv.close(() => resolve(typeof addr === "object" && addr ? addr.port : 0));
    });
  });
}

async function waitForTcp(port: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      await new Promise<void>((resolve, reject) => {
        const c = net.createConnection({ port, host: "127.0.0.1" }, () => {
          c.end();
          resolve();
        });
        c.on("error", reject);
      });
      return;
    } catch {
      if (Date.now() > deadline) throw new Error("server did not start");
      await delay(50);
    }
  }
}

/** One store directory, several server binaries over its lifetime — that IS the upgrade. */
class Broker {
  #proc: ChildProcess | undefined;
  readonly store = mkdtempSync(join(tmpdir(), "de-upgrade-"));
  #port = 0;

  get url(): string {
    return `nats://127.0.0.1:${this.#port}`;
  }

  async boot(bin: string): Promise<void> {
    if (this.#port === 0) this.#port = await freePort();
    const conf = join(this.store, "nats.conf");
    writeFileSync(
      conf,
      `host: 127.0.0.1\nport: ${String(this.#port)}\n` +
        `jetstream { store_dir: "${this.store}", max_file_store: 1073741824, max_mem_store: 268435456 }\n`,
    );
    this.#proc = spawn(bin, ["-c", conf], { stdio: "ignore" });
    await waitForTcp(this.#port, 8_000);
    await delay(1_200); // let JetStream finish restoring state from the store dir
  }

  async halt(): Promise<void> {
    this.#proc?.kill("SIGKILL");
    this.#proc = undefined;
    await delay(600);
  }

  destroy(): void {
    rmSync(this.store, { recursive: true, force: true });
  }
}

async function timersInfo(
  nc: NatsConnection,
  prefix: string,
): Promise<{ discard: string; schedules: boolean; messages: number } | "absent"> {
  const n = names(prefix);
  try {
    const i = await (await jetstreamManager(nc)).streams.info(n.timersStream);
    return {
      discard: i.config.discard,
      schedules: i.config.allow_msg_schedules === true,
      messages: i.state.messages,
    };
  } catch (e) {
    if (jsErrorCode(e) === 10059) return "absent";
    throw e;
  }
}

describe.skipIf(!oldBin)("broker upgrade across the discard-compat boundary", () => {
  const prefix = "upg";
  const n = names(prefix);
  const broker = new Broker();

  beforeAll(async () => {
    await broker.boot(oldBin!);
  });
  afterAll(async () => {
    await broker.halt();
    broker.destroy();
  });

  it("a 0.5.0-shaped DE_TIMERS survives the upgrade AND every restart after it", async () => {
    // 1. The pre-boundary deployment: DE_TIMERS as 0.5.0 provisioned it, with a run asleep on it.
    let nc = await connect({ servers: broker.url });
    const jsm = await jetstreamManager(nc);
    await jsm.streams.add({
      name: n.timersStream,
      subjects: [n.timerSubjectAll],
      storage: StorageType.File,
      retention: RetentionPolicy.Limits,
      discard: DiscardPolicy.New, // ← what this old server still allows, and 0.5.0 asked for
      max_bytes: 64 * 1024 * 1024,
      num_replicas: 1,
      allow_msg_schedules: true,
    });
    expect(await timersInfo(nc, prefix)).toMatchObject({ discard: "new", schedules: true });
    await nc.drain();
    await broker.halt();

    // 2. Upgrade the broker. The stream is refused on restore — invisible, data still on disk.
    await broker.boot(newBin);
    nc = await connect({ servers: broker.url });
    expect(await timersInfo(nc, prefix)).toBe("absent");

    // 3. Starting a worker/client provisions, which must heal it.
    // durable-sdk: provisioning is the engine's — its start, on this test's own broker.
    setEngineServer(broker.url);
    await provision(nc, n, { streamMaxBytes: 64 * 1024 * 1024 });
    expect(await timersInfo(nc, prefix)).toMatchObject({ discard: "old", schedules: true });
    await nc.drain();

    // 4. THE REGRESSION: the heal must be persisted, not just live in this server process. An `add`
    //    that adopts the on-disk store leaves the old config written down; the stream would vanish
    //    again here, and on every restart after it, until an explicit update rewrites it.
    await broker.halt();
    await broker.boot(newBin);
    nc = await connect({ servers: broker.url });
    expect(await timersInfo(nc, prefix)).toMatchObject({ discard: "old", schedules: true });
    await nc.drain();
  }, 90_000);
});
