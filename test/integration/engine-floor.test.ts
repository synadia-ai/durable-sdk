// The engine's floor and the one withdrawal (docs/findings.md F45), against a real nats-server.
//
// The floor: worker.start() reads `info.capabilities` and refuses an engine that does not announce
// `withdrawn-remembered` (the engine's F53, announced from its #13), naming it by `engine` and
// `commit`, never by its version. A stub responder on a prefix of the test's own answers `info`
// as an older engine would, and as one at the floor; and, for a start that found no engine, only
// after start() has resolved, so the first answered poll checks the floor and stops the worker.
//
// The one withdrawal: on the pinned engine, stop() sends one `cancelPoll` and resolves at once,
// never after a poll's wait, also when it follows start() at once (a poll sent just before the
// withdrawal arrives just after it, and the engine remembers the worker); and a restart of the
// same worker polls as before (its `waitMs` 0 poll ends the engine's memory of the stop).

import { type Msg, type NatsConnection } from "@nats-io/transport-node";
import { afterAll, beforeAll, describe, expect, inject, it } from "vitest";
import { defineWorkflow } from "../../src/core/workflow.js";
import { UnsupportedEngineError } from "../../src/engine/errors.js";
import { createWorker as sdkCreateWorker } from "../../src/engine/worker.js";
import { type Logger } from "../../src/internal/logger.js";
import { createWorker, mapPrefix } from "../harness/engine.js";
import { connect } from "../harness/existing.js";
import { WAN } from "../harness/mode.js";

const natsUrl = inject("natsUrl");
const enc = new TextEncoder();
const dec = new TextDecoder();
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** A logger that keeps the error lines. */
function errors(): Logger & { lines: string[] } {
  const lines: string[] = [];
  const quiet = (): void => {};
  return { lines, debug: quiet, info: quiet, warn: quiet, error: (msg: string) => void lines.push(msg) }; // prettier-ignore
}

/**
 * A stub engine on `prefix`: `info` answers `info` (none subscribed while it is undefined), `poll`
 * answers `none` after `pollMs` (a withdrawal at once), and every other op `{}`.
 */
async function stub(nc: NatsConnection, prefix: string, info: object | undefined, pollMs = 100) {
  const subs = [
    nc.subscribe(`${prefix}.api.v1.poll`, {
      callback: (_e, m: Msg) => {
        const body = JSON.parse(dec.decode(m.data)) as { cancelPoll?: boolean };
        setTimeout(() => m.respond(enc.encode('{"none":true}')), body.cancelPoll === true ? 0 : pollMs); // prettier-ignore
      },
    }),
    nc.subscribe(`${prefix}.api.v1.done`, { callback: (_e, m: Msg) => void m.respond(enc.encode("{}")) }), // prettier-ignore
  ];
  const answerInfo = (i: object): void => {
    subs.push(nc.subscribe(`${prefix}.api.v1.info`, { callback: (_e, m: Msg) => void m.respond(enc.encode(JSON.stringify(i))) })); // prettier-ignore
  };
  if (info !== undefined) answerInfo(info);
  await nc.flush();
  return {
    answerInfo: async (i: object): Promise<void> => {
      answerInfo(i);
      await nc.flush();
    },
    close: (): void => {
      for (const s of subs) s.unsubscribe();
    },
  };
}

const wf = defineWorkflow("floor_wf", () => Promise.resolve("ok"));
const OLD = { engine: "0.2.0-dev", commit: "stub5047d79", api: "v1" };
const AT_FLOOR = { ...OLD, commit: "stub16cb3da", capabilities: ["withdrawn-remembered"] };

describe.skipIf(!natsUrl)("the engine's floor: info.capabilities (stub engines)", () => {
  let nc: NatsConnection;
  beforeAll(async () => {
    nc = await connect({ servers: natsUrl! });
  });
  afterAll(async () => {
    await nc.drain();
  });
  // A prefix of the test's own, never the run's: no engine of the harness serves it.
  const own = (): string => `floor${globalThis.crypto.randomUUID().slice(0, 8)}`;

  it("start() rejects an engine whose info has no capabilities, naming its engine and commit", async () => {
    const prefix = own();
    const s = await stub(nc, prefix, OLD);
    try {
      const log = errors();
      const w = sdkCreateWorker({ nc, workflows: [wf], prefix, logger: log });
      const e = (await w.start().catch((x: unknown) => x)) as UnsupportedEngineError;
      expect(e).toBeInstanceOf(UnsupportedEngineError);
      expect(e).toMatchObject({ engine: "0.2.0-dev", commit: "stub5047d79", capabilities: undefined }); // prettier-ignore
      expect(e.message).toContain("stub5047d79");
      expect(e.message).toContain("0.2.0-dev");
      expect(e.message).toContain(prefix);
      expect(w.health().running).toBe(false);
      expect(log.lines).toHaveLength(1);
    } finally {
      s.close();
    }
  });

  it("an engine that announces withdrawn-remembered is let through", async () => {
    const prefix = own();
    const s = await stub(nc, prefix, AT_FLOOR);
    const w = sdkCreateWorker({ nc, workflows: [wf], prefix, concurrency: 2 });
    try {
      await w.start();
      expect(w.health().running).toBe(true);
    } finally {
      await w.stop();
      s.close();
    }
  });

  it("a start that found no engine checks the floor at its first answered poll, and stops the worker", async () => {
    const prefix = own();
    const s = await stub(nc, prefix, undefined, 300);
    const log = errors();
    const w = sdkCreateWorker({ nc, workflows: [wf], prefix, concurrency: 2, logger: log });
    try {
      await w.start(); // no `info` responder: not fatal, the slots poll
      expect(w.health().running).toBe(true);
      await s.answerInfo(OLD); // the engine answers info only now
      const deadline = Date.now() + 5_000 * WAN;
      while (w.health().running && Date.now() < deadline) await sleep(50);
      expect(w.health().running).toBe(false);
      expect(log.lines).toHaveLength(1);
      expect(log.lines[0]).toContain("stub5047d79");
      expect(log.lines[0]).toContain("this worker stops");
    } finally {
      await w.stop();
      s.close();
    }
  });
});

describe.skipIf(!natsUrl)("stop() withdraws once, on the pinned engine", () => {
  let nc: NatsConnection;
  beforeAll(async () => {
    nc = await connect({ servers: natsUrl! });
  });
  afterAll(async () => {
    await nc.drain();
  });

  it(
    "one cancelPoll per stop(), at once, also right after start(); and the restarted worker's polls wait",
    async () => {
      const prefix = mapPrefix("wstop");
      // The worker's connection, which can hold its polls back before they are sent: a poll sent
      // just before the withdrawal that arrives just after it, as over a slow link.
      let lagMs = 0;
      const lagging = new Proxy(nc, {
        get(t, k) {
          if (k === "request") {
            return async (subject: string, data?: Uint8Array, o?: { timeout: number }) => {
              const b = JSON.parse(dec.decode(data ?? new Uint8Array())) as { cancelPoll?: boolean; waitMs?: number }; // prettier-ignore
              if (lagMs > 0 && subject.endsWith(".api.v1.poll") && b.cancelPoll !== true && (b.waitMs ?? 0) > 0) await sleep(lagMs); // prettier-ignore
              return t.request(subject, data, o);
            };
          }
          const v: unknown = Reflect.get(t, k);
          return typeof v === "function" ? (v as (...a: unknown[]) => unknown).bind(t) : v;
        },
      });
      const w = createWorker({ nc: lagging, workflows: [defineWorkflow("wstop_wf", () => Promise.resolve(1))], prefix: "wstop", concurrency: 4 }); // prettier-ignore
      // Every poll request on the prefix, as the engine's queue group sees them (this subscription
      // is outside it, so it sees each one): this worker's are those naming its workflow.
      const polls: Array<{ worker?: string; cancelPoll?: boolean; waitMs?: number; t: number }> =
        [];
      const sub = nc.subscribe(`${prefix}.api.v1.poll`, {
        callback: (_e, m: Msg) => {
          const b = JSON.parse(dec.decode(m.data)) as { worker?: string; cancelPoll?: boolean; waitMs?: number; workflows?: Array<{ name: string }> }; // prettier-ignore
          if (b.cancelPoll === true || b.workflows?.some((x) => x.name === "wstop_wf") === true) polls.push({ ...b, t: Date.now() }); // prettier-ignore
        },
      });
      await nc.flush();
      const me = (): string => polls.find((p) => p.cancelPoll !== true)!.worker!;
      const cancels = (from: number): number => polls.filter((p) => p.cancelPoll === true && p.worker === me() && p.t >= from).length; // prettier-ignore
      try {
        // Polls out and waiting, then stop(); five stops right after their start(); and one whose
        // polls reach the engine 600 ms after the withdrawal (the engine remembers the worker and
        // answers them `none` at once; the SDK's former stop() withdrew again every 250 ms).
        for (const [settle, lag] of [
          [500, 0],
          [0, 0],
          [0, 0],
          [0, 0],
          [0, 0],
          [0, 0],
          [50, 600],
        ] as const) {
          // prettier-ignore
          lagMs = lag;
          await w.start();
          if (settle > 0) await sleep(settle);
          const t0 = Date.now();
          await w.stop();
          const took = Date.now() - t0;
          lagMs = 0;
          await nc.flush();
          await sleep(100);
          expect(cancels(t0)).toBe(1);
          expect(took).toBeLessThan(5_000 * WAN); // a poll's wait is 30 s
          if (lag > 0) expect(took).toBeGreaterThanOrEqual(lag - 100); // the late polls were out
        }
        // The restart after the stops: its waitMs 0 poll ends the engine's memory of them, so the
        // slots' polls wait as before instead of answering `none` at once in a loop.
        const t1 = Date.now();
        await w.start();
        await sleep(1_000);
        const sent = polls.filter((p) => p.cancelPoll !== true && p.worker === me() && p.t >= t1).length; // prettier-ignore
        expect(sent).toBeLessThanOrEqual(1 + 4 + 2); // start()'s and one per slot, a little slack
      } finally {
        await w.stop();
        sub.unsubscribe();
      }
    },
    60_000 * WAN,
  ); // durable-sdk: × the round-trip factor (harness/mode.ts)
});
