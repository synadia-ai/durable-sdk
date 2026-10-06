// durable-sdk: the engine's refusal codes, against a fake connection (no server). Every code of the
// engine's table (docs/API.md "Errors" at engine.pin) has a class here, so a code the engine adds
// fails this suite rather than reaching a caller as `unavailable` (the engine's F46; F40). The two
// limit codes are `LimitError`, retried by no call: a client's call rejects with it, a worker's
// start() rejects when its first poll is refused, a refused poll is logged at error and backs off,
// and a worker's failing polls say so once a minute (F41). An engine below the floor
// (`info.capabilities` without `withdrawn-remembered`) is refused at start(), or at the first
// answered poll when start() found none, and stop() withdraws once (F45).

import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { type NatsConnection, NoRespondersError } from "@nats-io/nats-core";
import { describe, expect, it } from "vitest";
import { createClient } from "../../src/engine/client.js";
import {
  ENGINE_ERROR_CODES,
  EngineError,
  EngineUnavailableError,
  LimitError,
  UnsupportedEngineError,
} from "../../src/engine/errors.js";
import { createTransport } from "../../src/engine/transport.js";
import { createWorker, EveryLog, limitBackoffMs } from "../../src/engine/worker.js";
import { defineWorkflow } from "../../src/core/workflow.js";
import { type Logger } from "../../src/internal/logger.js";

type Answer = object | Error;

/** A connection that answers each request by its op, recording the ops asked. */
function fakeNc(answer: (op: string, body: Record<string, unknown>) => Answer | Promise<Answer>): {
  nc: NatsConnection;
  ops: string[];
} {
  const ops: string[] = [];
  const nc = {
    info: { max_payload: 1024 * 1024 },
    isClosed: () => false,
    rtt: () => Promise.resolve(1),
    async request(subject: string, payload: Uint8Array) {
      const op = subject.split(".").at(-1)!;
      ops.push(op);
      const a = await answer(op, JSON.parse(new TextDecoder().decode(payload)) as Record<string, unknown>); // prettier-ignore
      if (a instanceof Error) throw a;
      return { data: new TextEncoder().encode(JSON.stringify(a)) };
    },
  };
  return { nc: nc as unknown as NatsConnection, ops };
}

/** A logger that keeps what it is told. */
function recorder(): Logger & {
  lines: Array<{ level: string; msg: string; fields?: Record<string, unknown> }>;
} {
  const lines: Array<{ level: string; msg: string; fields?: Record<string, unknown> }> = [];
  const at =
    (level: string) =>
    (msg: string, fields?: Record<string, unknown>): void => {
      lines.push({ level, msg, ...(fields !== undefined ? { fields } : {}) });
    };
  return { lines, debug: at("debug"), info: at("info"), warn: at("warn"), error: at("error") };
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

const refusal = (code: string, extra: object = {}): object => ({
  error: { code, message: `${code} said the engine`, ...extra },
});

const wf = defineWorkflow("lim", () => Promise.resolve("ok"));

/** An engine at the floor's `info` (its #13 and later). */
const INFO = { engine: "0.2.0-dev", commit: "16cb3da", capabilities: ["withdrawn-remembered"] };

/** The codes of the engine's table at engine.pin, or undefined without an engine checkout. */
function enginesCodes(): string[] | undefined {
  const repo = fileURLToPath(new URL("../..", import.meta.url));
  const src = process.env.DE_ENGINE_REPO ?? join(repo, "..", "durable-engine");
  if (!existsSync(join(src, ".git"))) return undefined;
  const pin = readFileSync(join(repo, "engine.pin"), "utf8").trim();
  let doc: string;
  try {
    doc = execFileSync("git", ["-C", src, "show", `${pin}:docs/API.md`], { encoding: "utf8" });
  } catch {
    return undefined;
  }
  const section = doc.split(/^## Errors$/m)[1]!.split(/^## /m)[0]!;
  return [...section.matchAll(/^\| `([a-z_]+)` \|/gm)]
    .map((m) => m[1]!)
    .filter((c) => c !== "code"); // the header
}

describe("every code of the engine's table has a class (F40)", () => {
  const codes = enginesCodes();

  it.skipIf(codes === undefined)("the mirror is the table at engine.pin, code for code", () => {
    expect(Object.keys(ENGINE_ERROR_CODES).sort()).toEqual([...codes!].sort());
  });

  it.each(Object.entries(ENGINE_ERROR_CODES))("%s is %s", async (code, cls) => {
    const extra = code === "blob_gone" ? { blob: `r1/sha256:${"a".repeat(64)}` } : code.endsWith("_limit") ? { limit: "runs" } : {}; // prettier-ignore
    const t = createTransport(fakeNc(() => refusal(code, extra)).nc, "de");
    const e = await t("start", { runId: "r1" }).catch((x: unknown) => x);
    expect((e as Error).name).toBe(cls);
  });

  it("a code this SDK does not know stays transient, and is said", async () => {
    const log = recorder();
    const t = createTransport(fakeNc(() => refusal("brand_new")).nc, "de", log);
    await expect(t("start", {})).rejects.toBeInstanceOf(EngineUnavailableError);
    expect(log.lines.map((l) => l.level)).toEqual(["warn"]);
  });
});

describe("tenant_limit and licence_limit are LimitError (the engine's F46)", () => {
  it.each([
    ["tenant_limit", "workflows"],
    ["licence_limit", "runs"],
  ] as const)(
    "%s names its limit, keeps the engine's message, and is not unavailable",
    async (code, limit) => {
      const t = createTransport(fakeNc(() => refusal(code, { limit })).nc, "de");
      const e = (await t("start", { runId: "r1" }).catch((x: unknown) => x)) as LimitError;
      expect(e).toBeInstanceOf(LimitError);
      expect(e).not.toBeInstanceOf(EngineUnavailableError);
      expect(e.code).toBe(code);
      expect(e.limit).toBe(limit);
      expect(e.op).toBe("start");
      expect(e.message).toContain(`${code} said the engine`);
    },
  );

  it("the back-off: rate a second, the others 30 s", () => {
    expect(limitBackoffMs(new LimitError("poll", "tenant_limit", "rate", ""))).toBe(1_000);
    expect(limitBackoffMs(new LimitError("poll", "licence_limit", "workers", ""))).toBe(30_000);
  });
});

describe("no client call retries a limit", () => {
  it("start, signal, cancel: one request, rejected", async () => {
    const { nc, ops } = fakeNc(() => refusal("licence_limit", { limit: "runs" }));
    const c = createClient({ nc });
    await expect(c.start(wf, {})).rejects.toBeInstanceOf(LimitError);
    await expect(c.signal("r1", "go", 1)).rejects.toBeInstanceOf(LimitError);
    await expect(c.cancel("r1")).rejects.toBeInstanceOf(LimitError);
    expect(ops.filter((o) => o !== "info")).toEqual(["start", "signal", "cancel"]); // info: the sizes
  });

  it("result() rejects at once, where unavailable is asked again", async () => {
    const { nc, ops } = fakeNc(() => refusal("tenant_limit", { limit: "rate" }));
    const c = createClient({ nc });
    await expect(c.get("r1").result(5_000)).rejects.toMatchObject({ name: "LimitError", limit: "rate" }); // prettier-ignore
    expect(ops).toEqual(["result"]);
  });
});

describe("a worker refused at a limit is loud, never quiet", () => {
  it("start() rejects when its first poll is refused, and runs nothing", async () => {
    const { nc, ops } = fakeNc((op) => (op === "poll" ? refusal("licence_limit", { limit: "workflows" }) : INFO)); // prettier-ignore
    const log = recorder();
    const w = createWorker({ nc, workflows: [wf], logger: log });
    await expect(w.start()).rejects.toMatchObject({ name: "LimitError", code: "licence_limit", limit: "workflows" }); // prettier-ignore
    expect(w.health().running).toBe(false);
    await sleep(50);
    expect(ops).toEqual(["info", "poll"]);
    expect(log.lines.filter((l) => l.level === "error")).toHaveLength(1);
    expect(log.lines[0]!.fields).toMatchObject({ code: "licence_limit", limit: "workflows" });
  });

  it("an info refused at a limit rejects start() too (a tenant new to the instance)", async () => {
    const { nc } = fakeNc(() => refusal("licence_limit", { limit: "tenants" }));
    await expect(createWorker({ nc, workflows: [wf] }).start()).rejects.toBeInstanceOf(LimitError); // prettier-ignore
  });

  it("rate, which clears within a second, does not reject start()", async () => {
    let polls = 0;
    const { nc } = fakeNc((op) => (op === "poll" && ++polls === 1 ? refusal("tenant_limit", { limit: "rate" }) : op === "poll" ? sleep(20).then(() => ({ none: true })) : INFO)); // prettier-ignore
    const w = createWorker({ nc, workflows: [wf], concurrency: 1 });
    await w.start();
    expect(w.health().running).toBe(true);
    await w.stop();
  });

  it("a poll refused later is logged at error once per back-off, naming the limit, and not polled again meanwhile", async () => {
    let polls = 0;
    const { nc, ops } = fakeNc((op) => {
      if (op !== "poll") return INFO;
      polls += 1;
      return polls === 1 ? { none: true } : refusal("licence_limit", { limit: "workers" });
    });
    const log = recorder();
    const w = createWorker({ nc, workflows: [wf], concurrency: 4, logger: log });
    await w.start();
    await sleep(300);
    // The first poll at start, then one refused poll per slot: each slot now waits 30 s.
    expect(ops.filter((o) => o === "poll")).toHaveLength(1 + 4);
    const errors = log.lines.filter((l) => l.level === "error");
    expect(errors).toHaveLength(1);
    expect(errors[0]!.fields).toMatchObject({ code: "licence_limit", limit: "workers", refused: 1 }); // prettier-ignore
    expect(errors[0]!.msg).toContain("polling again in 30 s");
    const t0 = Date.now();
    await w.stop(); // wakes the slots' back-off at once
    expect(Date.now() - t0).toBeLessThan(1_000);
  });
});

describe("a worker's failing polls say so once a minute, with a count (F41)", () => {
  it("EveryLog: one line per interval, standing for what it kept quiet", () => {
    let now = 0;
    const l = new EveryLog(60_000, () => now);
    expect(l.hit()).toBe(1);
    now = 10_000;
    expect(l.hit()).toBe(0);
    now = 59_999;
    expect(l.hit()).toBe(0);
    now = 60_000;
    expect(l.hit()).toBe(3);
    now = 60_001;
    expect(l.hit()).toBe(0);
  });

  it("every slot failing for a while is one warning", async () => {
    const { nc, ops } = fakeNc((op) => (op === "poll" ? new NoRespondersError("de.api.v1.poll") : INFO)); // prettier-ignore
    const log = recorder();
    const w = createWorker({ nc, workflows: [wf], concurrency: 4, logger: log });
    await w.start();
    await sleep(900);
    await w.stop();
    expect(ops.filter((o) => o === "poll").length).toBeGreaterThan(8);
    const warns = log.lines.filter((l) => l.msg.includes("poll failed; retrying"));
    expect(warns).toHaveLength(1);
    expect(warns[0]!.fields).toMatchObject({ failures: 1, everyMs: 60_000 });
  });
});

describe("the engine's floor: info.capabilities, never the version (F45)", () => {
  it("start() rejects an engine whose info has no capabilities, naming it by engine and commit", async () => {
    const { nc, ops } = fakeNc((op) => (op === "info" ? { engine: "0.2.0-dev", commit: "5047d79" } : { none: true })); // prettier-ignore
    const log = recorder();
    const w = createWorker({ nc, workflows: [wf], prefix: "old", logger: log });
    const e = (await w.start().catch((x: unknown) => x)) as UnsupportedEngineError;
    expect(e).toBeInstanceOf(UnsupportedEngineError);
    expect(e).toBeInstanceOf(EngineError);
    expect(e).toMatchObject({ code: "unsupported", engine: "0.2.0-dev", commit: "5047d79", capabilities: undefined, missing: ["withdrawn-remembered"] }); // prettier-ignore
    for (const s of ['"old"', "0.2.0-dev", "5047d79", "16cb3da", "withdrawn-remembered"]) expect(e.message).toContain(s); // prettier-ignore
    expect(ops).toEqual(["info"]);
    expect(w.health().running).toBe(false);
    expect(log.lines.filter((l) => l.level === "error")).toHaveLength(1);
  });

  it("start() rejects an engine whose capabilities lack withdrawn-remembered", async () => {
    const { nc } = fakeNc((op) => (op === "info" ? { engine: "0.2.0-dev", commit: "x", capabilities: ["something-else"] } : { none: true })); // prettier-ignore
    await expect(createWorker({ nc, workflows: [wf] }).start()).rejects.toMatchObject({ name: "UnsupportedEngineError", missing: ["withdrawn-remembered"] }); // prettier-ignore
  });

  it("an engine that announces it starts", async () => {
    const { nc, ops } = fakeNc((op) => (op === "info" ? INFO : sleep(20).then(() => ({ none: true })))); // prettier-ignore
    const w = createWorker({ nc, workflows: [wf], concurrency: 1 });
    await w.start();
    expect(w.health().running).toBe(true);
    await w.stop();
    expect(ops.filter((o) => o === "info")).toEqual(["info"]); // checked once, at start()
  });

  it("start() found no engine: it still sends its waitMs 0 poll, and the first answered poll checks the floor and stops the worker", async () => {
    let up = false;
    const bodies: Array<Record<string, unknown>> = [];
    const { nc, ops } = fakeNc((op, body) => {
      if (op === "poll") bodies.push(body);
      if (!up) return new NoRespondersError(`de.api.v1.${op}`);
      if (op === "info") return { engine: "0.2.0-dev", commit: "5047d79" };
      if (op === "poll") return body.cancelPoll === true ? { none: true } : sleep(10).then(() => ({ runId: "r1", lease: "L", workflow: "lim", version: 1, attempt: 1, journal: [], ackWaitMs: 30_000, workDeadlineMs: 0 })); // prettier-ignore
      return {};
    });
    const log = recorder();
    const w = createWorker({ nc, workflows: [wf], concurrency: 3, logger: log });
    await w.start(); // no engine: not fatal
    expect(ops.slice(0, 2)).toEqual(["info", "poll"]);
    expect(bodies[0]).toMatchObject({ waitMs: 0 });
    expect(w.health().running).toBe(true);
    up = true;
    for (let i = 0; i < 100 && w.health().running; i++) await sleep(20);
    expect(w.health().running).toBe(false);
    await sleep(50);
    expect(ops.filter((o) => o === "info")).toHaveLength(2); // start()'s, and one for every slot
    expect(ops.filter((o) => o === "done").length).toBeGreaterThan(0); // the items it was handed go back
    const errors = log.lines.filter((l) => l.level === "error");
    expect(errors).toHaveLength(1);
    expect(errors[0]!.msg).toContain("5047d79");
    expect(errors[0]!.msg).toContain("this worker stops");
    await w.stop();
  });

  it("stop() withdraws once, and returns when the engine answers the polls", async () => {
    const waiting: Array<() => void> = [];
    let cancels = 0;
    const { nc } = fakeNc((op, body) => {
      if (op === "info") return INFO;
      if (op !== "poll") return {};
      if (body.cancelPoll === true) {
        cancels += 1;
        for (const r of waiting.splice(0)) r();
        return { none: true };
      }
      if (body.waitMs === 0) return { none: true };
      return new Promise((r) => waiting.push(() => r({ none: true })));
    });
    const w = createWorker({ nc, workflows: [wf], concurrency: 4 });
    await w.start();
    await sleep(20);
    expect(waiting).toHaveLength(4);
    const t0 = Date.now();
    await w.stop();
    expect(Date.now() - t0).toBeLessThan(500);
    await sleep(300); // no repeat afterwards either
    expect(cancels).toBe(1);
  });
});
