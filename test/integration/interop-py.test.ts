// The cross-language proof, the TypeScript side: a TYPESCRIPT client driving a PYTHON worker
// process (python/conformance/worker.py, the twin of conformance/interop/ts-worker.ts), through
// durable-engine serve. Nothing is shared between the two processes except the engine's API and
// the protocol (durable-engine docs/protocol/PROTOCOL.md). Each case runs twice: plain, and with DE_PAYLOAD_KEY (the two
// SDKs' AES-GCM payload codecs must agree, and nothing on the journal may be plaintext).
//
// Skipped without `uv` on PATH (the Python worker's runner). The Python suites
// (python/tests/integration/test_interop*.py) are the other legs: a Python client driving the
// thin TypeScript worker, and the TypeScript client script driving an in-process Python worker.

import { type ChildProcess, execFileSync, spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { type NatsConnection } from "@nats-io/transport-node";
import { connect } from "../harness/existing.js";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, inject, it } from "vitest";
import { defineWorkflow } from "../../src/core/workflow.js";
import { CancelledError, WorkflowFailure } from "../../src/errors.js";
import { jsonCodec } from "../../src/journal/codec.js";
import { aesGcmPayloadCodec } from "../../src/journal/payload-codec.js";
import { createClient, ensureEngine, mapPrefix } from "../harness/engine.js";
import { NatsJournal } from "../harness/shell/journal-store.js";
import { names, skipAsTenant } from "../harness/shell/names.js";
import { WAN } from "../harness/mode.js";

const natsUrl = inject("natsUrl");
const repo = fileURLToPath(new URL("../..", import.meta.url));

function hasUv(): boolean {
  try {
    execFileSync("uv", ["--version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}
const uv = hasUv();

const KEY = Uint8Array.from({ length: 32 }, (_, i) => i);
const SECRET = "pan-4242424242424242";

// Matching names and versions, NO shared code: the bodies live in Python.
const inPython = (): Promise<never> => {
  throw new Error("this workflow's body is implemented in the Python worker");
};
const greet = defineWorkflow("greet", (_ctx, _input: { name: string }) => inPython());
const approve = defineWorkflow("approve", (_ctx, _input: { order: string }) => inPython());
const boom = defineWorkflow("boom", (_ctx, _input: unknown) => inPython());
const echoSignal = defineWorkflow("echo_signal", (_ctx, _input: unknown) => inPython());

let seq = 0;
const freshPrefix = (): string => `ipy${Date.now().toString(36)}${(seq++).toString(36)}`;

/** Spawn the Python worker on `prefix` (its engine first) and wait until it prints READY. */
async function startPythonWorker(prefix: string, encrypted: boolean): Promise<ChildProcess> {
  await ensureEngine(prefix);
  const proc = spawn(
    "uv",
    ["run", "--project", "python", "python", "python/conformance/worker.py"],
    {
      cwd: repo,
      env: {
        ...process.env,
        NATS_URL: natsUrl!,
        DE_PREFIX: mapPrefix(prefix),
        ...(encrypted ? { DE_PAYLOAD_KEY: Buffer.from(KEY).toString("hex") } : {}),
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  let err = "";
  proc.stderr.on("data", (d: Buffer) => (err += d.toString("utf8")));
  await new Promise<void>((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`the Python worker never printed READY:\n${err}`)), 60_000); // prettier-ignore
    let out = "";
    proc.stdout.on("data", (d: Buffer) => {
      out += d.toString("utf8");
      if (out.includes("READY")) {
        clearTimeout(t);
        resolve();
      }
    });
    proc.once("exit", (code) => {
      clearTimeout(t);
      reject(new Error(`the Python worker exited (${String(code)}):\n${err}`));
    });
  });
  return proc;
}

async function stopPythonWorker(proc: ChildProcess | undefined): Promise<void> {
  if (!proc || proc.exitCode !== null || proc.signalCode !== null) return;
  const exited = new Promise<void>((r) => proc.once("exit", () => r()));
  proc.kill("SIGTERM");
  await Promise.race([exited, new Promise((r) => setTimeout(r, 10_000))]);
  if (proc.exitCode === null && proc.signalCode === null) proc.kill("SIGKILL");
}

/** Every entry of a run's journal in its STORED form, read with the test's own client. */
async function rawEntries(
  nc: NatsConnection,
  prefix: string,
  runId: string,
): Promise<Array<Record<string, unknown>>> {
  skipAsTenant(); // the test reads the journal itself (shell/names.ts)
  const jsm = await jetstreamManager(nc);
  const journal = new NatsJournal(jetstream(nc), jsm, names(prefix), jsonCodec);
  return (await journal.loadRaw(runId)).map((l) => l.entry as unknown as Record<string, unknown>);
}

async function waitForState(
  status: () => Promise<{ state: string }>,
  state: string,
  timeoutMs = 20_000,
): Promise<void> {
  timeoutMs *= WAN; // durable-sdk: × the round-trip factor (harness/mode.ts, F29)
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if ((await status()).state === state) return;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`never reached ${state}`);
}

describe.skipIf(!natsUrl || !uv)("interop: a TypeScript client drives a Python worker", () => {
  let nc: NatsConnection;
  let worker: ChildProcess | undefined;
  beforeAll(async () => {
    nc = await connect({ servers: natsUrl! });
  });
  afterAll(async () => {
    await stopPythonWorker(worker); // durable-sdk: a test skipped part-way never reached afterEach
    await nc.drain();
  });
  // durable-sdk: a test skipped part-way (as a tenant, at its journal read) never reaches
  // afterEach, so the next test stops the worker it left (vitest skips the hooks of a skip).
  beforeEach(async () => {
    await stopPythonWorker(worker);
    worker = undefined;
  });
  afterEach(async () => {
    await stopPythonWorker(worker);
    worker = undefined;
  });

  describe.each([
    { mode: "plain", encrypted: false },
    { mode: "encrypted (DE_PAYLOAD_KEY)", encrypted: true },
  ])("$mode", ({ encrypted }) => {
    async function setup(): Promise<{ prefix: string; client: ReturnType<typeof createClient> }> {
      const prefix = freshPrefix();
      worker = await startPythonWorker(prefix, encrypted);
      const client = createClient({
        nc,
        prefix,
        ...(encrypted ? { payloadCodec: aesGcmPayloadCodec(KEY) } : {}),
      });
      return { prefix, client };
    }

    it("runs a Python workflow and reads its result and status", async () => {
      const { prefix, client } = await setup();
      const h = await client.start(greet, { name: SECRET });
      const r = (await h.result(20_000)) as { greeting: string; at: unknown; from: string };
      expect(r.greeting).toBe(`HELLO ${SECRET.toUpperCase()}`);
      expect(r.from).toBe("python");
      expect(typeof r.at).toBe("number");
      // durable-sdk: F27, as below; it lost the race in the existing-server mode at 150 ms (F29).
      // Harmless now: the engine guarantees the state since ee81460 (its F29); a failure is its.
      await waitForState(() => h.status(), "completed", 10_000);
      const s = await h.status();
      expect(s.state).toBe("completed");
      expect(s.workflow).toBe("greet");

      const raw = await rawEntries(nc, prefix, h.runId);
      const text = JSON.stringify(raw);
      if (encrypted) {
        // Self-checking: the journal really is v:3 with `*Enc` slots, and holds no plaintext.
        expect(raw.some((e) => e.v === 3 && "inputEnc" in e)).toBe(true);
        expect(raw.some((e) => e.kind === "result" && "valueEnc" in e)).toBe(true);
        expect(text).not.toContain(SECRET);
        expect(text).not.toContain(SECRET.toUpperCase());
      } else {
        expect(text).toContain(SECRET);
      }
    });

    it("a TypeScript signal wakes a parked Python run", async () => {
      const { client } = await setup();
      const h = await client.start(approve, { order: "ord-1" });
      await waitForState(() => h.status(), "suspended");
      await client.signal(h.runId, "decision", { ok: true });
      expect(await h.result(20_000)).toEqual({ order: "ord-1", approved: true });
    });

    it("a Python failure surfaces as WorkflowFailure", async () => {
      const { client } = await setup();
      const h = await client.start(boom, {});
      const e = await h.result(20_000).catch((x: unknown) => x);
      expect(e).toBeInstanceOf(WorkflowFailure);
      expect(String((e as Error).message)).toContain("carrier rejected");
      // durable-sdk: `status` read the engine's hint, written at the worker's `done`, one round
      // trip after the terminal `result` answers from (docs/findings.md F27): wait for it. Harmless
      // now: the engine guarantees the state since ee81460 (its F29), so a failure is the engine's.
      await waitForState(() => h.status(), "failed", 10_000);
    });

    it("a TypeScript cancel hard-stops a parked Python run", async () => {
      const { client } = await setup();
      const h = await client.start(approve, { order: "ord-2" });
      await waitForState(() => h.status(), "suspended");
      await client.cancel(h.runId, { reason: "customer withdrew" });
      await expect(h.result(20_000)).rejects.toBeInstanceOf(CancelledError);
      await waitForState(() => h.status(), "cancelled", 10_000); // durable-sdk: F27, as above
    });

    it("a small signal's data crosses to the Python workflow", async () => {
      const { prefix, client } = await setup();
      const h = await client.start(echoSignal, {});
      await waitForState(() => h.status(), "suspended");
      await client.signal(h.runId, "card", SECRET);
      expect(await h.result(20_000)).toEqual({ len: SECRET.length, head: SECRET.slice(0, 24) });
      const text = JSON.stringify(await rawEntries(nc, prefix, h.runId));
      if (encrypted) expect(text).not.toContain(SECRET);
    });
  });
});
