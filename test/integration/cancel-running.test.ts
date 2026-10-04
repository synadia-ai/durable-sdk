// A cancel sent while a run's step is RUNNING (the fabric test repo's F23): the cancel must reach the
// journal while that execution still holds the run, not wait behind it in the worker's per-run chain.
// Waiting behind it meant the execution never saw the cancel — the run COMPLETED, every later step ran
// after the cancel was stored, and no cancel-requested was ever journaled. PROTOCOL §11.7 step 2.

import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { type NatsConnection } from "@nats-io/transport-node";
import { connect } from "../harness/existing.js";
import { afterAll, beforeAll, describe, expect, inject, it } from "vitest";
import { defineWorkflow } from "../../src/core/workflow.js";
import { CancelledError } from "../../src/errors.js";
import { jsonCodec } from "../../src/journal/codec.js";
import { type CancelledEntry, type LoadedEntry } from "../../src/journal/entry.js";
import { createClient } from "../harness/engine.js";
import { NatsJournal } from "../harness/shell/journal-store.js";
import { names } from "../harness/shell/names.js";
import { createWorker } from "../harness/engine.js";
import { type Worker } from "../../src/engine/worker.js";

const natsUrl = inject("natsUrl");
const MIB = 1024 * 1024;
const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** A workflow whose middle step's effect holds until the test releases it. */
function heldWorkflow(name: string) {
  const effects = { before: 0, hold: 0, after: 0, undo: 0 };
  let release!: () => void;
  const gate = new Promise<void>((r) => (release = r));
  let entered!: () => void;
  const holding = new Promise<void>((r) => (entered = r));
  const wf = defineWorkflow(name, async (ctx) => {
    ctx.onCancel(() => ctx.run("undo", () => void effects.undo++));
    await ctx.run("before", () => void effects.before++);
    await ctx.run("hold", async () => {
      effects.hold++;
      entered();
      await gate;
      return "held";
    });
    await ctx.run("after", () => void effects.after++);
    return "done";
  });
  return { wf, effects, release: () => release(), holding };
}

describe.skipIf(!natsUrl)("cancel while a step is running (real nats-server)", () => {
  let nc: NatsConnection;
  beforeAll(async () => {
    nc = await connect({ servers: natsUrl! });
  });
  afterAll(async () => {
    await nc.drain();
  });

  async function cancelWhileHeld(prefix: string, workerCount: number): Promise<void> {
    const { wf, effects, release, holding } = heldWorkflow(`cancel_running_${workerCount}`);
    const workers: Worker[] = [];
    for (let i = 0; i < workerCount; i++) {
      workers.push(createWorker({ nc, workflows: [wf], prefix, streamMaxBytes: 64 * MIB }));
    }
    for (const w of workers) await w.start();
    try {
      const client = createClient({ nc, prefix });
      const handle = await client.start(wf, {});
      await holding; // the step's effect is running on some worker
      await client.cancel(handle.runId, { reason: "operator stop" });
      const ackedAt = Date.now();

      const journal = new NatsJournal(
        jetstream(nc),
        await jetstreamManager(nc),
        names(prefix),
        jsonCodec,
      );
      const has = (ls: LoadedEntry[], kind: string): boolean =>
        ls.some((l) => l.entry.kind === kind);
      // The cancel is journaled WHILE the step is still held — not after the execution ends.
      const deadline = Date.now() + 5_000;
      while (!has(await journal.loadRaw(handle.runId), "cancel-requested")) {
        if (Date.now() > deadline) {
          release(); // let the run finish so teardown is clean, then say what it did instead
          const outcome = await handle.result(15_000).then(
            (v) => `completed with ${JSON.stringify(v)}`,
            (e: unknown) => `rejected with ${String(e)}`,
          );
          throw new Error(
            `cancel-requested was not journaled while the step was held; the run ${outcome}, ` +
              `effects ${JSON.stringify(effects)}`,
          );
        }
        await delay(25);
      }
      release();

      await expect(handle.result(15_000)).rejects.toThrow(CancelledError);
      const tookMs = Date.now() - ackedAt;
      console.log(`cancel ack → cancelled terminal, ${workerCount} worker(s): ${tookMs} ms`);

      const loaded = await journal.loadRaw(handle.runId);
      const kinds = loaded.map((l) => l.entry.kind);
      const holdSeq = loaded.find((l) => l.entry.kind === "step-begin" && l.entry.name === "hold")
        ?.entry.seq;
      expect(holdSeq).toBeDefined();
      const holdEnd = loaded.findIndex(
        (l) => l.entry.kind === "step-end" && l.entry.seq === holdSeq,
      );
      const creq = kinds.indexOf("cancel-requested");
      expect(creq).toBeGreaterThan(-1);
      // No step-end for the held step was ever accepted — or, if one was, only after the request.
      if (holdEnd !== -1) expect(holdEnd).toBeGreaterThan(creq);
      expect(effects).toEqual({ before: 1, hold: 1, after: 0, undo: 1 });
      const terminal = loaded.at(-1)!.entry as CancelledEntry;
      expect(terminal.kind).toBe("cancelled");
      expect(terminal.reason).toBe("operator stop");
      expect((await handle.status()).state).toBe("cancelled");
    } finally {
      release();
      for (const w of workers) await w.stop();
    }
  }

  it("ONE worker: the cancel lands while the step is held; the run ends cancelled", async () => {
    await cancelWhileHeld("cxrun1", 1);
  });

  it("TWO workers: the cancel lands while the step is held; the run ends cancelled", async () => {
    await cancelWhileHeld("cxrun2", 2);
  });
});
