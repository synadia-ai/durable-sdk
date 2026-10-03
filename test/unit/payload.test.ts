// An entry over the server's max_payload is DETERMINISTIC for its value: it must terminally FAIL
// the run (like a BigInt/NaN), never be classified as transient infrastructure — the pre-fix
// behavior was an infinite NAK loop that also head-of-line-blocked the workflow's consumer.

import { describe, expect, it } from "vitest";
import { executeRun } from "../../src/core/execute.js";
import { defineWorkflow } from "../../src/core/workflow.js";
import { PayloadTooLargeError, SerializationError } from "../../src/errors.js";
import { jsonCodec } from "../../src/journal/codec.js";
import { msgId, stamp } from "../../src/journal/entry.js";
import { InMemoryJournal } from "../../src/journal/store.js";

const LIMIT = 1024;

async function seedStart(j: InMemoryJournal, runId: string, input: unknown = {}): Promise<void> {
  await j.append(runId, {
    entry: stamp({ kind: "start", seq: 0, wf: "w", wfVersion: 1, input }, 0),
    msgID: msgId.start(runId),
    expectLastSeq: 0,
  });
}

describe("payload too large (deterministic → terminal fail, never retry)", () => {
  it("the journal double rejects an oversized entry with PayloadTooLargeError (a SerializationError)", async () => {
    const j = new InMemoryJournal(jsonCodec, { maxPayload: LIMIT });
    const p = j.append("r0", {
      entry: stamp(
        { kind: "start", seq: 0, wf: "w", wfVersion: 1, input: "x".repeat(4 * LIMIT) },
        0,
      ),
      msgID: msgId.start("r0"),
      expectLastSeq: 0,
    });
    await expect(p).rejects.toBeInstanceOf(PayloadTooLargeError);
    await expect(p).rejects.toBeInstanceOf(SerializationError); // the core's fail classification
  });

  it("a step whose RESULT is oversized fails the run terminally (fail entry, not a retry outcome)", async () => {
    const j = new InMemoryJournal(jsonCodec, { maxPayload: LIMIT });
    await seedStart(j, "r1");
    let executions = 0;
    const wf = defineWorkflow("w", async (ctx) => {
      return ctx.run("huge", () => {
        executions += 1;
        return "x".repeat(4 * LIMIT);
      });
    });
    const out = await executeRun("r1", { workflow: wf, journal: j, loaded: await j.load("r1") });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.name).toBe("PayloadTooLargeError");
    expect(executions).toBe(1);
    const loaded = await j.load("r1");
    expect(loaded.filter((l) => l.entry.kind === "fail")).toHaveLength(1);

    // A bare re-execution reports the SAME failed terminal without running anything: a journal
    // that holds a terminal is never executed (no loop, no second firing, still one fail entry).
    const out2 = await executeRun("r1", { workflow: wf, journal: j, loaded });
    expect(out2.kind).toBe("failed");
    if (out2.kind === "failed") expect(out2.error.name).toBe("PayloadTooLargeError");
    expect(executions).toBe(1);
    expect((await j.load("r1")).filter((l) => l.entry.kind === "fail")).toHaveLength(1);
  });

  it("an oversized sub-workflow INPUT fails the parent terminally at the call command", async () => {
    const j = new InMemoryJournal(jsonCodec, { maxPayload: LIMIT });
    await seedStart(j, "r2");
    const child = defineWorkflow("child", (_ctx, _input: string) => Promise.resolve("ok"));
    const wf = defineWorkflow("w", async (ctx) => ctx.call(child, "x".repeat(4 * LIMIT)));
    const out = await executeRun("r2", {
      workflow: wf,
      journal: j,
      loaded: await j.load("r2"),
      children: { start: () => Promise.resolve() },
    });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.name).toBe("PayloadTooLargeError");
  });

  it("an oversized workflow RETURN VALUE fails terminally with a (small) fail entry", async () => {
    const j = new InMemoryJournal(jsonCodec, { maxPayload: LIMIT });
    await seedStart(j, "r3");
    const wf = defineWorkflow("w", () => Promise.resolve("x".repeat(4 * LIMIT)));
    const out = await executeRun("r3", { workflow: wf, journal: j, loaded: await j.load("r3") });
    expect(out.kind).toBe("failed");
    if (out.kind === "failed") expect(out.error.name).toBe("PayloadTooLargeError");
    expect((await j.load("r3")).some((l) => l.entry.kind === "fail")).toBe(true);
  });
});
