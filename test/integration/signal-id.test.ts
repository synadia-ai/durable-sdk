// The signal `id` (durable-engine #19: signals journaled at send, deduplicated by `id`; its
// docs/API.md "signal"). The SDK sends an id on every signal, the caller's or a generated UUID, so
// a caller that retries after EngineUnavailableError can pass the id of the first try. The twin is
// python/tests/integration/test_signal_id.py.

import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { type NatsConnection } from "@nats-io/transport-node";
import { afterAll, beforeAll, describe, expect, inject, it } from "vitest";
import { defineWorkflow } from "../../src/core/workflow.js";
import { DurableTimeoutError } from "../../src/errors.js";
import { jsonCodec } from "../../src/journal/codec.js";
import { createClient, createWorker } from "../harness/engine.js";
import { connect } from "../harness/existing.js";
import { NatsJournal } from "../harness/shell/journal-store.js";
import { names, skipAsTenant } from "../harness/shell/names.js";

let nc: NatsConnection;

beforeAll(async () => {
  nc = await connect({ servers: inject("natsUrl")! });
});

afterAll(async () => {
  await nc.drain();
});

const factor = (): number => inject("rttFactor");

async function waitUntil(cond: () => Promise<boolean>, ms: number, what: string): Promise<void> {
  const until = Date.now() + ms * factor();
  while (Date.now() < until) {
    if (await cond()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`timed out waiting for ${what}`);
}

const PREFIX = "sigid";

/** The `signal` entries of a run's journal, named `name`. */
async function signalEntries(runId: string, name: string): Promise<Array<Record<string, unknown>>> {
  const journal = new NatsJournal(jetstream(nc), await jetstreamManager(nc), names(PREFIX), jsonCodec); // prettier-ignore
  return (await journal.load(runId))
    .map((l) => l.entry as unknown as Record<string, unknown>)
    .filter((e) => e.kind === "signal" && e.name === name);
}

// Parks on "gate" while the "go" signals are journaled, then takes one "go" and waits briefly
// for a second: what the run took is its result.
const twice = defineWorkflow("sigid_twice", async (ctx) => {
  await ctx.await<string>("gate");
  const first = await ctx.await<string>("go");
  try {
    return [first, await ctx.await<string>("go", { timeoutMs: 1_500 })];
  } catch (e) {
    if (e instanceof DurableTimeoutError) return [first];
    throw e;
  }
});

// Generation 0 takes one "go", then continues; generation 1 parks.
const rolls = defineWorkflow("sigid_rolls", async (ctx, input: { n: number }) => {
  if (input.n === 0) {
    await ctx.await<string>("go");
    return ctx.continueAsNew({ n: 1 });
  }
  return ctx.await<string>("never");
});

describe("the signal id", () => {
  it("two signals with the same id journal one entry, and the run takes it once", async () => {
    skipAsTenant(); // the test reads the journal itself (shell/names.ts)
    const worker = createWorker({ nc, workflows: [twice], prefix: PREFIX, streamMaxBytes: 64 * 1024 * 1024 }); // prettier-ignore
    await worker.start();
    try {
      const client = createClient({ nc, prefix: PREFIX });
      const h = await client.start(twice, {});
      await waitUntil(async () => (await h.status()).state === "suspended", 10_000, "the run parked"); // prettier-ignore
      await client.signal(h.runId, "go", "a", { id: "go-1" });
      await client.signal(h.runId, "go", "a", { id: "go-1" }); // the retry: ok, nothing journaled
      await client.signal(h.runId, "gate", null);
      expect(await h.result(15_000)).toEqual(["a"]);
      const entries = await signalEntries(h.runId, "go");
      expect(entries).toHaveLength(1);
      expect(entries[0]!.signalId).toBe("go-1");
    } finally {
      await worker.stop();
    }
  });

  it("two signals without an id are two signals, each with a generated id", async () => {
    skipAsTenant(); // the test reads the journal itself (shell/names.ts)
    const worker = createWorker({ nc, workflows: [twice], prefix: PREFIX, streamMaxBytes: 64 * 1024 * 1024 }); // prettier-ignore
    await worker.start();
    try {
      const client = createClient({ nc, prefix: PREFIX });
      const h = await client.start(twice, {});
      await waitUntil(async () => (await h.status()).state === "suspended", 10_000, "the run parked"); // prettier-ignore
      await client.signal(h.runId, "go", "a");
      await client.signal(h.runId, "go", "b");
      await client.signal(h.runId, "gate", null);
      expect(await h.result(15_000)).toEqual(["a", "b"]);
      const entries = await signalEntries(h.runId, "go");
      expect(entries).toHaveLength(2);
      const ids = entries.map((e) => e.signalId as string);
      expect(new Set(ids).size).toBe(2);
      for (const id of ids) expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/); // prettier-ignore
    } finally {
      await worker.stop();
    }
  });

  it("an invalid id is refused before any request", async () => {
    const client = createClient({ nc, prefix: PREFIX });
    // A subscriber of its own on the signal subject sees every request the client would send.
    const sent: string[] = [];
    const probe = await connect({ servers: inject("natsUrl")! });
    const sub = probe.subscribe(`${names(PREFIX).prefix}.api.v1.signal`, { callback: (_e, m) => void sent.push(m.subject) }); // prettier-ignore
    await probe.flush();
    try {
      for (const id of ["", "has space", "a.b", "a~b", "line\n"]) {
        await expect(client.signal("some-run", "go", 1, { id })).rejects.toThrow(/signal id must be ASCII alphanumeric/); // prettier-ignore
      }
      await probe.flush();
      expect(sent).toEqual([]);
    } finally {
      sub.unsubscribe();
      await probe.close();
    }
  });

  it("a retry after a rollover is recognised in the run named: ok, nothing journaled in _g1", async () => {
    skipAsTenant(); // the test reads the journal itself (shell/names.ts)
    const worker = createWorker({ nc, workflows: [rolls], prefix: PREFIX, streamMaxBytes: 64 * 1024 * 1024 }); // prettier-ignore
    await worker.start();
    try {
      const client = createClient({ nc, prefix: PREFIX });
      const h = await client.start(rolls, { n: 0 });
      await waitUntil(async () => (await h.status()).state === "suspended", 10_000, "generation 0 parked"); // prettier-ignore
      // The first try is journaled in generation 0, the run the retry names too: one of the two
      // journals the engine looks in (the other, the generation current at the retry, is _g1).
      await client.signal(h.runId, "go", "first", { id: "roll-1" });
      const gen1 = `${h.runId}_g1`;
      await waitUntil(async () => (await client.get(gen1).status()).state === "suspended", 10_000, "generation 1 parked"); // prettier-ignore
      await client.signal(h.runId, "go", "first", { id: "roll-1" }); // the retry: answered ok
      expect(await signalEntries(h.runId, "go")).toHaveLength(1);
      expect(await signalEntries(gen1, "go")).toHaveLength(0);
      expect((await client.get(gen1).status()).state).toBe("suspended");
    } finally {
      await worker.stop();
    }
  });
});
