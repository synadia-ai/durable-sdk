// Large-payload offload against a real nats-server (sizes relative to its max_payload, so multi-×
// values MUST offload to complete at all). Proves: end-to-end round-trips through the real object
// store, crash → fresh-worker replay resolves blobs without re-executing steps, a missing blob
// PARKS the run (loud, recoverable) and a restored blob heals it, and small runs never touch the
// bucket.

import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import { Objm } from "@nats-io/obj";
import { listObjects, readBlob } from "../harness/blobs.js";
import { connect, type NatsConnection } from "@nats-io/transport-node";
import { afterAll, beforeAll, describe, expect, inject, it } from "vitest";
import { defineWorkflow } from "../../src/core/workflow.js";
import { BlobMissingError } from "../../src/errors.js";
import { type DurableEventFields, type DurableEventName } from "../../src/internal/metrics.js";
import { jsonCodec } from "../../src/journal/codec.js";
import { createClient } from "../harness/engine.js";
import { NatsJournal } from "../harness/shell/journal-store.js";
import { names } from "../harness/shell/names.js";
import { createWorker } from "../harness/engine.js";
import { maxPayload } from "../harness/payload.js";

const natsUrl = inject("natsUrl");

async function waitUntil(cond: () => Promise<boolean>, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await cond()) return;
    if (Date.now() > deadline) throw new Error("waitUntil timed out");
    await new Promise((r) => setTimeout(r, 50));
  }
}

describe.skipIf(!natsUrl)("large-payload offload (real nats-server + object store)", () => {
  let nc: NatsConnection;
  /** This server's max_payload, and 1.5× it — every "big" value below is sized from these. */
  // The object-store client writes a blob as 128 KiB chunks, ONE ACKED PUBLISH AT A TIME, so an
  // offloaded payload costs a dependent round trip per chunk: this test moves ~56 MiB (≈ 450
  // chunks) and, under DE_TEST_RTT_MS, that is what its time is made of. 1000 ms per ms of RTT is
  // those chunks twice over. (A cost, measured and recorded in OPERATIONS — not a failure.)
  const rtt = inject("natsRttMs");
  const BIG_CHILD_PER_RTT_MS = 1_000;
  let P = 0;
  let P15 = 0;
  beforeAll(async () => {
    nc = await connect({ servers: natsUrl! });
    P = maxPayload(nc);
    P15 = Math.ceil(1.5 * P);
  });
  afterAll(async () => {
    await nc.drain();
  });

  // durable-sdk: BLOCKED by docs/findings.md F1 (API v1 has no chunked upload: a value over max_payload − 16 KiB cannot
  // reach the engine, where the library offloaded it to the blob store).
  it.skip("multi-MiB input and step values round-trip; the stream carries refs, the bucket the bytes", async () => {
    const prefix = "off1";
    const events: Array<{ name: DurableEventName; fields: DurableEventFields }> = [];
    const sink = { onEvent: (name: DurableEventName, fields: DurableEventFields): void => void events.push({ name, fields }) }; // prettier-ignore
    const wf = defineWorkflow("blobby", async (ctx, input: { data: string }) => {
      const derived = await ctx.run("derive", () => input.data.length + "b".repeat(2 * P));
      return { inLen: input.data.length, derivedLen: derived.length };
    });
    const worker = createWorker({ nc, workflows: [wf], prefix });
    await worker.start();
    try {
      const client = createClient({ nc, prefix, metrics: sink });
      // 1.5 × max_payload of input — over this server's limit: without offload, start() itself
      // would reject with PayloadTooLargeError.
      const handle = await client.start(wf, { data: "a".repeat(P15) });
      expect(await handle.result(20_000)).toEqual({
        inLen: P15,
        derivedLen: String(P15).length + 2 * P,
      });

      // The client offloaded the start input (blob.put observed through the metrics seam).
      expect(events.some((e) => e.name === "blob.put" && e.fields.outcome === "ok")).toBe(true);

      // The bucket holds this run's content-addressed objects…
      const mine = (await listObjects(nc, names(prefix).blobsBucket)).filter((o) => o.name.startsWith(`${handle.runId}/`)); // prettier-ignore
      expect(mine.length).toBeGreaterThan(0);
      expect(mine.every((o) => /\/sha256:[0-9a-f]{64}$/.test(o.name))).toBe(true);

      // …and its backing stream is discard:new (a full bucket rejects loudly, never corrupts).
      const jsm = await jetstreamManager(nc);
      const cfg = (await jsm.streams.info(`OBJ_${names(prefix).blobsBucket}`)).config;
      expect(cfg.discard).toBe("new");

      // A pre-offload reader (no blob store) fails LOUD on this journal — the v:2/ref guard that
      // turns a mixed-fleet mistake into a parked run instead of silent undefined-corruption.
      const bare = new NatsJournal(
        jetstream(nc),
        jsm,
        names(prefix),
        jsonCodec,
        nc.info?.max_payload,
      );
      await expect(bare.load(handle.runId)).rejects.toBeInstanceOf(BlobMissingError);
    } finally {
      await worker.stop();
    }
  });

  // durable-sdk: BLOCKED by docs/findings.md F1 (API v1 has no chunked upload: a value over max_payload − 16 KiB cannot
  // reach the engine, where the library offloaded it to the blob store).
  it.skip("crash mid-run: a FRESH worker resolves blobs from the store and replays without re-executing", async () => {
    const prefix = "off2";
    let executions = 0;
    const wf = defineWorkflow("resume_big", async (ctx) => {
      const big = await ctx.run("expensive", () => {
        executions += 1;
        return "x".repeat(2 * P);
      });
      const go = await ctx.await<string>("go");
      return `${go}:${big.length}`;
    });

    const w1 = createWorker({ nc, workflows: [wf], prefix });
    await w1.start();
    const client = createClient({ nc, prefix });
    const handle = await client.start(wf, {});
    await waitUntil(async () => (await handle.status()).state === "suspended", 15_000);
    await w1.stop(); // the "crash": the only worker that ever saw the live value is gone

    const w2 = createWorker({ nc, workflows: [wf], prefix });
    await w2.start();
    try {
      await client.signal(handle.runId, "go", "resumed");
      expect(await handle.result(20_000)).toBe(`resumed:${String(2 * P)}`);
      expect(executions).toBe(1); // replayed from the blob, never re-executed
    } finally {
      await w2.stop();
    }
  });

  // durable-sdk: BLOCKED by docs/findings.md F1 (API v1 has no chunked upload: a value over max_payload − 16 KiB cannot
  // reach the engine, where the library offloaded it to the blob store).
  it.skip("a missing blob PARKS the run (loud, never a terminal) and a restored blob heals it", async () => {
    const prefix = "off3";
    const outcomes: string[] = [];
    const sink = {
      onEvent: (name: DurableEventName, fields: DurableEventFields): void => {
        if (name === "work.done") outcomes.push(String(fields.outcome));
      },
    };
    const wf = defineWorkflow("parky", async (ctx) => {
      const big = await ctx.run("big", () => "y".repeat(2 * P));
      await ctx.await("go");
      return big.length;
    });
    const worker = createWorker({ nc, workflows: [wf], prefix, metrics: sink });
    await worker.start();
    try {
      const client = createClient({ nc, prefix });
      const handle = await client.start(wf, {});
      await waitUntil(async () => (await handle.status()).state === "suspended", 15_000);

      // Sabotage: delete the step's blob out-of-band (an early GC / lost bucket), keeping a copy.
      const bucket = names(prefix).blobsBucket;
      const mine = (await listObjects(nc, bucket)).filter((o) => o.name.startsWith(`${handle.runId}/`)); // prettier-ignore
      expect(mine).toHaveLength(1);
      const name = mine[0]!.name;
      const bytes = await readBlob(nc, bucket, name);
      expect(bytes).not.toBeNull();
      const os = await new Objm(nc).open(bucket); // delete + putBlob are publishes: no consumer
      await os.delete(name);

      // The signal cannot be processed — the journal is unreadable — so the run PARKS: work items
      // NAK with the distinct outcome, and the run's status never becomes a terminal.
      await client.signal(handle.runId, "go", null);
      await waitUntil(() => Promise.resolve(outcomes.includes("blob-unresolvable")), 10_000);
      expect((await handle.status()).state).toBe("suspended");

      // Heal: restore the object (same name, same bytes — content-addressed) → the next
      // redelivery resolves, the signal lands, the run completes.
      await os.putBlob({ name }, bytes);
      expect(await handle.result(20_000)).toBe(2 * P);
    } finally {
      await worker.stop();
    }
  });

  // durable-sdk: BLOCKED by docs/findings.md F1 (API v1 has no chunked upload: a value over max_payload − 16 KiB cannot
  // reach the engine, where the library offloaded it to the blob store).
  it.skip("a multi-MiB SIGNAL rides the work queue as a ref and the journal entry carries it verbatim", async () => {
    const prefix = "off5";
    const wf = defineWorkflow("sigblob", async (ctx) => {
      const data = await ctx.await<string>("blob");
      return data.length;
    });
    const worker = createWorker({ nc, workflows: [wf], prefix });
    await worker.start();
    try {
      const client = createClient({ nc, prefix });
      const handle = await client.start(wf, {});
      await waitUntil(async () => (await handle.status()).state === "suspended", 15_000);
      // 2 × max_payload of signal data: the WORK ITEM itself could never publish inline.
      await client.signal(handle.runId, "blob", "s".repeat(2 * P));
      expect(await handle.result(20_000)).toBe(2 * P);

      // The wire form: the journal's signal entry holds dataRef (no inline data) — created once
      // by the client, passed through the work item and the worker append untouched.
      const jsm = await jetstreamManager(nc);
      const journal = new NatsJournal(jetstream(nc), jsm, names(prefix), jsonCodec);
      const raw = await journal.loadRaw(handle.runId);
      const sig = raw.find((l) => l.entry.kind === "signal")?.entry as
        { data?: unknown; dataRef?: { name: string } } | undefined;
      expect(sig?.dataRef?.name).toMatch(new RegExp(`^${handle.runId}/sha256:`));
      expect(sig?.data).toBeUndefined();
    } finally {
      await worker.stop();
    }
  });

  // durable-sdk: BLOCKED by docs/findings.md F1 (API v1 has no chunked upload: a value over max_payload − 16 KiB cannot
  // reach the engine, where the library offloaded it to the blob store).
  it.skip(
    "a big child INPUT and big child RESULT flow as refs through call/start/result/child-done/resolve",
    async () => {
      const prefix = "off6";
      const child = defineWorkflow("kid", async (ctx, input: { seed: string }) => {
        const out = await ctx.run("expand", () => input.seed.slice(0, P) + "z".repeat(P));
        return out;
      });
      const parent = defineWorkflow("dad", async (ctx) => {
        const childOut = await ctx.call(child, { seed: "q".repeat(P15) });
        return { childLen: childOut.length };
      });
      const worker = createWorker({ nc, workflows: [parent, child], prefix });
      await worker.start();
      try {
        const client = createClient({ nc, prefix });
        const handle = await client.start(parent, {});
        expect(await handle.result(25_000 + BIG_CHILD_PER_RTT_MS * rtt)).toEqual({
          childLen: 2 * P,
        });

        // Storage shape: each run's journal references only objects under ITS OWN runId (the GC
        // unit) — the same content stored under two prefixes is the price of per-run GC safety.
        const all = (await listObjects(nc, names(prefix).blobsBucket)).map((o) => o.name);
        const parentObjs = all.filter((x) => x.startsWith(`${handle.runId}/`));
        // parent: the call INPUT + the child-done VALUE, both under the parent's prefix
        expect(parentObjs).toHaveLength(2);
      } finally {
        await worker.stop();
      }
    },
    30_000 + BIG_CHILD_PER_RTT_MS * rtt,
  );

  // durable-sdk: BLOCKED by docs/findings.md F1 (API v1 has no chunked upload: a value over max_payload − 16 KiB cannot
  // reach the engine, where the library offloaded it to the blob store).
  it.skip("a big continue-as-new INPUT crosses generations as refs", async () => {
    const prefix = "off7";
    const wf = defineWorkflow("canblob", async (ctx, input: { n: number; payload: string }) => {
      if (input.n === 0) {
        await ctx.continueAsNew({ n: 1, payload: input.payload });
      }
      return input.payload.length;
    });
    const worker = createWorker({ nc, workflows: [wf], prefix });
    await worker.start();
    try {
      const client = createClient({ nc, prefix });
      const handle = await client.start(wf, { n: 0, payload: "c".repeat(P15) });
      expect(await handle.result(25_000)).toBe(P15);
    } finally {
      await worker.stop();
    }
  });

  // durable-sdk: BLOCKED by docs/findings.md F1 (API v1 has no chunked upload: a value over max_payload − 16 KiB cannot
  // reach the engine, where the library offloaded it to the blob store).
  it.skip("a multi-MiB failure MESSAGE offloads (fail entry + KV hint) and result() rejects with it", async () => {
    const prefix = "off8";
    const boom = "m".repeat(P15);
    const wf = defineWorkflow("failblob", async (ctx) => {
      await ctx.run("s", () => 1);
      throw new Error(boom);
    });
    const worker = createWorker({ nc, workflows: [wf], prefix });
    await worker.start();
    try {
      const client = createClient({ nc, prefix });
      const handle = await client.start(wf, {});
      await expect(handle.result(20_000)).rejects.toMatchObject({
        name: "WorkflowFailure",
        message: expect.stringContaining("mmmm") as unknown,
      });
      expect((await handle.status()).state).toBe("failed");
    } finally {
      await worker.stop();
    }
  });

  // durable-sdk: BLOCKED by docs/findings.md F1 (API v1 has no chunked upload: a value over max_payload − 16 KiB cannot
  // reach the engine, where the library offloaded it to the blob store).
  it.skip("a multi-MiB RESULT round-trips and the KV hint stores a ref, not the value", async () => {
    const prefix = "off9";
    const wf = defineWorkflow("resblob", async (ctx) => {
      const big = await ctx.run("make", () => "r".repeat(2 * P));
      return big;
    });
    const worker = createWorker({ nc, workflows: [wf], prefix });
    await worker.start();
    try {
      const client = createClient({ nc, prefix });
      const handle = await client.start(wf, {});
      expect((await handle.result(20_000)).length).toBe(2 * P);

      // Without the RunsStore shrink, finalizeCompleted would blow the KV max_payload and the
      // terminal re-drive would NAK-loop. The record must carry resultRef, never the value.
      const kv = await new Kvm(nc).create(names(prefix).runsKv);
      const rec = await kv.get(handle.runId);
      const parsed = JSON.parse(new TextDecoder().decode(rec!.value)) as {
        status: string;
        result?: unknown;
        resultRef?: { name: string; size: number };
      };
      expect(parsed.status).toBe("completed");
      expect(parsed.result).toBeUndefined();
      expect(parsed.resultRef?.name).toMatch(new RegExp(`^${handle.runId}/sha256:`));
    } finally {
      await worker.stop();
    }
  });

  // durable-sdk: BLOCKED by docs/findings.md F1 (API v1 has no chunked upload: a value over max_payload − 16 KiB cannot
  // reach the engine, where the library offloaded it to the blob store).
  it.skip("cancelling a run with offloaded entries stays clean (cancelled terminal, no value fetches needed)", async () => {
    const prefix = "off10";
    const wf = defineWorkflow("cancelblob", async (ctx) => {
      await ctx.run("big", () => "k".repeat(P15));
      await ctx.await("never");
      return "unreachable";
    });
    const worker = createWorker({ nc, workflows: [wf], prefix });
    await worker.start();
    try {
      const client = createClient({ nc, prefix });
      const handle = await client.start(wf, {});
      await waitUntil(async () => (await handle.status()).state === "suspended", 15_000);
      await client.cancel(handle.runId, { reason: "test" });
      await expect(handle.result(15_000)).rejects.toMatchObject({ name: "CancelledError" });
      expect((await handle.status()).state).toBe("cancelled");
    } finally {
      await worker.stop();
    }
  });

  it("small runs never touch the blob store", async () => {
    const prefix = "off4";
    const wf = defineWorkflow("tiny", async (ctx, n: number) => {
      const v = await ctx.run("s", () => n * 2);
      return v + 1;
    });
    const worker = createWorker({ nc, workflows: [wf], prefix });
    await worker.start();
    try {
      const client = createClient({ nc, prefix });
      const handle = await client.start(wf, 20);
      expect(await handle.result(15_000)).toBe(41);
      expect(await listObjects(nc, names(prefix).blobsBucket)).toHaveLength(0);
    } finally {
      await worker.stop();
    }
  });
});
