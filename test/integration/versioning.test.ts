// End-to-end against a real nats-server: version-pinned dispatch. A worker may register multiple
// versions of one workflow name; each run executes the EXACT version its `start` recorded, and
// continueAsNew({version}) migrates a chain onto a new version. See docs/DESIGN-versioning.md.

import { type NatsConnection } from "@nats-io/transport-node";
import { connect } from "../harness/existing.js";
import { afterAll, beforeAll, describe, expect, inject, it } from "vitest";
import { defineWorkflow } from "../../src/core/workflow.js";
import { CancelledError } from "../../src/errors.js";
import { createClient } from "../harness/engine.js";
import { createWorker } from "../harness/engine.js";
import { WAN } from "../harness/mode.js";

const natsUrl = inject("natsUrl");
const SMALL = 64 * 1024 * 1024; // keep each prefix's reserved max_bytes frugal

async function waitUntil(cond: () => Promise<boolean>, timeoutMs: number): Promise<void> {
  timeoutMs *= WAN; // durable-sdk: × the round-trip factor (harness/mode.ts, F29)
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await cond()) return;
    if (Date.now() > deadline) throw new Error("waitUntil timed out");
    await new Promise((r) => setTimeout(r, 50));
  }
}

describe.skipIf(!natsUrl)("versioning (version-pinned dispatch, real nats-server)", () => {
  let nc: NatsConnection;
  beforeAll(async () => {
    nc = await connect({ servers: natsUrl! });
  });
  afterAll(async () => {
    await nc.drain();
  });

  it("a worker registers v1 + v2 of one name; each run executes its recorded version", async () => {
    const v1 = defineWorkflow("ver_coexist", (ctx) => ctx.run("s", () => "v1-result"), {
      version: 1,
    });
    const v2 = defineWorkflow("ver_coexist", (ctx) => ctx.run("s", () => "v2-result"), {
      version: 2,
    });
    const worker = createWorker({
      nc,
      workflows: [v1, v2],
      prefix: "e2ever1",
      streamMaxBytes: SMALL,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2ever1" });
      const h1 = await client.start(v1, {});
      const h2 = await client.start(v2, {});
      expect(await h1.result(15_000)).toBe("v1-result");
      expect(await h2.result(15_000)).toBe("v2-result");
      expect((await h1.status()).version).toBe(1);
      expect((await h2.status()).version).toBe(2);
    } finally {
      await worker.stop();
    }
  });

  it("a run started under v1 resumes on v1 after v2 is ALSO deployed (across a redeploy)", async () => {
    const v1 = defineWorkflow("ver_resume", (ctx) => ctx.await<string>("go").then((s) => `v1:${s}`), { version: 1 }); // prettier-ignore
    const v2 = defineWorkflow("ver_resume", (ctx) => ctx.await<string>("go").then((s) => `v2:${s}`), { version: 2 }); // prettier-ignore

    // Deploy #1: only v1 exists. Start a v1 run; it suspends on the signal.
    const w1 = createWorker({ nc, workflows: [v1], prefix: "e2ever2", streamMaxBytes: SMALL });
    await w1.start();
    const client = createClient({ nc, prefix: "e2ever2" });
    const h = await client.start(v1, {});
    await waitUntil(async () => (await h.status()).state === "suspended", 10_000);
    await w1.stop();

    // Deploy #2: v1 + v2 both registered. The wake must dispatch the in-flight run to v1, not v2.
    const w2 = createWorker({ nc, workflows: [v1, v2], prefix: "e2ever2", streamMaxBytes: SMALL });
    await w2.start();
    try {
      await client.signal(h.runId, "go", "ok");
      expect(await h.result(15_000)).toBe("v1:ok"); // v1 — NOT "v2:ok"
      expect((await h.status()).version).toBe(1);
    } finally {
      await w2.stop();
    }
  });

  it("continueAsNew({version}) migrates the chain onto v2 end-to-end", async () => {
    // gen0 runs v1 and immediately rolls the chain onto v2; gen1 runs v2 and returns.
    const v1 = defineWorkflow("ver_migrate", (ctx) => ctx.continueAsNew({}, { version: 2 }), { version: 1 }); // prettier-ignore
    const v2 = defineWorkflow("ver_migrate", (ctx) => ctx.run("done", () => "ran-on-v2"), {
      version: 2,
    });
    const worker = createWorker({
      nc,
      workflows: [v1, v2],
      prefix: "e2ever3",
      streamMaxBytes: SMALL,
    });
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2ever3" });
      const h = await client.start(v1, {});
      // result() transparently follows the chain gen0(v1, continued) → gen1(v2, result).
      expect(await h.result(15_000)).toBe("ran-on-v2");
      expect((await h.status()).state).toBe("continued");
      const g1 = client.get(`${h.runId}_g1`);
      expect((await g1.status()).state).toBe("completed");
      expect((await g1.status()).version).toBe(2); // the next generation was stamped v2
    } finally {
      await worker.stop();
    }
  });

  it("a run whose version no worker has PARKS (blocked), then RESUMES once it is deployed", async () => {
    const v1 = defineWorkflow("ver_park", (ctx) => ctx.run("s", () => "done-on-v1"), {
      version: 1,
    });
    const v2 = defineWorkflow("ver_park", (ctx) => ctx.run("s", () => "done-on-v2"), {
      version: 2,
    });

    // Only v2 is deployed; start a v1 run — no worker can run it, so it must park (not fail, not drop).
    const onlyV2 = createWorker({ nc, workflows: [v2], prefix: "e2ever4", versionWaitMs: 250, streamMaxBytes: SMALL }); // prettier-ignore
    await onlyV2.start();
    const client = createClient({ nc, prefix: "e2ever4" });
    const h = await client.start(v1, {});
    await waitUntil(async () => (await h.status()).state === "blocked", 10_000);
    expect((await h.status()).detail).toMatch(/ver_park v1/); // loud, with a reason
    await onlyV2.stop();

    // Deploy v1 → the parked run resumes on v1 and completes. Nothing was lost.
    const withV1 = createWorker({ nc, workflows: [v1], prefix: "e2ever4", versionWaitMs: 250, streamMaxBytes: SMALL }); // prettier-ignore
    await withV1.start();
    try {
      expect(await h.result(15_000)).toBe("done-on-v1");
      expect((await h.status()).state).toBe("completed");
    } finally {
      await withV1.stop();
    }
  });

  it("cancelling a BLOCKED run terminates it (cancelled) with no code to run", async () => {
    const v1 = defineWorkflow("ver_cb", (ctx) => ctx.run("s", () => "v1"), { version: 1 });
    const v2 = defineWorkflow("ver_cb", (ctx) => ctx.run("s", () => "v2"), { version: 2 });
    const onlyV2 = createWorker({ nc, workflows: [v2], prefix: "e2ever5", versionWaitMs: 250, streamMaxBytes: SMALL }); // prettier-ignore
    await onlyV2.start();
    try {
      const client = createClient({ nc, prefix: "e2ever5" });
      const h = await client.start(v1, {}); // parks: v1 not deployed here
      await waitUntil(async () => (await h.status()).state === "blocked", 10_000);
      // The operator deliberately kills it — the worker writes the `cancelled` terminal directly.
      await client.cancel(h.runId, { reason: "v1 decommissioned" });
      await expect(h.result(10_000)).rejects.toBeInstanceOf(CancelledError); // journal truth
    } finally {
      await onlyV2.stop();
    }
  });

  it("mixed fleet: a v2 run completes on v2 even while a v1-only worker keeps parking it", async () => {
    const v1 = defineWorkflow("ver_fleet", (ctx) => ctx.run("s", () => "v1"), { version: 1 });
    const v2 = defineWorkflow("ver_fleet", (ctx) => ctx.run("s", () => "v2"), { version: 2 });
    const a = createWorker({
      nc,
      workflows: [v1],
      prefix: "e2ever6",
      versionWaitMs: 250,
      streamMaxBytes: SMALL,
    }); // v1-only: parks v2 // prettier-ignore
    const b = createWorker({
      nc,
      workflows: [v1, v2],
      prefix: "e2ever6",
      versionWaitMs: 250,
      streamMaxBytes: SMALL,
    }); // has v2 // prettier-ignore
    await a.start();
    await b.start();
    try {
      const client = createClient({ nc, prefix: "e2ever6" });
      const h = await client.start(v2, {});
      expect(await h.result(15_000)).toBe("v2"); // B ran it; A's repeated parking never lost it
    } finally {
      await a.stop();
      await b.stop();
    }
  });

  it("continueAsNew to an UNREGISTERED version parks the next generation (doesn't fail the chain)", async () => {
    // Only v1 is deployed; gen1 is stamped v2 and must PARK — proving the persisted next-gen version
    // (on the `continued` entry) is what the worker re-drives to, and that a missing migration target
    // is recoverable, not fatal.
    const v1 = defineWorkflow("ver_migpark", (ctx) => ctx.continueAsNew({}, { version: 2 }), { version: 1 }); // prettier-ignore
    const worker = createWorker({ nc, workflows: [v1], prefix: "e2ever7", versionWaitMs: 250, streamMaxBytes: SMALL }); // prettier-ignore
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2ever7" });
      const h = await client.start(v1, {});
      const g1 = client.get(`${h.runId}_g1`);
      await waitUntil(async () => (await g1.status()).state === "blocked", 10_000);
      expect((await g1.status()).version).toBe(2); // gen1 was stamped v2 (persisted), then parked
      expect((await g1.status()).detail).toMatch(/ver_migpark v2/);
    } finally {
      await worker.stop();
    }
  });

  it("a sub-workflow (ctx.call) dispatches the child to its recorded version", async () => {
    const kidV1 = defineWorkflow("ver_kid", (ctx, x: number) => ctx.run("c", () => `kid-v1:${x}`), { version: 1 }); // prettier-ignore
    const kidV2 = defineWorkflow("ver_kid", (ctx, x: number) => ctx.run("c", () => `kid-v2:${x}`), { version: 2 }); // prettier-ignore
    const dad = defineWorkflow("ver_dad", (ctx) => ctx.call(kidV2, 7), { version: 1 });
    const worker = createWorker({ nc, workflows: [dad, kidV1, kidV2], prefix: "e2ever8", streamMaxBytes: SMALL }); // prettier-ignore
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2ever8" });
      const h = await client.start(dad, {});
      expect(await h.result(15_000)).toBe("kid-v2:7"); // the child ran v2, not v1
    } finally {
      await worker.stop();
    }
  });

  it("a child whose version is missing PARKS; the parent waits; deploying it resumes both", async () => {
    const kidV1 = defineWorkflow("ver_kid2", (ctx, x: number) => ctx.run("c", () => `cv1:${x}`), { version: 1 }); // prettier-ignore
    const kidV2 = defineWorkflow("ver_kid2", (ctx, x: number) => ctx.run("c", () => `cv2:${x}`), { version: 2 }); // prettier-ignore
    const parent = defineWorkflow("ver_dad2", (ctx) => ctx.call(kidV2, 5), { version: 1 });

    // Deploy parent + child v1 only — the parent calls child v2, so the child run parks.
    const w1 = createWorker({ nc, workflows: [parent, kidV1], prefix: "e2ever9", versionWaitMs: 250, streamMaxBytes: SMALL }); // prettier-ignore
    const w2 = createWorker({ nc, workflows: [kidV2], prefix: "e2ever9", versionWaitMs: 250, streamMaxBytes: SMALL }); // prettier-ignore
    await w1.start();
    try {
      const client = createClient({ nc, prefix: "e2ever9" });
      const h = await client.start(parent, {});
      const childId = `${h.runId}_0`; // deterministic child runId (${parent}_${seq})
      await waitUntil(async () => (await client.get(childId).status()).state === "blocked", 10_000);
      expect((await client.get(childId).status()).version).toBe(2);
      expect((await h.status()).state).not.toBe("completed"); // parent still waiting on the child

      await w2.start(); // deploy child v2 → child resumes → parent resumes
      expect(await h.result(15_000)).toBe("cv2:5");
    } finally {
      await w1.stop();
      await w2.stop();
    }
  });

  it("a same-version continue-as-new loop does NOT jump versions when a newer version is also deployed", async () => {
    const v1 = defineWorkflow(
      "ver_loop",
      async (ctx, input: { n: number }) =>
        input.n >= 2 ? `done@${input.n}` : ctx.continueAsNew({ n: input.n + 1 }), // no version → same
      { version: 1 },
    );
    const v2 = defineWorkflow("ver_loop", () => Promise.resolve("v2-should-not-run"), {
      version: 2,
    });
    const worker = createWorker({ nc, workflows: [v1, v2], prefix: "e2ever10", streamMaxBytes: SMALL }); // prettier-ignore
    await worker.start();
    try {
      const client = createClient({ nc, prefix: "e2ever10" });
      const h = await client.start(v1, { n: 0 });
      expect(await h.result(15_000)).toBe("done@2"); // ran v1 across every generation
      expect((await client.get(`${h.runId}_g1`).status()).version).toBe(1); // stayed v1, never jumped
    } finally {
      await worker.stop();
    }
  });
});
