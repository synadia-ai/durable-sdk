// durable-sdk: the engine's two limit refusals, end to end (F40; the engine's F46). A licence
// limit on a standalone engine of the test's own (a test licence with `-workflows 1
// -runs-in-flight 1`), and a tenant limit on an engine in the fabric mode on a three-account
// server of the test's own (the tenant's record `maxWorkflows: 1` in `KV_DE_TENANTS`, as an
// operator sets it). Each is a `LimitError` naming the limit: `worker.start()` rejects with it,
// `client.start()` rejects with it after one request. Both need a server and an engine of their
// own, so neither runs against an existing server.

import { Kvm } from "@nats-io/kv";
import { type NatsConnection } from "@nats-io/nats-core";
import { afterEach, expect, inject, it } from "vitest";
import { defineWorkflow } from "../../src/core/workflow.js";
import { createClient } from "../../src/engine/client.js";
import { LimitError } from "../../src/engine/errors.js";
import { createWorker, type Worker } from "../../src/engine/worker.js";
import { mintTestLicence } from "../harness/engine-bin.js";
import { countOf } from "../harness/engine.js";
import { EngineProcess } from "../harness/engine-process.js";
import { connect } from "../harness/existing.js";
import { describeUnlessExisting } from "../harness/mode.js";
import { NatsServerProcess, renderTenancyConfig } from "../harness/nats-server.js";

const bin = inject("engineBin");
const OWN = "needs a server and an engine of its own, with a licence or a tenant's limits of its own"; // prettier-ignore
const own = describeUnlessExisting(OWN, !!bin && !!inject("natsDirectUrl"));
const MIB = 1024 * 1024;

/** What a test opened, closed after it whatever happened. */
class Scope {
  #closers: Array<() => Promise<unknown>> = [];
  defer(f: () => Promise<unknown>): void {
    this.#closers.push(f);
  }
  async conn(opts: Parameters<typeof connect>[0]): Promise<NatsConnection> {
    const nc = await connect(opts);
    this.defer(() => nc.close());
    return nc;
  }
  worker(w: Worker): Worker {
    this.defer(() => w.stop());
    return w;
  }
  async close(): Promise<void> {
    for (const f of this.#closers.splice(0).reverse()) await f().catch(() => undefined);
  }
}

const parked = defineWorkflow("lim_parked", async (ctx) => ctx.await<number>("go"));
const other = defineWorkflow("lim_other", () => Promise.resolve("never"));

async function counts(nc: NatsConnection, prefix: string): Promise<Record<string, number>> {
  const r = await nc.request(`${prefix}.api.v1.info`, new TextEncoder().encode("{}"), { timeout: 5_000 }); // prettier-ignore
  return r.json<{ counts?: Record<string, number> }>().counts ?? {};
}

own("a licence limit (a standalone engine)", () => {
  const scope = new Scope();
  afterEach(() => scope.close());

  it("a worker for a workflow past the licence's is refused at start(), and a run past its runs in flight at start()", async () => {
    const prefix = "delim";
    const url = inject("natsDirectUrl")!;
    const licence = mintTestLicence(bin!, "durable-sdk-limits", { workflows: 1, "runs-in-flight": 1 }); // prettier-ignore
    const engine = new EngineProcess(bin!, url, prefix, { streamMaxBytes: 16 * MIB }, { licence });
    await engine.start();
    scope.defer(() => engine.stop());
    expect(engine.log.join("\n")).toMatch(/msg=licence .*licensee=durable-sdk-limits.*max_workflows=1/); // prettier-ignore
    const nc = await scope.conn({ servers: inject("natsUrl")! });

    // The licence's one workflow name binds; a second is refused, loud, and start() says so.
    const first = scope.worker(createWorker({ nc, prefix, workflows: [parked], concurrency: 1 }));
    await first.start();
    const second = createWorker({ nc, prefix, workflows: [other], concurrency: 1 });
    const e = (await second.start().catch((x: unknown) => x)) as LimitError;
    expect(e).toBeInstanceOf(LimitError);
    expect([e.code, e.limit, e.op]).toEqual(["licence_limit", "workflows", "poll"]);
    expect(e.message).toContain("licence's limit of 1");
    expect(second.health().running).toBe(false);

    // One run in flight is the licence's; a second start is refused once, never retried.
    const client = createClient({ nc, prefix });
    await client.start(parked, undefined, { runId: "lim-1" });
    const before = countOf(await counts(nc, prefix), "de_engine_refused_total", { code: "licence_limit" }); // prettier-ignore
    const r = (await client.start(parked, undefined, { runId: "lim-2" }).catch((x: unknown) => x)) as LimitError; // prettier-ignore
    expect(r).toBeInstanceOf(LimitError);
    expect([r.code, r.limit, r.op]).toEqual(["licence_limit", "runs", "start"]);
    const after = countOf(await counts(nc, prefix), "de_engine_refused_total", { code: "licence_limit" }); // prettier-ignore
    expect(after - before).toBe(1);

    // The run in flight still finishes: a limit refuses what is new, never what runs.
    await client.signal("lim-1", "go", 7);
    expect(await client.get("lim-1").result(20_000)).toBe(7);
  });
});

own("a tenant limit (the fabric mode)", () => {
  const scope = new Scope();
  afterEach(() => scope.close());

  it("a worker for a workflow past the tenant's is refused at start()", async () => {
    const prefix = "de";
    const server = new NatsServerProcess({ config: (port, store) => renderTenancyConfig(port, store, prefix, ["A", "B"]) }); // prettier-ignore
    await server.start();
    scope.defer(() => server.stop());
    const engine = new EngineProcess(bin!, `nats://de:de@127.0.0.1:${server.url.split(":").at(-1)!}`, prefix, {}, {
      licence: null, // the fabric mode needs none, and one would bound its tenants
      args: ["--tenancy", "--tenant-max-bytes", "0"],
    }); // prettier-ignore
    await engine.start();
    scope.defer(() => engine.stop());

    // The tenant's record, before its first request (the engine reads it then, and every 10 s).
    const admin = await scope.conn({ servers: server.url, user: "de", pass: "de" });
    const kv = await new Kvm(admin).open(`${prefix.toUpperCase()}_TENANTS`);
    await kv.put("A", JSON.stringify({ maxWorkflows: 1 }));

    const nc = await scope.conn({ servers: server.url, user: "a-client", pass: "a-client" });
    const first = scope.worker(createWorker({ nc, workflows: [parked], concurrency: 1 }));
    await first.start();
    const second = createWorker({ nc, workflows: [other], concurrency: 1 });
    const e = (await second.start().catch((x: unknown) => x)) as LimitError;
    expect(e).toBeInstanceOf(LimitError);
    expect([e.code, e.limit, e.op]).toEqual(["tenant_limit", "workflows", "poll"]);
    expect(e.message).toContain("at its limit of 1");
    expect(second.health().running).toBe(false);

    // Tenant B is not affected: its own two names bind.
    const bnc = await scope.conn({ servers: server.url, user: "b-client", pass: "b-client" });
    for (const wf of [parked, other]) {
      await scope.worker(createWorker({ nc: bnc, workflows: [wf], concurrency: 1 })).start();
    }
  });
});
