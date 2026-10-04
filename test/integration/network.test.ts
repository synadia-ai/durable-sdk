// The network axes: LATENCY and DISCONNECTS (docs/plans/test-axes.md, increment 3). Every other
// suite talks to a server ~0.1 ms away over a link that never drops, so the engine's timing
// constants and its reconnect behaviour had only ever been argued. Here the library's connections
// go through test/harness/proxy.ts — a byte-level TCP proxy that adds round-trip time, resets
// connections, and withholds replies — while the test's OWN observations go straight to the server.
//
// The instrument is tested first: a proxy that silently added no delay would turn every latency
// test below into a test at 0.1 ms that passes.

import net from "node:net";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { type NatsConnection } from "@nats-io/nats-core";
import { connect } from "../harness/existing.js";
import { afterEach, expect, inject, it } from "vitest";
import { defineWorkflow } from "../../src/core/workflow.js";
import { ConnectionClosedError } from "../../src/errors.js";
import { encodeJson } from "../../src/internal/json.js";
import {
  type DurableEventFields,
  type DurableEventName,
  type Metrics,
} from "../../src/internal/metrics.js";
import { jsonCodec } from "../../src/journal/codec.js";
import { createClient, engineLog } from "../harness/engine.js";
import { NatsJournal } from "../harness/shell/journal-store.js";
import { names } from "../harness/shell/names.js";
import { type WorkItem } from "../../src/core/work-item.js";
import { provision, setEngineServer } from "../harness/engine.js";
import { createWorker } from "../harness/engine.js";
import { type Worker } from "../../src/engine/worker.js";
import { type Ctx } from "../../src/types.js";
import { assertNoEphemeralConsumers } from "../harness/leftovers.js";
import { NatsServerProcess } from "../harness/nats-server.js";
import { NatsProxy } from "../harness/proxy.js";
import { describeUnlessExisting, OWN_LINK, OWN_SERVER, WAN } from "../harness/mode.js";

// The server itself. With DE_TEST_RTT_MS set, `natsUrl` is already a delay proxy, and a second
// proxy stacked on it would make every number below mean something else.
const serverUrl = inject("natsDirectUrl");
// durable-sdk: every suite here owns the link or the server, which an existing server (DE_NATS_URL)
// does not give it (harness/mode.ts).
const ownLink = describeUnlessExisting(OWN_LINK, !!serverUrl);
const ownServer = describeUnlessExisting(OWN_SERVER, !!serverUrl);
const MIB = 1024 * 1024;
const SIZED = { streamMaxBytes: 16 * MIB };
const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

type Recorded = { name: DurableEventName; fields: DurableEventFields; at: number; who: string };
/** One event log shared by every worker in a test; `who` says which worker spoke. */
function recorder(): { events: Recorded[]; sink: (who: string) => Metrics } {
  const events: Recorded[] = [];
  return {
    events,
    sink: (who) => ({
      onEvent: (name, fields) => void events.push({ name, fields, at: Date.now(), who }),
    }),
  };
}

/** Counts a connection's disconnects and reconnects (the emergence check of every cut test). */
function linkEvents(nc: NatsConnection): { disconnects: number; reconnects: number } {
  const seen = { disconnects: 0, reconnects: 0 };
  void (async () => {
    for await (const s of nc.status()) {
      if (s.type === "disconnect") seen.disconnects += 1;
      if (s.type === "reconnect") seen.reconnects += 1;
    }
  })();
  return seen;
}

async function waitUntil(
  cond: () => boolean | Promise<boolean>,
  timeoutMs: number,
  what: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await cond()) return;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await delay(25);
  }
}

/** Everything a test opened, closed in reverse order whatever happened in the test body. */
class Scope {
  readonly #closers: Array<() => Promise<unknown>> = [];
  defer(fn: () => Promise<unknown>): void {
    this.#closers.push(fn);
  }
  async proxy(target: string, rttMs = 0): Promise<NatsProxy> {
    const p = new NatsProxy(target, { rttMs });
    await p.start();
    this.defer(() => p.stop());
    return p;
  }
  /** A connection that reconnects fast and forever — the posture a worker should run with. */
  async conn(url: string, opts: { reconnect?: boolean } = {}): Promise<NatsConnection> {
    const nc = await connect({
      servers: url,
      reconnect: opts.reconnect ?? true,
      reconnectTimeWait: 100,
      reconnectJitter: 0,
      maxReconnectAttempts: -1,
    });
    this.defer(() => nc.close());
    return nc;
  }
  worker(w: Worker): Worker {
    this.defer(() => w.stop());
    return w;
  }
  async close(): Promise<void> {
    for (const fn of this.#closers.reverse()) {
      try {
        await fn();
      } catch {
        /* a closer failing must not hide the test's own failure */
      }
    }
    this.#closers.length = 0;
  }
}

async function journalOf(nc: NatsConnection, prefix: string, runId: string) {
  const j = new NatsJournal(jetstream(nc), await jetstreamManager(nc), names(prefix), jsonCodec);
  return (await j.loadRaw(runId)).map((l) => l.entry);
}

/** One terminal, and no command position journaled twice — whatever the network did. */
async function assertJournalSane(nc: NatsConnection, prefix: string, runId: string): Promise<void> {
  const entries = await journalOf(nc, prefix, runId);
  const terminals = entries.filter((e) =>
    ["result", "fail", "cancelled", "continued"].includes(e.kind),
  );
  expect(terminals, `terminals of ${runId}`).toHaveLength(1);
  const seen = new Map<string, number>();
  for (const e of entries) {
    if (
      ["step-begin", "step-end", "now", "random", "uuid", "sleep", "await", "call"].includes(e.kind)
    ) {
      const key = `${e.kind}:${String(e.seq)}`;
      seen.set(key, (seen.get(key) ?? 0) + 1);
    }
  }
  expect(
    [...seen].filter(([, n]) => n > 1),
    `duplicated positions in ${runId}`,
  ).toEqual([]);
}

ownLink("the proxy itself (the instrument)", () => {
  const scope = new Scope();
  afterEach(() => scope.close());

  it("adds the round-trip time it says it adds — and none when told none", async () => {
    const proxy = await scope.proxy(serverUrl!, 0);
    const nc = await scope.conn(proxy.url);
    const rtt = async (): Promise<number> => {
      const samples: number[] = [];
      for (let i = 0; i < 5; i++) samples.push(await nc.rtt());
      return samples.sort((a, b) => a - b)[2]!; // the median
    };
    expect(await rtt()).toBeLessThan(20); // the control: no delay configured, none added
    proxy.rttMs = 120;
    const slow = await rtt();
    expect(slow).toBeGreaterThanOrEqual(118);
    expect(slow).toBeLessThan(170);
  });

  it("keeps a stream in order when the delay drops mid-flight", async () => {
    const proxy = await scope.proxy(serverUrl!, 200);
    const pub = await scope.conn(proxy.url);
    const direct = await scope.conn(serverUrl!);
    const got: number[] = [];
    direct.subscribe("net.order", { callback: (_e, m) => void got.push(Number(m.string())) });
    await direct.flush();
    for (let i = 0; i < 50; i++) {
      pub.publish("net.order", String(i));
      if (i === 25) {
        // Flush so the first half is IN the delay queue, then remove the delay: the second half
        // would overtake it if the queue released by delay rather than by arrival order.
        await delay(20);
        proxy.rttMs = 0;
      }
    }
    await waitUntil(() => got.length === 50, 5_000, "all 50 messages");
    expect(got).toEqual(Array.from({ length: 50 }, (_, i) => i));
  });

  it("delivers what was written before a close (a final ack must not die in the delay queue)", async () => {
    const proxy = await scope.proxy(serverUrl!, 150);
    const direct = await scope.conn(serverUrl!);
    const got: string[] = [];
    direct.subscribe("net.lastwords", { callback: (_e, m) => void got.push(m.string()) });
    await direct.flush();
    // A raw socket, so "written, then closed" is exactly that: a client library's close() is free
    // to discard its own outbound buffer, which would test the library and not the proxy.
    const sock = net.connect(Number(new URL(proxy.url).port), "127.0.0.1");
    await new Promise<void>((r) => sock.once("connect", () => r()));
    sock.end('CONNECT {"verbose":false}\r\nPUB net.lastwords 3\r\nbye\r\n'); // FIN 75 ms before the bytes are due
    await waitUntil(() => got.length === 1, 3_000, "the message published just before close");
  });

  it("cut() resets connections; the client reconnects THROUGH the proxy, not around it", async () => {
    const proxy = await scope.proxy(serverUrl!, 0);
    const nc = await scope.conn(proxy.url);
    let reconnects = 0;
    void (async () => {
      for await (const s of nc.status()) if (s.type === "reconnect") reconnects += 1;
    })();
    expect(proxy.stats.accepted).toBe(1);
    expect(proxy.cut()).toBe(1);
    await waitUntil(() => reconnects === 1, 5_000, "the reconnect");
    // Had the client learned the server's real port from INFO, it would be back without this.
    expect(proxy.stats.accepted).toBe(2);
    expect(await nc.rtt()).toBeGreaterThanOrEqual(0);
  });

  it('stall "down": the request LANDS, the reply is withheld until the stall lifts', async () => {
    const proxy = await scope.proxy(serverUrl!, 0);
    const nc = await scope.conn(proxy.url);
    const direct = await scope.conn(serverUrl!);
    direct.subscribe("net.echo", { callback: (_e, m) => void m.respond("pong") });
    let landed = 0;
    direct.subscribe("net.echo", { callback: () => void (landed += 1) });
    await direct.flush();
    proxy.stall = "down";
    let replied = false;
    const req = nc.request("net.echo", "ping", { timeout: 5_000 }).then(() => (replied = true));
    await waitUntil(() => landed === 1, 2_000, "the request to land at the server");
    await delay(300);
    expect(replied).toBe(false);
    proxy.stall = "none";
    await req;
    expect(replied).toBe(true);
  });
});

ownLink("latency", () => {
  const scope = new Scope();
  afterEach(() => scope.close());

  it("a 5-step run costs a pinned number of DEPENDENT round trips — and the pin resolves one step", async () => {
    // The cost budget (cost-budget.test.ts) counts REQUESTS. It cannot see a request that became
    // SEQUENTIAL: at 0.1 ms a new dependent round trip costs nothing, at 50 ms it is the latency
    // of every run. So measure the only thing that shows it — wall time at a known RTT, minus the
    // same run at no RTT, in units of RTT.
    const RTT = 100;
    const prefix = "netrt";
    const proxy = await scope.proxy(serverUrl!, 0);
    const wnc = await scope.conn(proxy.url);
    const cnc = await scope.conn(proxy.url);
    const wf = defineWorkflow("net_steps", async (ctx: Ctx, input: { n: number }) => {
      let acc = 0;
      for (let i = 0; i < input.n; i++) acc += await ctx.run(`s${String(i)}`, () => i);
      return acc;
    });
    const worker = scope.worker(createWorker({ nc: wnc, workflows: [wf], prefix, ...SIZED }));
    await worker.start();
    const client = createClient({ nc: cnc, prefix, ...SIZED });
    const once = async (n: number): Promise<number> => {
      const t0 = performance.now();
      await (await client.start(wf, { n })).result(30_000);
      const ms = performance.now() - t0;
      await delay(250); // the worker's tail (ack, lease release) must not bleed into the next run
      return ms;
    };
    await once(5); // warm-up: provisioning, the first pull
    const median = async (): Promise<number> => {
      const samples: number[] = [];
      for (let i = 0; i < 5; i++) samples.push(await cnc.rtt());
      return samples.sort((a, b) => a - b)[2]!;
    };
    const roundTrips = async (n: number): Promise<number> => {
      proxy.rttMs = 0;
      const base = Math.min(await once(n), await once(n));
      const floor = await median();
      proxy.rttMs = RTT;
      await delay(RTT); // let the idle pull's in-flight bytes settle at the new delay
      // Divide by the round trip the proxy ACTUALLY adds, not the one it was asked for: timers
      // overshoot by a percent or three, and over ~19 hops that reads as half a round trip (the
      // Python twin's proxy did exactly that).
      const added = (await median()) - floor;
      const slow = Math.min(await once(n), await once(n));
      return (slow - base) / added;
    };
    const five = await roundTrips(5);
    const six = await roundTrips(6);
    console.log(`dependent round trips: 5-step run ${five.toFixed(2)}, 6-step run ${six.toFixed(2)}`); // prettier-ignore

    // The instrument resolves ONE step: a step is two dependent appends (begin, end). If this did
    // not hold, the pin below could not see a new sequential request either.
    expect(six - five).toBeGreaterThan(1.5);
    expect(six - five).toBeLessThan(2.5);
    // The pin, derived and then measured (19.0 ± 0.1 over repeated runs): start() is 3 (journal
    // start, status hint, work item — the item reaches the worker as its ack reaches the client);
    // the worker's load 1, lease 1, post-gate reload 1; ten step appends; the result 1; the status
    // finalize travelling on to the waiting client as its hint 1; the client's terminal read 1.
    expect(five).toBeLessThan(19.5);
  });

  it("a worker's start costs a pinned number of DEPENDENT round trips — and none per workflow name", async () => {
    // The fabric test repo's F25: 5 s per start against NGS from a laptop, 0.45 s on a local
    // server — every request of start() and provision() awaited after the one before it. Measured
    // on resources that already exist (a restart: the floor of every cold start), with one
    // workflow name and with four, so a per-name sequential request shows up as the difference.
    const RTT = 60;
    const prefix = "netstart";
    const flows = ["a", "b", "c", "d"].map((x) =>
      defineWorkflow(`net_start_${x}`, async (ctx: Ctx) => ctx.run("s", () => x)),
    );
    const proxy = await scope.proxy(serverUrl!, 0);
    const nc = await scope.conn(proxy.url);
    const once = async (k: number): Promise<number> => {
      const w = createWorker({ nc, workflows: flows.slice(0, k), prefix, ...SIZED });
      const t0 = performance.now();
      await w.start();
      const ms = performance.now() - t0;
      await w.stop();
      return ms;
    };
    await once(4); // provisions, and binds all four names' consumers
    const median = async (): Promise<number> => {
      const samples: number[] = [];
      for (let i = 0; i < 5; i++) samples.push(await nc.rtt());
      return samples.sort((a, b) => a - b)[2]!;
    };
    const base = { 1: Math.min(await once(1), await once(1)), 4: Math.min(await once(4), await once(4)) }; // prettier-ignore
    const floor = await median();
    proxy.rttMs = RTT;
    await delay(RTT);
    const added = (await median()) - floor;
    const one = (Math.min(await once(1), await once(1)) - base[1]) / added;
    const four = (Math.min(await once(4), await once(4)) - base[4]) / added;
    console.log(`dependent round trips: worker start, 1 name ${one.toFixed(2)}, 4 names ${four.toFixed(2)}`); // prettier-ignore

    // Names cost no round trip of their own: their consumers are read together. (Before: two
    // round trips per name — 34.3 at four names against 28.3 at one.)
    expect(four - one).toBeLessThan(1);
    // The pin, derived and then measured (4.2): provision's API check 1, every resource's
    // STREAM.INFO at once 1, the batched-direct-get preflight 1, every consumer's INFO at once 1.
    // Before: 28.3 — each of those one after another, the three buckets opened twice, and an
    // API check per open.
    expect(one).toBeLessThan(4.6);
  });

  it("a client's first start() costs a pinned number of DEPENDENT round trips", async () => {
    // A fresh client provisions, then starts a run. Its buckets used to be OPENED after the
    // provision that had just verified them — an API check plus a STREAM.INFO, and an API check of
    // the client's own manager besides (F25's follow-up). Measured on a prefix that exists.
    const RTT = 60;
    const prefix = "netclient";
    const wf = defineWorkflow("net_client_first", async (ctx: Ctx) => ctx.run("s", () => 1));
    const proxy = await scope.proxy(serverUrl!, 0);
    const nc = await scope.conn(proxy.url);
    await provision(nc, names(prefix), SIZED);
    let i = 0;
    const once = async (): Promise<number> => {
      const client = createClient({ nc, prefix, ...SIZED }); // fresh: nothing memoized
      const t0 = performance.now();
      await client.start(wf, {}, { runId: `first-${String(i++)}` });
      return performance.now() - t0;
    };
    const median = async (): Promise<number> => {
      const samples: number[] = [];
      for (let k = 0; k < 5; k++) samples.push(await nc.rtt());
      return samples.sort((a, b) => a - b)[2]!;
    };
    await once(); // warm-up
    const base = Math.min(await once(), await once());
    const floor = await median();
    proxy.rttMs = RTT;
    await delay(RTT);
    const added = (await median()) - floor;
    const first = (Math.min(await once(), await once()) - base) / added;
    console.log(`dependent round trips: a client's first start() ${first.toFixed(2)}`);
    // The pin, derived and then measured: provision's API check 1, every resource's STREAM.INFO at
    // once 1, the batched-direct-get preflight 1; then the start itself — the journal start entry
    // 1, the status hint 1, the work item 1. Before: 9.2 (the manager's API check, and the runs
    // bucket opened with an API check plus a STREAM.INFO).
    expect(first).toBeLessThan(6.5);
  });

  // durable-sdk: "an offloaded payload costs ONE dependent round trip per 128 KiB chunk" is dropped.
  // It timed the library's blob writer (NatsBlobStore) through the proxy; the blob writes are the
  // engine's now, next to the server, and the SDK sends every value inline in one request.

  // The lease is a logical deadline the holder re-writes every TTL/3 and a contender reads with a
  // plain get. At 0.1 ms neither the renewal's flight time nor the staleness of the contender's
  // read exists; at a WAN round trip both eat into the TTL. A live holder that is taken over is a
  // step running twice at once, so: RTT 500 ms against a 2 s TTL, a step three TTLs long, and a
  // second worker asking for the run the whole time.
  async function leaseUnderLatency(prefix: string, maxWorkMs: number | undefined) {
    const RTT = 500;
    const TTL = 2_000;
    const STEP_MS = 5_500;
    const proxy = await scope.proxy(serverUrl!, 0);
    const direct = await scope.conn(serverUrl!);
    const rec = recorder();
    let executions = 0;
    const wf = defineWorkflow("net_lease", (ctx: Ctx) =>
      ctx.run("slow", async () => {
        executions += 1;
        await delay(STEP_MS);
        return "done";
      }),
    );
    for (const who of ["A", "B"]) {
      const w = createWorker({
        nc: await scope.conn(proxy.url),
        workflows: [wf],
        prefix,
        ...SIZED,
        leaseTtlMs: TTL,
        ...(maxWorkMs !== undefined ? { maxWorkMs } : {}),
        metrics: rec.sink(who),
      });
      scope.worker(w);
      await w.start(); // provisioning at no added latency; the link slows down once it is serving
    }
    const client = createClient({ nc: direct, prefix, ...SIZED });
    proxy.rttMs = RTT;
    await delay(RTT);
    const handle = await client.start(wf, undefined, { runId: `${prefix}-run` });
    // Nudges: extra `start` items for the SAME run. They journal nothing (an inbound event would
    // fence the step's own append and re-run it for a reason that has nothing to do with the
    // lease) — each one just sends whichever worker receives it to the gate.
    const js = jetstream(direct);
    const n = names(prefix);
    void (async () => {
      for (let i = 0; i < 9; i++) {
        await delay(450);
        const item: WorkItem = { runId: handle.runId, kind: "start" };
        await js.publish(n.workSubject(wf.name), encodeJson(item), { msgID: `nudge-${String(i)}` });
      }
    })();
    const result = await handle.result(60_000);
    proxy.rttMs = 0; // the nudges still queued re-drive the terminal: no need to do that at 500 ms
    const gates = rec.events.filter((e) => e.name === "lease.gate");
    const acquired = gates.find((e) => e.fields.outcome === "acquired");
    return { result, executions: () => executions, gates, acquired, TTL };
  }

  // durable-sdk: dropped — "LEASE at RTT 500 ms / TTL 2 s: a live holder is never taken over".
  // Every assertion read the worker's `lease.gate` events; the gating run lease is the engine's,
  // held next to the server (no RTT between them), and a run's items wait in the engine while one
  // is held, so the second worker is never even handed the run: no SDK event exists to observe.
  // The test is kept below, skipped, for the diff.
  it.skip("LEASE at RTT 500 ms / TTL 2 s: a live holder is never taken over", async () => {
    const r = await leaseUnderLatency("netls1", undefined);
    expect(r.result).toBe("done");
    expect(r.acquired).toBeDefined();
    // Emergence: the OTHER worker asked for the run after the original deadline had passed and
    // was still refused — without that, nothing here ever tested a renewal.
    const lateRefusals = r.gates.filter(
      (e) =>
        e.fields.outcome === "held" &&
        e.who !== r.acquired!.who &&
        e.at > r.acquired!.at + r.TTL + 250,
    );
    expect(lateRefusals.length, JSON.stringify(r.gates.map((g) => [g.who, g.fields.outcome]))).toBeGreaterThan(0); // prettier-ignore
    expect(r.gates.filter((e) => e.fields.outcome === "takeover")).toEqual([]);
    expect(r.executions()).toBe(1);
  });

  // durable-sdk: docs/findings.md F9, answered by the engine's F18 (past maxWorkMs the lease is
  // lapsed, not gone). The takeover is the engine's to see now: its log, `run taken over past max
  // work` (or `run lease taken over`, across instances), stands for the `lease.gate` event.
  it("…and the control: the same run with renewal cut short IS taken over, and the step runs twice", async () => {
    // `maxWorkMs` is the public knob that stops a holder's renewal (the hung-step escape hatch):
    // at 300 ms the lease is written once and never again, so its 2 s deadline simply passes.
    const r = await leaseUnderLatency("netls2", 300);
    expect(r.result).toBe("done");
    expect((await engineLog("netls2")).some((l) => l.includes("taken over"))).toBe(true);
    expect(r.executions()).toBeGreaterThanOrEqual(2);
  });
});

ownLink("disconnects", () => {
  const scope = new Scope();
  afterEach(() => scope.close());

  /** s1 → a slow s2 that reports when it starts → s3; every effect is counted. */
  function threeSteps(name: string, slowMs: number) {
    const ran = { s1: 0, s2: 0, s3: 0 };
    let entered!: () => void;
    const inS2 = new Promise<void>((r) => (entered = r));
    const wf = defineWorkflow(name, async (ctx: Ctx) => {
      await ctx.run("s1", () => void (ran.s1 += 1));
      await ctx.run("s2", async () => {
        ran.s2 += 1;
        entered();
        await delay(slowMs);
      });
      await ctx.run("s3", () => void (ran.s3 += 1));
      return "ok";
    });
    return { wf, ran, inS2 };
  }

  it("a connection reset MID-STEP: the run completes after the reconnect, every effect exactly once", async () => {
    const prefix = "netcut1";
    const proxy = await scope.proxy(serverUrl!, 20);
    const direct = await scope.conn(serverUrl!);
    const wnc = await scope.conn(proxy.url);
    const link = linkEvents(wnc);
    const { wf, ran, inS2 } = threeSteps("net_cut", 1_500);
    const worker = scope.worker(createWorker({ nc: wnc, workflows: [wf], prefix, ...SIZED }));
    await worker.start();
    const client = createClient({ nc: direct, prefix, ...SIZED });
    const handle = await client.start(wf, undefined);
    await inS2;
    proxy.down = true; // an outage, not a blip: the first reconnect attempts fail too
    expect(proxy.cut()).toBe(1);
    await delay(600);
    proxy.down = false;
    expect(await handle.result(20_000)).toBe("ok");
    expect(link).toEqual({ disconnects: 1, reconnects: 1 }); // it happened, and once
    expect(ran).toEqual({ s1: 1, s2: 1, s3: 1 });
    await assertJournalSane(direct, prefix, handle.runId);
  });

  it("…and the control: with reconnect off the same cut STRANDS the run until another worker takes it", async () => {
    const prefix = "netcut2";
    const proxy = await scope.proxy(serverUrl!, 20);
    const direct = await scope.conn(serverUrl!);
    const wnc = await scope.conn(proxy.url, { reconnect: false });
    const { wf, ran, inS2 } = threeSteps("net_cut", 1_000);
    const opts = { workflows: [wf], prefix, ...SIZED, ackWaitMs: 2_000, leaseTtlMs: 2_000 };
    const worker = scope.worker(createWorker({ nc: wnc, ...opts }));
    await worker.start();
    const client = createClient({ nc: direct, prefix, ...SIZED });
    const handle = await client.start(wf, undefined);
    await inS2;
    proxy.cut();
    await delay(1_800); // s2 has long finished on the dead worker; its step-end has nowhere to go
    expect(wnc.isClosed()).toBe(true);
    const stranded = await journalOf(direct, prefix, handle.runId);
    expect(stranded.map((e) => e.kind)).toEqual(["start", "step-begin", "step-end", "step-begin"]);
    // What the reconnect bought above is visible in what recovery costs without it: the item
    // redelivers to a fresh worker after AckWait, and s2 — begun, never ended — runs AGAIN.
    const rescue = scope.worker(createWorker({ nc: await scope.conn(serverUrl!), ...opts }));
    await rescue.start();
    expect(await handle.result(30_000)).toBe("ok");
    expect(ran).toEqual({ s1: 1, s2: 2, s3: 1 });
    await assertJournalSane(direct, prefix, handle.runId);
  });

  it("a LOST ACK: the append landed, its reply did not — one entry, one effect, the run completes", async () => {
    // The ambiguous failure: the worker cannot know whether its step-end was written. It must not
    // assume either way — the redelivery replays from the journal, which is where the answer is.
    const prefix = "netack";
    const proxy = await scope.proxy(serverUrl!, 0);
    const direct = await scope.conn(serverUrl!);
    const rec = recorder();
    let s2 = 0;
    const wf = defineWorkflow("net_ack", async (ctx: Ctx) => {
      await ctx.run("s1", () => 1);
      await ctx.run("s2", () => {
        s2 += 1;
        if (s2 === 1) proxy.stall = "down"; // from here on requests land and replies do not
        return 2;
      });
      return ctx.run("s3", () => "ok");
    });
    const worker = scope.worker(createWorker({ nc: await scope.conn(proxy.url), workflows: [wf], prefix, ...SIZED, metrics: rec.sink("W") })); // prettier-ignore
    await worker.start();
    const client = createClient({ nc: direct, prefix, ...SIZED });
    const handle = await client.start(wf, undefined);
    const landed = async (): Promise<boolean> =>
      (await journalOf(direct, prefix, handle.runId)).some((e) => e.kind === "step-end" && e.seq === 1); // prettier-ignore
    await waitUntil(landed, 10_000, "s2's step-end to land while its ack is withheld");
    proxy.cut(); // the withheld ack dies with the connection
    proxy.stall = "none";
    expect(await handle.result(30_000)).toBe("ok");
    // Emergence: the worker really did see that append FAIL, and really did retry the item.
    const lost = rec.events.filter((e) => e.name === "journal.append" && e.fields.kind === "step-end" && e.fields.seq === 1); // prettier-ignore
    expect(lost.map((e) => e.fields.outcome)).toEqual(["error"]);
    expect(rec.events.some((e) => e.name === "work.done" && e.fields.outcome === "retry")).toBe(true); // prettier-ignore
    expect(s2).toBe(1);
    await assertJournalSane(direct, prefix, handle.runId);
  });

  it("a result() waiter rides out a cut — and resolves even though its wake-up call was lost", async () => {
    const prefix = "netres1";
    const proxy = await scope.proxy(serverUrl!, 20);
    const direct = await scope.conn(serverUrl!);
    const cnc = await scope.conn(proxy.url);
    const link = linkEvents(cnc);
    const wf = defineWorkflow("net_res", async (ctx: Ctx) => {
      await ctx.await("go");
      return "woken";
    });
    const worker = scope.worker(createWorker({ nc: direct, workflows: [wf], prefix, ...SIZED }));
    await worker.start();
    const client = createClient({ nc: cnc, prefix, ...SIZED });
    const handle = await client.start(wf, undefined);
    // The waiter's own view of its wake-up subject, on the same connection.
    let hints = 0;
    cnc.subscribe(names(prefix).runsHintSubject(handle.runId), { callback: () => void (hints += 1) }); // prettier-ignore
    await cnc.flush();
    const waiting = handle.result(20_000);
    await delay(300);
    const hintsBefore = hints;
    proxy.down = true;
    proxy.cut();
    // The run finishes while the waiter is cut off: the status hint is published to nobody.
    await createClient({ nc: direct, prefix, ...SIZED }).signal(handle.runId, "go", {});
    await waitUntil(
      async () => (await journalOf(direct, prefix, handle.runId)).some((e) => e.kind === "result"),
      10_000,
      "the run to finish during the outage",
    );
    proxy.down = false;
    expect(await waiting).toBe("woken");
    expect(link.reconnects).toBe(1);
    expect(hints).toBe(hintsBefore); // no hint ever reached this connection: the TICK found it
  });

  it("a result() waiter on a connection that CLOSES fails at once, by name — and never starves the event loop", async () => {
    // Before the fix this spun: the ended subscription's next() settles immediately, forever, so
    // the wait loop ran on microtasks alone — no timer fired in the whole process until the
    // timeout — and then reported a timeout the run had nothing to do with.
    const prefix = "netres2";
    const proxy = await scope.proxy(serverUrl!, 0);
    const cnc = await scope.conn(proxy.url, { reconnect: false });
    const wf = defineWorkflow("net_res_closed", async (ctx: Ctx) => {
      await ctx.await("never");
      return 1;
    });
    const client = createClient({ nc: cnc, prefix, ...SIZED });
    const handle = await client.start(wf, undefined);
    let ticks = 0;
    const timer = setInterval(() => (ticks += 1), 20);
    scope.defer(() => Promise.resolve(clearInterval(timer)));
    const t0 = Date.now();
    const waiting = handle.result(8_000).then(
      () => "resolved",
      (e: unknown) => e,
    );
    await delay(300);
    proxy.cut();
    const outcome = await waiting;
    expect(outcome).toBeInstanceOf(ConnectionClosedError);
    expect(Date.now() - t0).toBeLessThan(2_000); // at once, not at the timeout
    const before = ticks;
    await delay(200);
    expect(ticks - before).toBeGreaterThan(5); // the loop is alive
  });
});

ownLink("a connection that closes for good", () => {
  const scope = new Scope();
  afterEach(() => scope.close());

  it("a worker says so — health() stops claiming to run, one error names it, and it stops 'recreating'", async () => {
    // The library does not own the connection and cannot reopen it. Before this, such a worker
    // reported `running: true` forever and logged "consumer pump failed; recreating" with backoff
    // for the rest of its life — which it never could — so a readiness probe kept a permanently
    // deaf process in service. (`@nats-io` closes a connection after 10 failed reconnects by
    // default: a 20-second outage is enough.)
    const prefix = "netdeaf";
    const proxy = await scope.proxy(serverUrl!, 0);
    const wnc = await scope.conn(proxy.url, { reconnect: false });
    const errors: string[] = [];
    const logger = {
      debug: () => {},
      info: () => {},
      warn: () => {},
      error: (m: string) => void errors.push(m),
    };
    const wf = defineWorkflow("net_deaf", () => Promise.resolve(1));
    const worker = scope.worker(createWorker({ nc: wnc, workflows: [wf], prefix, ...SIZED, logger })); // prettier-ignore
    await worker.start();
    const serving = (): boolean => {
      const h = worker.health();
      return h.running && !h.connectionClosed && h.pumps.every((p) => p.alive);
    };
    await waitUntil(serving, 10_000, "both pumps to be attached");
    proxy.cut();
    await waitUntil(() => worker.health().connectionClosed && errors.length >= 1, 10_000, "the worker to notice"); // prettier-ignore
    await delay(2_500); // long enough for the old loop to have "recreated" twice more
    const after = worker.health();
    expect(after.running).toBe(false);
    // durable-sdk: assertion touched, `toEqual([false, false])` → every entry false. The worker's
    // `pumps` are its poll slots (16 by default), not a work and a timers consumer per name.
    expect(after.pumps.map((p) => p.alive).every((a) => !a)).toBe(true);
    // Once, by name — and no "consumer pump failed; recreating", which is what it used to say,
    // at error level, with backoff, forever.
    expect(errors).toHaveLength(1);
    expect(errors[0]).toContain("CLOSED for good");
  });
});

ownServer("a broker crash mid-storm", () => {
  const scope = new Scope();
  afterEach(() => scope.close());

  // prettier-ignore
  it("SIGKILL + restart under a two-worker signal storm: every run completes, every journal is sane", async () => {
    const prefix = "netboom";
    const server = new NatsServerProcess();
    await server.start();
    scope.defer(() => server.stop());
    setEngineServer(server.url); // durable-sdk: this test's engine serves this test's own server
    const RUNS = 6;
    const ran = new Map<string, number>();
    const wf = defineWorkflow("net_storm", async (ctx: Ctx, input: { id: string }) => {
      await ctx.await("go");
      const bump = (step: string) => async () => {
        const k = `${input.id}:${step}`;
        ran.set(k, (ran.get(k) ?? 0) + 1);
        await delay(300); // long enough that the crash can land INSIDE a step
        return step;
      };
      await ctx.run("s1", bump("s1"));
      await ctx.sleep("nap", 1_000); // a native schedule that must survive the restart
      await ctx.run("s2", bump("s2"));
      return ctx.run("s3", bump("s3"));
    });
    const rec = recorder();
    const links = [];
    for (const who of ["A", "B"]) {
      const nc = await scope.conn(server.url);
      links.push(linkEvents(nc));
      const w = scope.worker(createWorker({ nc, workflows: [wf], prefix, ...SIZED, metrics: rec.sink(who) })); // prettier-ignore
      await w.start();
    }
    const cnc = await scope.conn(server.url);
    const client = createClient({ nc: cnc, prefix, ...SIZED });
    const handles = await Promise.all(
      Array.from({ length: RUNS }, (_, i) => client.start(wf, { id: `r${String(i)}` }, { runId: `boom-${String(i)}` })), // prettier-ignore
    );
    const results = handles.map((h) => h.result(90_000));
    await Promise.all(
      handles.flatMap((h) => [1, 2, 3].map(() => client.signal(h.runId, "go", {}))),
    );
    // Crash at a moment worth crashing at: some run is INSIDE a step (begin journaled, end not).
    const observer = await scope.conn(server.url);
    const openSteps = async (): Promise<number> => {
      let open = 0;
      for (const h of handles) {
        const entries = await journalOf(observer, prefix, h.runId);
        const begun = entries.filter((e) => e.kind === "step-begin").length;
        open += begun - entries.filter((e) => e.kind === "step-end").length;
      }
      return open;
    };
    let openAtCrash = 0;
    await waitUntil(async () => (openAtCrash = await openSteps()) > 0, 10_000, "a run mid-step");
    const terminalsAt = async (): Promise<number> => {
      let n = 0;
      for (const h of handles) if ((await journalOf(observer, prefix, h.runId)).some((e) => e.kind === "result")) n += 1; // prettier-ignore
      return n;
    };
    const doneBefore = await terminalsAt();
    await server.restart({ kill: true, downMs: 500 });
    expect(await Promise.all(results)).toEqual(Array.from({ length: RUNS }, () => "s3"));

    const tally = (name: DurableEventName, field: string): Record<string, number> => {
      const out: Record<string, number> = {};
      for (const e of rec.events) if (e.name === name) out[String(e.fields[field])] = (out[String(e.fields[field])] ?? 0) + 1; // prettier-ignore
      return out;
    };
    const repeats = [...ran.values()].filter((v) => v > 1).length;
    console.log(`broker crash: ${String(openAtCrash)} step(s) open at the kill · work.done`, tally("work.done", "outcome"), "· appends", tally("journal.append", "outcome"), `· pump restarts ${String(rec.events.filter((e) => e.name === "pump.restart").length)} · effects repeated ${String(repeats)}`); // prettier-ignore

    // Emergence: the crash interrupted work in progress, and both workers lived through it.
    expect(openAtCrash).toBeGreaterThan(0);
    expect(doneBefore).toBe(0);
    for (const l of links) expect(l.reconnects).toBeGreaterThanOrEqual(1);
    // State is exactly-once whatever the crash interleaved; an EFFECT may repeat across a crash
    // (begin journaled, end not — the documented at-least-once window), never go missing.
    for (const h of handles) await assertJournalSane(observer, prefix, h.runId);
    for (let i = 0; i < RUNS; i++) {
      for (const step of ["s1", "s2", "s3"]) expect(ran.get(`r${String(i)}:${step}`) ?? 0).toBeGreaterThanOrEqual(1); // prettier-ignore
    }
    await assertNoEphemeralConsumers(server.url);
  }, 120_000 * WAN); // durable-sdk: × the round-trip factor (harness/mode.ts)
});
