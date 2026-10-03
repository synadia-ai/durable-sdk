// A NATS-transparent TCP proxy for the two network axes no test had ever exercised: LATENCY and
// DISCONNECTS (docs/plans/test-axes.md, increment 3). Every suite had run against a server ~0.1 ms
// away over a link that never dropped, so every timing constant (lease renewal at TTL/3, the
// working() heartbeat, the result() tick) and every reconnect path was only ever ARGUED.
//
// It forwards bytes and knows nothing about the protocol, which is the point — the client library
// and the server see a real socket with real semantics:
//   - `rttMs`   each direction is held for rtt/2 in an ORDERED queue (a release time never precedes
//               the one before it, so lowering the delay mid-flight cannot reorder a stream);
//   - `cut()`   destroys every socket pair NOW, dropping whatever is queued — a connection reset;
//   - `down`    while true a new connection is accepted and destroyed at once — an outage that
//               outlasts the client's first reconnect attempts;
//   - `stall`   bytes are held, sockets stay open — the half-open link a WAN actually gives you,
//               which a client only notices by missing PINGs. `"down"` stalls server→client only:
//               requests LAND and their replies never arrive (the lost-ack case).
//
// A single nats-server sends no `connect_urls`, so a client cannot learn the real port from INFO
// and reconnect around the proxy. A clustered harness must add `no_advertise: true`.
//
// python/tests/netproxy.py is the twin (a subprocess there: a proxy sharing the event loop of the
// code under test would not be an independent link).

import net from "node:net";

export type Stall = "none" | "both" | "down";

export interface ProxyStats {
  /** Connections accepted since start (a reconnect shows up here). */
  accepted: number;
  /** Socket pairs open right now. */
  open: number;
  /** Connections destroyed by cut() or refused while down. */
  dropped: number;
}

interface Chunk {
  at: number;
  data: Buffer | null; // null = the source ended: end the destination once the queue drains
}

/** One direction of one connection: an ordered, delayed byte queue. */
class Pipe {
  private readonly queue: Chunk[] = [];
  private timer: ReturnType<typeof setTimeout> | undefined;
  private lastAt = 0;
  private dead = false;

  constructor(
    private readonly dst: net.Socket,
    private readonly delayMs: () => number,
    private readonly stalled: () => boolean,
  ) {}

  push(data: Buffer | null): void {
    if (this.dead) return;
    const at = Math.max(this.lastAt, Date.now() + this.delayMs());
    this.lastAt = at;
    this.queue.push({ at, data });
    this.schedule();
  }

  /** Re-evaluate the queue (called when a stall lifts). */
  schedule(): void {
    if (this.dead || this.timer !== undefined) return;
    const head = this.queue[0];
    if (head === undefined) return;
    this.timer = setTimeout(
      () => {
        this.timer = undefined;
        this.drain();
      },
      Math.max(0, head.at - Date.now()),
    );
  }

  private drain(): void {
    if (this.dead) return;
    if (this.stalled()) return; // held, not dropped: schedule() runs again when the stall lifts
    const now = Date.now();
    for (let head = this.queue[0]; head !== undefined && head.at <= now; head = this.queue[0]) {
      this.queue.shift();
      if (head.data === null) this.dst.end();
      else this.dst.write(head.data);
    }
    this.schedule();
  }

  kill(): void {
    this.dead = true;
    this.queue.length = 0;
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = undefined;
  }
}

interface Pair {
  client: net.Socket;
  upstream: net.Socket;
  up: Pipe;
  down: Pipe;
}

export class NatsProxy {
  #server: net.Server | undefined;
  #port = 0;
  readonly #pairs = new Set<Pair>();
  readonly #target: { host: string; port: number };
  readonly #stats = { accepted: 0, dropped: 0 };
  #stall: Stall = "none";

  /** Round-trip time added by the proxy, in ms (half per direction). Adjustable at runtime. */
  rttMs: number;
  /** While true, a new connection is dropped at once (an outage, not a blip). */
  down = false;

  constructor(targetUrl: string, opts: { rttMs?: number } = {}) {
    const u = new URL(targetUrl);
    this.#target = { host: u.hostname, port: Number(u.port) };
    this.rttMs = opts.rttMs ?? 0;
  }

  get url(): string {
    return `nats://127.0.0.1:${String(this.#port)}`;
  }

  get stats(): ProxyStats {
    return { ...this.#stats, open: this.#pairs.size };
  }

  get stall(): Stall {
    return this.#stall;
  }

  /** Hold bytes without closing anything; lifting the stall releases what was held, in order. */
  set stall(mode: Stall) {
    this.#stall = mode;
    if (mode !== "both") for (const p of this.#pairs) p.up.schedule();
    if (mode === "none") for (const p of this.#pairs) p.down.schedule();
  }

  start(): Promise<void> {
    return new Promise((resolve, reject) => {
      // Half-open on purpose: a client that writes and then closes must have those bytes DELIVERED
      // (after the delay) before the upstream sees the FIN, exactly as on a real link. Without it
      // the socket's `close` lands first and the queue — a final ack, say — is thrown away.
      const server = net.createServer({ allowHalfOpen: true }, (client) => this.#accept(client));
      server.on("error", reject);
      server.listen(0, "127.0.0.1", () => {
        const addr = server.address();
        this.#port = typeof addr === "object" && addr ? addr.port : 0;
        this.#server = server;
        resolve();
      });
    });
  }

  #accept(client: net.Socket): void {
    this.#stats.accepted += 1;
    if (this.down) {
      this.#stats.dropped += 1;
      client.destroy();
      return;
    }
    client.setNoDelay(true);
    const upstream = net.connect({
      port: this.#target.port,
      host: this.#target.host,
      allowHalfOpen: true,
    });
    upstream.setNoDelay(true);
    const half = (): number => this.rttMs / 2;
    const pair: Pair = {
      client,
      upstream,
      up: new Pipe(upstream, half, () => this.#stall === "both"),
      down: new Pipe(client, half, () => this.#stall !== "none"),
    };
    this.#pairs.add(pair);
    client.on("data", (b: Buffer) => pair.up.push(b));
    upstream.on("data", (b: Buffer) => pair.down.push(b));
    client.on("end", () => pair.up.push(null));
    upstream.on("end", () => pair.down.push(null));
    const teardown = (): void => this.#destroy(pair);
    client.on("error", teardown);
    upstream.on("error", teardown);
    client.on("close", teardown);
    upstream.on("close", teardown);
  }

  #destroy(pair: Pair): void {
    if (!this.#pairs.delete(pair)) return;
    pair.up.kill();
    pair.down.kill();
    pair.client.destroy();
    pair.upstream.destroy();
  }

  /** Reset every connection NOW; queued bytes are lost. Returns how many were open. */
  cut(): number {
    const open = [...this.#pairs];
    for (const p of open) this.#destroy(p);
    this.#stats.dropped += open.length;
    return open.length;
  }

  async stop(): Promise<void> {
    this.cut();
    const server = this.#server;
    this.#server = undefined;
    if (server) await new Promise<void>((r) => server.close(() => r()));
  }
}
