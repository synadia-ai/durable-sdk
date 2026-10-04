# durable-sdk

The thin durable execution SDKs, TypeScript and Python. Each keeps the replay core of the library `@synadia-ai/durable` (`synadia-ai/durable-ts`, frozen at `7456c75`): workflows as ordinary functions over a `ctx`, every side-effecting or nondeterministic step journaled and replayed after a crash, the determinism guards, the out-of-band halts. Behind the core's three seams (`Journal`, `Scheduler`, `Children`) and the client's five calls sits a request/reply client for [`durable-engine`](https://github.com/synadia-ai/durable-engine), which owns the streams. A customer's workflow functions run in the customer's process; nothing runs without the engine.

Private until published. Created 2026-10-03 on the Synadia Agent Fabric's decision 61 (`synadia-agent-fabric-docs`, `DECISIONS.md`; the research `docs/research/2026-10-03--durable-execution-as-a-product.md`).

- **The public API** is the library's: `defineWorkflow`, `createClient`, `createWorker`, `ctx.run/sleep/await/call/now/random/uuid/continueAsNew/onCancel`. The library's integration suites are this repo's acceptance suites, run against the engine.
- **The only dependency** is `@nats-io/nats-core`: every call is one request to `{prefix}.api.v1.<op>` (`../durable-engine/docs/API.md`). There are no consumers, streams or buckets here, and no JetStream client.
- **Documentation** is this README, the [examples](examples/), [`docs/findings.md`](docs/findings.md) (what building it found) and [`docs/ORIGIN.md`](docs/ORIGIN.md) (which file came from where), kept minimal on purpose. The protocol stays in the frozen library (`../synadia-durable-execution/docs/PROTOCOL.md`).

**Status:** the TypeScript SDK, `0.7.0-alpha` (board row P2-19a), and the Python SDK in [`python/`](python/), `0.7.0a0` (P2-19b), with its own [README](python/README.md).

## Quickstart

A local NATS, the engine next to it, and the `charge` example:

```sh
nats-server -js &
durable-engine serve --nats nats://127.0.0.1:4222 &      # built from ../durable-engine at engine.pin
bun install
NATS_URL=nats://127.0.0.1:4222 npm run example:charge
```

To build the engine: `cd ../durable-engine && git checkout $(cat ../durable-sdk/engine.pin) && go build -o durable-engine ./cmd/durable-engine`. The engine provisions its seven resources at start and answers on `de.api.v1.*`. Check it with `nats req de.api.v1.info '{}'`.

```ts
import { connect } from "@nats-io/transport-node";
import { createClient, createWorker, defineWorkflow } from "@synadia-ai/durable";

const order = defineWorkflow("order", async (ctx, cart: { cartId: string }) => {
  const orderId = await ctx.run("reserve", () => `ord-${cart.cartId}`); // journaled, never re-run
  const payment = await ctx.await<{ approved: boolean }>("payment"); // parks until a signal
  if (!payment.approved) return { status: "abandoned", orderId };
  await ctx.sleep("settle", 500); // a durable timer
  return { status: "paid", orderId };
});

const nc = await connect({ servers: "nats://127.0.0.1:4222" });
const worker = createWorker({ nc, workflows: [order] });
await worker.start();

const client = createClient({ nc });
const handle = await client.start(order, { cartId: "c-42" });
await client.signal(handle.runId, "payment", { approved: true });
console.log(await handle.result(20_000));
```

## The examples

Each runs with `npm run example:<name>` against a local `nats-server -js` and a running `durable-engine serve`. See [examples/README.md](examples/README.md).

| Example        | Shows                                                                                                                |
| -------------- | -------------------------------------------------------------------------------------------------------------------- |
| `charge`       | `run`, `await`, `sleep`, `call`; `start`, `signal`, `result`, `status`                                               |
| `saga`         | compensations in reverse on failure                                                                                  |
| `crash-replay` | a worker killed mid-run; a fresh one resumes without re-running finished steps (run the engine with `--ack-wait 2s`) |
| `fan-out`      | `ctx.startCall` + `Promise.all`, and `ctx.race`                                                                      |
| `cancel`       | `client.cancel` and `ctx.onCancel`                                                                                   |

## What moved to the engine

The worker is the API's loop: one `poll` per concurrency slot; for each item the journal (paged with `load`), the library's `executeRun` with its seams as `append`, `wake` and `child` under the item's lease, `extend` every third of the ack wait, then `done`; every poll names the worker, and `stop()` withdraws its waiting polls (`poll {cancelPoll}`), so a stop takes as long as the items in flight. A value too large for one request goes up in `put` parts and travels as a ref; a ref the engine hands back, for a value too large for a reply, comes down in `get` parts, its digest checked (`src/engine/blobs.ts`, through the library's own offload code). Everything else the library's worker did is the engine's: the consumers, provisioning, the inbound event, the run lease, the ack matrix, `child-done`, the cascade of a cancel, the forward across continue-as-new, the parked index and the lost-wake repair, the maintenance pass, offload.

So `createWorker` takes `nc`, `workflows`, `prefix`, `dependencies`, `concurrency` (default 16), `pollWaitMs` (default 30 000), `logger`, `metrics` and `payloadCodec`. The library's other options are accepted and ignored, each named for the `serve` flag it became: `ackWaitMs` → `--ack-wait`, `maxWorkMs` → `--max-work`, `lease: false` → `--no-lease`, `leaseTtlMs` → `--lease-ttl`, `streamMaxBytes` → `--stream-max-bytes`, `maintenanceIntervalMs` → `--maintenance-interval`, `reconcileGraceMs` → `--reconcile-grace`, and `offload`'s threshold → `--offload-threshold`; `offload: false` keeps its meaning (nothing goes up in parts, a value too large for a request fails loud). `createClient` takes `nc`, `prefix`, `payloadCodec`, `logger`, `metrics` (`blob.put`, `blob.get`) and `offload: false`.

A refusal from the engine is a typed error: `fenced` → `FencedError` (with `expectLastSeq` and `serverCode`, for logs), `expired` → `LeaseExpiredError`, `invalid` → `InvalidRequestError` (or `SerializationError` inside an execution), `too_large` → `PayloadTooLargeError`, `not_found` → `RunNotFoundError`, `unavailable` / `internal` / no responders / a timeout → `EngineUnavailableError`, a closed connection → `ConnectionClosedError`.

**Sizes:** a value of any size the blob store holds; one larger than a request (`request_max`, about `max_payload` − 16 KiB) costs a `put` round trip per part of about 6 MiB on an 8 MiB server (`docs/findings.md` F1, F26). After `result()`, `status()` can read the previous state for a round trip of the worker's (F27).

## Testing

```sh
bun install && npm test            # the 13 unit suites of the pure core (no server)
npm run test:integration           # 9 of the library's integration suites, interop-py and metrics, on durable-engine serve
```

The integration suites spawn a `nats-server` (≥ 2.14, from `PATH`) under the library's account-limits profile (`DE_TEST_PROFILE=ngs|tight|none`, default `ngs`), and one `durable-engine serve` per prefix a test uses, stopped after each test. The engine binary is `DE_ENGINE` (a path), or is built with `go build -o .engine-bin/…` from `../durable-engine` (`DE_ENGINE_REPO`) at the commit in `engine.pin`. A checkout at another commit, or with local changes, is refused unless `DE_ENGINE_ANY=1`. Without the binary or Go the suites skip, saying why. `DE_ENGINE_LOG=1` echoes the engines' logs, and `DE_SDK_LOG=1` gives a worker without a logger a console one. `DE_TEST_RTT_MS=<ms>` puts the library's delay proxy between every test's connection and the server (the engines stay on the direct URL); `docs/findings.md` F28 has the runs at 150 and 300 ms. Nothing reaches beyond 127.0.0.1.

`npm run check` adds the typecheck, lint and formatting.

## The interop worker

`conformance/interop/ts-worker.ts` is the library's, on the thin worker: `greet`, `approve`, `boom` and the parked `echo_signal`, with `DE_PAYLOAD_KEY` (hex) for encryption at rest. It needs a `durable-engine serve` on the same prefix:

```sh
NATS_URL=nats://127.0.0.1:4222 DE_PREFIX=de npx tsx conformance/interop/ts-worker.ts   # prints READY
```

The Python suites drive it (`python/tests/integration/test_interop.py`), and `test/integration/interop-py.test.ts` drives the Python interop worker (`python/conformance/worker.py`) with the TypeScript client, each with and without `DE_PAYLOAD_KEY`. `conformance/interop/ts-client.ts` is the library's TypeScript client for the Python reverse suite.
