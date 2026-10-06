# @synadia-ai/durable

Durable execution for TypeScript on NATS: workflows as ordinary functions over a `ctx`, every side-effecting or nondeterministic step journaled and replayed after a crash, with determinism guards and out-of-band halts. Your workflow functions run in your process; the journal, the timers and the work queue are [`durable-engine`](https://github.com/synadia-ai/durable-engine)'s, which this SDK reaches by request/reply. **It needs the engine:** nothing runs without one ([Which engine](#which-engine)).

The SDK keeps the replay core of the library `@synadia-ai/durable` (`synadia-ai/durable-ts`, frozen at `7456c75`). Behind the core's three seams (`Journal`, `Scheduler`, `Children`) and the client's five calls sits the engine's client.

**It runs in two places, with the same code:**

| Where                           | The engine                                                        | Start here                                                                             |
| ------------------------------- | ----------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| **standalone**                  | yours, next to your NATS, with a licence                          | the [Quickstart](#quickstart) below                                                    |
| **on the Synadia Agent Fabric** | ours, in the DE account; your tenant reaches it through an import | [`docs/FABRIC.md`](https://github.com/synadia-ai/durable-sdk/blob/main/docs/FABRIC.md) |

- **The public API** is the library's: `defineWorkflow`, `createClient`, `createWorker`, `ctx.run/sleep/await/call/startCall/race/now/random/uuid/key/continueAsNew/onCancel`. The library's integration suites are this repo's acceptance suites, run against the engine.
- **The only dependency** is `@nats-io/nats-core`: every call is one request to `{prefix}.api.v1.<op>` (the engine's [`docs/API.md`](https://github.com/synadia-ai/durable-engine/blob/main/docs/API.md)). There are no consumers, streams or buckets here, and no JetStream client.
- **Documentation** is this README, [`docs/FABRIC.md`](https://github.com/synadia-ai/durable-sdk/blob/main/docs/FABRIC.md) (on the fabric), the [examples](https://github.com/synadia-ai/durable-sdk/tree/main/examples), [`docs/findings.md`](https://github.com/synadia-ai/durable-sdk/blob/main/docs/findings.md) (what building it found) and [`docs/ORIGIN.md`](https://github.com/synadia-ai/durable-sdk/blob/main/docs/ORIGIN.md) (which file came from where), kept minimal on purpose. The engine's operator has the engine's `docs/QUICKSTART.md` and `docs/OPERATIONS.md`; the protocol stays in the frozen library's [`docs/PROTOCOL.md`](https://github.com/synadia-ai/durable-ts/blob/main/docs/PROTOCOL.md).

**Status:** the TypeScript SDK, `0.1.0`, and the Python SDK in [`python/`](https://github.com/synadia-ai/durable-sdk/tree/main/python), `0.7.0a0`, with its own [README](https://github.com/synadia-ai/durable-sdk/blob/main/python/README.md).

## Quickstart

For those who have the engine's repo: it builds the engine from a checkout (without it, [Which engine](#which-engine) says how you get one). A local NATS, the engine next to it with a test licence, and the `charge` example. You need `nats-server` ≥ 2.14 on `PATH`, Go ≥ 1.26 (to build the engine), Node ≥ 20 with `bun` or `npm`, and [`durable-engine`](https://github.com/synadia-ai/durable-engine) checked out beside this repo. From this repo's root:

```sh
nats-server -js &
(cd ../durable-engine && git checkout $(cat ../durable-sdk/engine.pin) && make build-dev)   # bin/durable-engine-dev, bin/durable-licence
../durable-engine/bin/durable-licence mint -test -name "$USER" -out de.licence           # a test licence: 30 days, generous limits
../durable-engine/bin/durable-engine-dev serve --nats nats://127.0.0.1:4222 --licence de.licence &
bun install
NATS_URL=nats://127.0.0.1:4222 npm run example:charge
```

The `git checkout` leaves the engine's checkout detached at the pin; `git checkout main` there afterwards. A standalone `serve` starts only with a valid licence (`--licence FILE` or `$DE_LICENCE`; the engine's `docs/QUICKSTART.md`, `docs/API.md` "The licence"); a test licence is accepted by a `-dev` build alone. The engine logs `licence … licensee=<you>`, provisions its seven resources at start and answers on `de.api.v1.*`. Check it with `nats req de.api.v1.info '{}'`. The example ends with `result: { status: 'shipped', orderId: 'ord-c-42', tracking: 'TRK-c-42' }` and `status: { state: 'completed', … }`. Stop the engine and the server with `kill %2 %1`.

### Which engine

**How you get one:** on the Synadia Agent Fabric the tenant's engine is already there, the operator's, reached through your tenant's import ([`docs/FABRIC.md`](https://github.com/synadia-ai/durable-sdk/blob/main/docs/FABRIC.md)); you run none. Standalone the engine is not public: Synadia hands you its archive or image and a licence, and you run it next to your NATS (`serve --nats … --licence FILE`).

**Which build:** this SDK needs the engine at or after its #13 (`16cb3da`, the pin), which announces `withdrawn-remembered` in `info.capabilities` (a worker withdrawn by `stop()` is remembered 5 s, so `stop()` sends one `cancelPoll`; the engine's F53, F57). `worker.start()` rejects with `UnsupportedEngineError` on an older one, naming the engine by its `engine` and `commit` as `info` gives them; a worker whose start found no engine checks at its first answered poll and stops there, logged at error (`health()` says stopped). The floor is read from `info.capabilities`, never from the version: every build so far says `0.2.0-dev` (`docs/findings.md` F45).

The same workflow in a file of your own (`NATS_URL` as above):

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

const nc = await connect({ servers: process.env.NATS_URL ?? "nats://127.0.0.1:4222" });
const worker = createWorker({ nc, workflows: [order] });
try {
  await worker.start();
  const client = createClient({ nc });
  const handle = await client.start(order, { cartId: "c-42" });
  await client.signal(handle.runId, "payment", { approved: true });
  console.log(await handle.result(20_000)); // { status: 'paid', orderId: 'ord-c-42' }
} finally {
  await worker.stop(); // on any error too: its polls withdrawn, an item they were handed goes back
  await nc.drain();
}
```

### Using it from your own project

```sh
npm install @synadia-ai/durable @nats-io/transport-node
npx tsx order.ts                                           # the file above
```

`@nats-io/transport-node` is yours to pick; the SDK takes any `NatsConnection`.

## The examples

Each runs with `npm run example:<name>` against the quickstart's `nats-server -js` and engine. [examples/README.md](https://github.com/synadia-ai/durable-sdk/blob/main/examples/README.md) has each one's command line, and what changes on the fabric (the connection, never the workflow).

| Example        | Shows                                                                                                                |
| -------------- | -------------------------------------------------------------------------------------------------------------------- |
| `charge`       | `run`, `await`, `sleep`, `call`; `start`, `signal`, `result`, `status`                                               |
| `saga`         | compensations in reverse on failure                                                                                  |
| `crash-replay` | a worker killed mid-run; a fresh one resumes without re-running finished steps (run the engine with `--ack-wait 2s`) |
| `fan-out`      | `ctx.startCall` + `Promise.all`, and `ctx.race`                                                                      |
| `cancel`       | `client.cancel` and `ctx.onCancel`                                                                                   |

## What moved to the engine

The worker is the API's loop: one `poll` per concurrency slot; for each item the journal (paged with `load`), the library's `executeRun` with its seams as `append`, `wake` and `child` under the item's lease, `extend` every third of the ack wait, then `done`; every poll names the worker, and `stop()` withdraws its waiting polls (`poll {cancelPoll}`), so a stop takes as long as the items in flight; it returns once every poll has returned, an item one was handed in the instant before handed back (`done retry`), so a process may exit right after it (`docs/findings.md` F44). Stop the worker in a `finally`, as the examples do: a process that exits on an error without `stop()` leaves its polls to the engine for their wait, and an item handed to one waits out its ack wait. A value too large for one request goes up in `put` parts and travels as a ref; a ref the engine hands back, for a value too large for a reply, comes down in `get` parts, its digest checked (`src/engine/blobs.ts`, through the library's own offload code). Everything else the library's worker did is the engine's: the consumers, provisioning, the inbound event, the run lease, the ack matrix, `child-done`, the cascade of a cancel, the forward across continue-as-new, the parked index and the lost-wake repair, the maintenance pass, offload.

So `createWorker` takes `nc`, `workflows`, `prefix`, `dependencies`, `concurrency` (default 16), `pollWaitMs` (default 30 000), `logger`, `metrics` and `payloadCodec`. The library's other options are accepted and ignored, each named for the `serve` flag it became: `ackWaitMs` → `--ack-wait`, `maxWorkMs` → `--max-work`, `lease: false` → `--no-lease`, `leaseTtlMs` → `--lease-ttl`, `streamMaxBytes` → `--stream-max-bytes`, `maintenanceIntervalMs` → `--maintenance-interval`, `reconcileGraceMs` → `--reconcile-grace`, and `offload`'s threshold → `--offload-threshold`; `offload: false` keeps its meaning (nothing goes up in parts, a value too large for a request fails loud). `createClient` takes `nc`, `prefix`, `payloadCodec`, `logger`, `metrics` (`blob.put`, `blob.get`) and `offload: false`.

### Errors

A refusal from the engine is a typed error, one per code of the engine's closed set (the engine's [`docs/API.md`](https://github.com/synadia-ai/durable-engine/blob/main/docs/API.md) "Errors"; mirrored as `ENGINE_ERROR_CODES` in `src/engine/errors.ts` and `python/src/synadia_durable/_engine/errors.py`, and a unit test in each language fails when the engine's table at `engine.pin` has a code the mirror lacks):

| `code`                    | Error                                                                                      | What the SDK does with it                                                                                                                                                                                                                         |
| ------------------------- | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `invalid`                 | `InvalidRequestError` (inside an execution `SerializationError`)                           | never retried; inside an execution the run fails                                                                                                                                                                                                  |
| `fenced`                  | `FencedError` (`EngineFencedError`, with `expectLastSeq` and `serverCode`)                 | the pure core's halt                                                                                                                                                                                                                              |
| `expired`                 | `LeaseExpiredError`                                                                        | the worker drops the item; it redelivers                                                                                                                                                                                                          |
| `not_found`               | `RunNotFoundError`                                                                         | raised to the caller                                                                                                                                                                                                                              |
| `too_large`               | `PayloadTooLargeError`                                                                     | deterministic: raised, or the run fails                                                                                                                                                                                                           |
| `blob_gone`               | `BlobMissingError` / `BlobIntegrityError`                                                  | the worker parks the run, loud (`done retry`)                                                                                                                                                                                                     |
| `unavailable`, `internal` | `EngineUnavailableError` (also no responders, a timeout)                                   | transient: `result()` asks again, inside an execution `done retry`; `internal` is logged at error                                                                                                                                                 |
| `tenant_limit`            | `LimitError` (`code`, `limit`: `bytes`, `runs`, `workflows`, `polls`, `rate`)              | **a refusal, never retried** (F40): a client's call rejects with it; `worker.start()` rejects with it when its first poll is refused (`rate` aside); a refused poll is logged at error naming the limit and polled again after 30 s (`rate`: 1 s) |
| `licence_limit`           | `LimitError` (`limit`: `workflows`, `workers`, `runs`, `runsPerDay`, `tenants`, `expired`) | the same; inside an execution (a `child` at `runs`, an `append` at `bytes`) the item goes back (`done retry`), logged at error                                                                                                                    |

A closed connection is `ConnectionClosedError`. An engine older than this SDK's floor (no `withdrawn-remembered` in `info.capabilities`) is `UnsupportedEngineError` (`code` `unsupported`, this SDK's, with `engine`, `commit`, `capabilities`, `missing`), never retried ([Which engine](#which-engine)). A code the SDK does not know (an engine newer than it) stays `EngineUnavailableError`, logged at warn. A worker whose polls fail (no engine, a slow link) says `poll failed; retrying` once a minute, with the count of failures since (F41).

**Sizes:** a value of any size the blob store holds; one larger than a request (`request_max`, about `max_payload` − 16 KiB) costs a `put` round trip per part of about 6 MiB on an 8 MiB server (`docs/findings.md` F1, F26). After `result()`, `status()` can read the previous state for a round trip of the worker's (F27).

## Running a worker

The user's half of running it; the engine's operator has the other (the engine's `docs/OPERATIONS.md`). The library's `docs/OPERATIONS.md` held both; what is the engine's now is named where it went.

| Concern                            | What to do                                                                                                                                                                       | Mechanism                                                                                                                                                                                                                         |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| the connection                     | connect with `withReconnectDefaults({ servers, … })`                                                                                                                             | `@nats-io` gives up after 10 reconnect attempts (~20 s) and closes the connection for good; the helper sets the limit to −1 and never overrides an option you set                                                                 |
| a connection closed for good       | restart the process; point a readiness probe at `worker.health()`                                                                                                                | a waiting `result()` rejects with `ConnectionClosedError` (the run is unaffected); the worker reports `running: false`, `connectionClosed: true`                                                                                  |
| no engine, or a slow link          | nothing                                                                                                                                                                          | `worker.start()` warns `no engine answered info; polling until one does`, and the first answered poll checks the engine's floor (F45); a failing poll says `poll failed; retrying` once a minute with a count (F41)               |
| a crash mid-step                   | make a step's effect idempotent, keyed by `ctx.key()`                                                                                                                            | a finished step is journaled and never re-run; a step in flight runs again on the next worker (at-least-once)                                                                                                                     |
| a step longer than the ack wait    | nothing, up to the engine's `--max-work` (5 min by default)                                                                                                                      | the worker `extend`s the lease every third of the ack wait; past `--max-work` the item redelivers. Raising it is the engine operator's flag                                                                                       |
| how soon a dead worker's run moves | the engine's `--ack-wait` (30 s by default)                                                                                                                                      | the item redelivers once the lease lapses                                                                                                                                                                                         |
| plaintext values in the journal    | `payloadCodec: aesGcmPayloadCodec(key)` on **every** worker and client of the prefix (32 bytes, from your secret store)                                                          | it transforms the payload slot only; the engine and the console see a run's shape and never a value. Not retroactive, one-way in practice, and the key's rotation is yours (the library's `docs/OPERATIONS.md` "Payload privacy") |
| who may start, signal or cancel    | the account's authorization (standalone), or your tenant's users ([on the fabric](https://github.com/synadia-ai/durable-sdk/blob/main/docs/FABRIC.md#standalone-and-the-fabric)) | every call is a request on `{prefix}.api.v1.<op>`                                                                                                                                                                                 |

## Contributing

The test suites, the harness's modes against a spawned or an existing server, and the interop worker: [`docs/DEVELOPING.md`](https://github.com/synadia-ai/durable-sdk/blob/main/docs/DEVELOPING.md).
