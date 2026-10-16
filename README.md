# @synadia-ai/durable

Durable execution for TypeScript on NATS. Workflows are ordinary async functions over a `ctx`: every step is journaled, and after a crash a run resumes where it was, without repeating finished steps. Your workflow code runs in your process. The journal, the timers and the work queue are kept by **durable-engine**, which Synadia ships to you, or which the Synadia Agent Fabric runs for you.

## Quickstart

A workflow on your laptop: NATS with JetStream, the engine next to it, and a workflow in a project of your own. You need `nats-server` ≥ 2.14 on `PATH` and Node ≥ 20, and from Synadia **the engine for your platform** (the archive `durable-engine_<version>_<os>_<arch>.tar.gz`, or the container image) and **a licence file**. On the Synadia Agent Fabric you need neither: the engine is already there ([`docs/FABRIC.md`](https://github.com/synadia-ai/durable-sdk/blob/main/docs/FABRIC.md)).

```sh
nats-server -js &                                                    # 1. NATS with JetStream, on 127.0.0.1:4222
tar xzf durable-engine_<version>_<os>_<arch>.tar.gz                  # 2. the engine Synadia sent you
./durable-engine_<version>_<os>_<arch>/durable-engine serve \
  --nats nats://127.0.0.1:4222 --licence durable.licence &           # 3. the engine, with your licence
mkdir order && cd order && npm init -y && npm pkg set type=module    # 4. a project of your own
npm install @synadia-ai/durable @nats-io/transport-node tsx
NATS_URL=nats://127.0.0.1:4222 npx tsx order.ts                      # 5. the file below
```

`order.ts`:

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

It prints `{ status: 'paid', orderId: 'ord-c-42' }`. The engine logs `licence … licensee=<you>` and `serving` at start; it creates its streams and buckets on the first start and answers on `de.api.v1.*` (`nats req de.api.v1.info '{}'` shows it, with the `nats` CLI). Without a valid licence it exits at once and says why. Stop the engine and the server with `kill %2 %1`.

**A signal's id.** Every `client.signal` carries an id, a generated UUID unless you pass your own: `client.signal(runId, "payment", data, { id: "pay-c-42" })` (`[A-Za-z0-9_-]+`, checked before any request). A retry passes the same id: if you send a signal again after an `EngineUnavailableError`, a timeout, or a restart of your own process, pass the id of the first try, and the engine answers `ok` and journals nothing more. Without an id each call is a new signal. The id is looked for in two journals only: the run the request names and the generation current when the request arrives. A signal journaled in any other generation (one that was current at the first try and has continued since) is not recognised, and is journaled again. The id dedups one signal: two signals with the same name and different ids are two signals.

**With the container image** instead of the archive, step 3 is `docker run --rm --network host -v $PWD/durable.licence:/etc/durable-engine/licence:ro durable-engine:<version>` (on Docker Desktop add `--nats nats://host.docker.internal:4222` and drop `--network host`).

`@nats-io/transport-node` is yours to pick; the SDK takes any `NatsConnection`. On the fabric only the connection changes: your tenant's URL and credentials, never the workflow.

## Which engine

**How you get one:** on the Synadia Agent Fabric the tenant's engine is already there, the operator's, reached through your tenant's import ([`docs/FABRIC.md`](https://github.com/synadia-ai/durable-sdk/blob/main/docs/FABRIC.md)); you run none. Standalone, Synadia hands you the engine's archive or image and a licence, and you run it next to your NATS, as in the Quickstart.

## The examples

Each runs with `npm run example:<name>` from a clone of this repository, against the Quickstart's `nats-server -js` and engine. [examples/README.md](https://github.com/synadia-ai/durable-sdk/blob/main/examples/README.md) has each one's command line, and what changes on the fabric (the connection, never the workflow).

| Example        | Shows                                                                                                                |
| -------------- | -------------------------------------------------------------------------------------------------------------------- |
| `charge`       | `run`, `await`, `sleep`, `call`; `start`, `signal`, `result`, `status`                                               |
| `saga`         | compensations in reverse on failure                                                                                  |
| `crash-replay` | a worker killed mid-run; a fresh one resumes without re-running finished steps (run the engine with `--ack-wait 2s`) |
| `fan-out`      | `ctx.startCall` + `Promise.all`, and `ctx.race`                                                                      |
| `cancel`       | `client.cancel` and `ctx.onCancel`                                                                                   |

## Errors

A refusal from the engine is a typed error, exported from the package:

| Error                    | When                                                           | Retried       |
| ------------------------ | -------------------------------------------------------------- | ------------- |
| `InvalidRequestError`    | the request was malformed                                      | no            |
| `RunNotFoundError`       | no run with that id                                            | no            |
| `PayloadTooLargeError`   | a value too large to store                                     | no            |
| `LimitError`             | your licence or tenant limit is reached (`limit` names which)  | no            |
| `LeaseExpiredError`      | a worker lost an item it held; the item goes to another worker | by the engine |
| `EngineUnavailableError` | no engine answered in time                                     | yes           |
| `ConnectionClosedError`  | your NATS connection closed for good                           | no: restart   |
| `UnsupportedEngineError` | the engine is older than this SDK supports                     | no            |

## Running a worker

- **Stop it in a `finally`**, as in the Quickstart: `worker.stop()` hands back what it holds, so a process may exit right after it.
- **Make a step's side effect idempotent**, keyed by `ctx.key()`: a finished step never runs again, a step interrupted by a crash runs again on the next worker.
- **Connect with `withReconnectDefaults({ servers, … })`**, so the connection never gives up reconnecting, and point a readiness probe at `worker.health()`.
- **To keep values private**, give `payloadCodec: aesGcmPayloadCodec(key)` (32 bytes, from your secret store) to every worker and client: the engine then sees a run's shape and never a value.

## Contributing

How the SDK works with the engine, the test suites and the interop worker: [`docs/DEVELOPING.md`](https://github.com/synadia-ai/durable-sdk/blob/main/docs/DEVELOPING.md).
