# On the fabric

How a workflow author runs this SDK on the Synadia Agent Fabric, in either language. The SDK is the same one as standalone, and so is the code; the connection differs, and the limits. This page is the user's side. The operator's side (the engine in the DE account, its tenants' limits, onboarding) is the engine's `docs/OPERATIONS.md` and the console's. The contract under both is the engine's `docs/API.md`, "Tenancy", at `engine.pin`.

Walked on 2026-10-05 at `engine.pin` `97a1778`, on a local three-account server with the engine's fabric build in tenancy mode ([Walk it locally](#walk-it-locally)), not on the fabric itself.

## What you need

| You need                                            | What it is                                                                                                                             | Who makes it                                            |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------- |
| a connection to **your own tenant**                 | any user of the tenant's account, with its creds file                                                                                  | you, from the tenant's users                            |
| the tenant's **service import** of the engine's API | `de.api.>`, imported from the DE account with `share: true`                                                                            | the console's onboarding (decision 61), once per tenant |
| this SDK                                            | `@synadia-ai/durable` or `synadia-durable`, unpublished: [Using it from your own project](../README.md#using-it-from-your-own-project) |                                                         |

That is all. **No durable rights:** the user needs no right on a stream, a bucket or an object store, only to publish on `de.api.>`. **No provisioning and no sizing:** the engine's resources live in the DE account, and a worker's first poll binds its workflow names there. **No licence:** `DE_LICENCE` and `--licence` are standalone's. The agent creates nothing.

## The code

`defineWorkflow`, `createWorker` and `createClient`, as standalone. The prefix is `de`, the SDK's default, so nothing is passed. Every call is one request on `de.api.v1.<op>` in your account; the import carries it to the engine. Only the connection changes:

```ts
import { readFileSync } from "node:fs";
import { connect, credsAuthenticator } from "@nats-io/transport-node";
import { createClient, createWorker, withReconnectDefaults } from "@synadia-ai/durable";

const nc = await connect(
  withReconnectDefaults({
    servers: "tls://connect.ngs.global",
    authenticator: credsAuthenticator(readFileSync(process.env.NATS_CREDS!)),
  }),
);
const worker = createWorker({ nc, workflows: [order] }); // prefix "de", the default
await worker.start();
const client = createClient({ nc });
```

```python
import os

import nats

from synadia_durable import create_client, create_worker, with_reconnect_defaults

# inside your async main(), as in the README's quickstart
nc = await nats.connect(
    "tls://connect.ngs.global",
    user_credentials=os.environ["NATS_CREDS"],  # needs nkeys: pip install 'nats-py[nkeys]' (findings F30)
    **with_reconnect_defaults(),
)
worker = create_worker(nc, [order])  # prefix "de", the default
await worker.start()
client = create_client(nc)
```

The URL is the fabric's; NGS is shown. The examples in [`examples/`](../examples/README.md) read `NATS_URL` alone and take no creds file, so on the fabric the connection above is the line to put in their place.

## Where the run lives

| What                                         | Where                                                          | Mechanism                                                                                                  |
| -------------------------------------------- | -------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| the journal                                  | the DE account, `de.<tenant>.journal.<run id>` on `DE_JOURNAL` | the engine takes the tenant from the import's subject alone, never from the request                        |
| the status, the lease, the timers, the blobs | the DE account, under the tenant's key                         | shared by every tenant, the tenant the first token (`docs/API.md` "The resources")                         |
| your account                                 | nothing                                                        | the import is the only thing there                                                                         |
| who did it                                   | `by` on the `start`, `signal` and `cancel-requested` entries   | the server's stamp of the caller's user at the account boundary (`Nats-Request-Info`), not the SDK's claim |

**What the console shows** (the fabric's `afo-design.md` §8.5): every run of the tenant in its Runs section, with `by`. A run whose id follows [the join](#the-join-for-an-agent-author) also shows under the thread and the tool call that started it. A run of any other id shows in the Runs section and in no task. With a payload codec ([Running a worker](../README.md#running-a-worker)) the console sees a run's shape, names, timing and status, never a value.

## The join, for an agent author

A run started by an agent's tool call belongs to that call's task by its id. It is a convention, not a record; the fabric SDK's [`docs/durable.md`](../../agent-fabric-sdk/docs/durable.md) (0.2.0-alpha) is the page, and its helpers make it one line. In short:

| Part                       | What                                                                                           | The fabric SDK's helper                                     |
| -------------------------- | ---------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| the run id                 | `<thread_id>-<tool call id>`, passed as `start`'s `runId` (`StartOptions(run_id=…)` in Python) | `durableRunId(toolCallId)` / `durable_run_id(tool_call_id)` |
| children, generations      | `<run id>_<n>`, `<run id>_g<n>`: the SDK derives them, so they join the same task              | none needed                                                 |
| the trace                  | `{thread_id, root_id}` in the run's input, since a step runs outside the prompt's context      | `durableTrace()` / `durable_trace()`                        |
| a model call inside a step | the two headers the model proxy reads, from the trace in the input                             | `durableHeaders(trace)` / `durable_headers(trace)`          |

This SDK knows none of it: it takes the run id and journals the input it is given.

## The limits

A tenant has five limits, set by the fabric's operator (`docs/API.md` "The limits"). A request over one is refused `tenant_limit` with `limit` naming it. No other tenant is affected.

| `limit`     | Counts                                            | Refused                                             |
| ----------- | ------------------------------------------------- | --------------------------------------------------- |
| `bytes`     | bytes journaled and offloaded, the tenant's total | `start`, `child`, `signal`, `append`, `put`         |
| `runs`      | runs in flight, the tenant's total                | a `start` or `child` of a new run (a re-kick never) |
| `workflows` | workflow names with durables                      | a `poll` naming a new name                          |
| `polls`     | polls waiting, per engine instance                | `poll`                                              |
| `rate`      | requests per second, per engine instance          | every op but `done` and `extend`                    |

The SDK surfaces every one as **`LimitError`** (`code` `tenant_limit`, `limit`, the engine's message). **It is a refusal, never retried** (`docs/findings.md` F40):

| Where                                                             | What happens                                                                                          |
| ----------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| a client's call (`start`, `signal`, `cancel`, `result`, …)        | rejects (raises) with it, after one request                                                           |
| `worker.start()`                                                  | rejects with it when its first poll is refused (`rate` aside: it clears within a second)              |
| a worker's later poll                                             | logged at error naming the limit; polled again after 30 s (`rate`: 1 s)                               |
| inside an execution (an `append` at `bytes`, a `child` at `runs`) | the item goes back (`done retry`), logged at error; the run waits until the tenant is under the limit |

The totals are eventual by a second: several instances together can pass a limit by what they wrote in that second. A limit is raised by the operator, never by the SDK.

## The refusals

Every refusal on the fabric, with the engine's code (`docs/API.md` "Errors"; mirrored as `ENGINE_ERROR_CODES` in both languages):

| `code`                    | On the fabric                                                                                                                    | Error                                                            | What you do                                                                                                                                                                                                    |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tenant_limit`            | the tenant is at one of its five limits                                                                                          | `LimitError`                                                     | read `limit`; wait for runs to end, or ask the operator                                                                                                                                                        |
| `licence_limit`           | not expected: the fabric's engine runs with no licence                                                                           | `LimitError`                                                     | ask the operator                                                                                                                                                                                               |
| `not_found`               | `signal` or `cancel` of a run that is not **your tenant's** (never started, or another tenant's)                                 | `RunNotFoundError`                                               | check the run id                                                                                                                                                                                               |
| `invalid`                 | a malformed request; an identifier outside `[A-Za-z0-9_-]+`; a request with no caller identity (an import without `share: true`) | `InvalidRequestError` (inside an execution `SerializationError`) | fix the request; for no identity, the onboarding                                                                                                                                                               |
| `too_large`               | a value past `request_max` even in parts                                                                                         | `PayloadTooLargeError`                                           | make the value smaller                                                                                                                                                                                         |
| `blob_gone`               | a blob a journal entry names is gone or corrupt                                                                                  | `BlobMissingError` / `BlobIntegrityError`                        | the run parks, loud; the operator restores the blob, or you cancel the run                                                                                                                                     |
| `fenced`                  | another writer moved the run on                                                                                                  | `FencedError`                                                    | nothing: the worker's halt, the item redelivers                                                                                                                                                                |
| `expired`                 | the item's lease lapsed                                                                                                          | `LeaseExpiredError`                                              | nothing: the item redelivers                                                                                                                                                                                   |
| `unavailable`, `internal` | the engine or the broker, transient; also no responders (no import, or no engine) and a timeout                                  | `EngineUnavailableError`                                         | `result()` asks again and a worker polls again (`worker.start()` warns `no engine answered info; polling until one does`); a client's other call raises it. No responders that last mean the import is missing |

`ConnectionClosedError` is the connection's, not the engine's.

## Standalone and the fabric

|                      | Standalone                                                            | The fabric                                                                                |
| -------------------- | --------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| the engine           | yours, next to your NATS: `durable-engine serve`                      | ours, in the DE account: `serve --tenancy`, the fabric build                              |
| the licence          | required: `--licence FILE` or `DE_LICENCE`                            | none                                                                                      |
| provisioning         | the engine's, at its start, in your account                           | the engine's, in the DE account; nothing in yours                                         |
| your user's rights   | publish on `de.api.>` (the engine's own user holds the stream rights) | publish on `de.api.>`, through the import                                                 |
| the prefix           | `de` unless the engine says `--prefix`                                | `de`                                                                                      |
| the limits           | the licence's: `licence_limit`                                        | the tenant's: `tenant_limit`                                                              |
| who may cancel a run | anyone who may publish on `de.api.v1.cancel`                          | any user of your tenant, and the console (as its own user, stamped `by`); no other tenant |
| `by` on entries      | absent                                                                | the caller's user                                                                         |
| the code             | the same                                                              | the same                                                                                  |

## What the SDK cannot do on the fabric

- **Read another tenant's run.** The engine takes the tenant from the import's subject; another tenant's run is `pending` to `status` and `result` and `not_found` to `signal` and `cancel`.
- **Read the journal or the streams directly.** The tenant holds no right on them; `status()` and `result()` are the API's.
- **Create, size or provision a resource.** The engine provisions; the operator sizes.
- **Bypass a limit.** A `LimitError` is never retried in a loop, and nothing the SDK sends raises a limit.
- **Claim an identity.** `by` is the server's stamp of the connection's user; an entry the SDK appends carrying `by` or `for` is refused `invalid`. `onBehalfOf` is the console's alone.
- **See the usage records.** They are the engine's, on its own subject in the DE account.

## Walk it locally

The fabric's shape on your laptop: a server with three accounts (`DE`, the engine's; `A`, a tenant; the import between them, as onboarding makes it) and the engine's fabric build in tenancy mode. No licence. From this repo's root, the engine checked out beside it at `engine.pin`:

```sh
cat > tenancy.conf <<'EOF'
host: 127.0.0.1
port: 14322
jetstream { store_dir: "/tmp/de-tenancy" }
accounts {
  DE { jetstream: enabled, users: [ { user: de, password: de } ],
       exports: [ { service: "de.A.api.>", accounts: [A] } ] }
  A  { users: [ { user: a-client, password: a-client } ],
       imports: [ { service: { account: DE, subject: "de.A.api.>" }, to: "de.api.>", share: true } ] }
}
EOF
nats-server -c tenancy.conf &
(cd ../durable-engine && make build-fabric)                         # bin/durable-engine-fabric: the only build with --tenancy (F43)
../durable-engine/bin/durable-engine-fabric serve --tenancy --nats nats://de:de@127.0.0.1:14322 &
nats --server nats://127.0.0.1:14322 --user a-client --password a-client req de.api.v1.info '{}'   # "tenant":{"id":"A",…}
(cd python && NATS_URL=nats://a-client:a-client@127.0.0.1:14322 uv run python examples/charge.py)
```

The Python example runs unchanged as tenant A: nats-py reads `user:password@` from the URL. The TypeScript examples do not (nats.js takes no user from a URL), so use the quickstart's snippet with `connect({ servers: "nats://127.0.0.1:14322", user: "a-client", pass: "a-client" })`. Then, as the engine's account, see where the run went and who started it:

```sh
nats --server nats://127.0.0.1:14322 --user de --password de stream subjects DE_JOURNAL   # de.A.journal.<run id>
nats --server nats://127.0.0.1:14322 --user de --password de stream get DE_JOURNAL 1      # …,"by":"a-client"}
```

The integration suites run the same way as a tenant (`DE_TENANT`; the root README's "As a tenant", `docs/findings.md` F38), and `limits` proves a `tenant_limit` refusal end to end (F40).
