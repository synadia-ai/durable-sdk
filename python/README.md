# `synadia-durable` (Python)

Durable execution for Python on NATS. Workflows are ordinary async functions over a `ctx`: every step is journaled, and after a crash a run resumes where it was, without repeating finished steps. Your workflow code runs in your process. The journal, the timers and the work queue are kept by **durable-engine**, which Synadia ships to you, or which the Synadia Agent Fabric runs for you.

```sh
pip install synadia-durable        # or: uv add synadia-durable
```

This is the thin durable execution SDK for Python, the twin of the TypeScript SDK (`@synadia-ai/durable` on npm); both live in the [durable-sdk repository](https://github.com/synadia-ai/durable-sdk). Its replay core, with the determinism guards and the out-of-band halts, runs in your process. Behind the core's seams sits a request/reply client for the engine, which owns the streams: nothing runs without the engine.

**It runs in two places, with the same code:**

| Where                           | The engine                                                    | Start here                                                                             |
| ------------------------------- | ------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| **standalone**                  | yours, next to your NATS, with a licence                      | the Quickstart below                                                                   |
| **on the Synadia Agent Fabric** | the fabric runs it; your account reaches it through an import | [`docs/FABRIC.md`](https://github.com/synadia-ai/durable-sdk/blob/main/docs/FABRIC.md) |

- **The public API:** `@workflow` / `define_workflow`, `create_client`, `create_worker`, and `ctx.run/sleep/wait/call/start_call/race/now/random/uuid/key/continue_as_new/on_cancel`.
- **The only dependency is `nats-py`**, used for core request/reply alone: every call is one request to `{prefix}.api.v1.<op>` (the engine's `API.md`, the document delivered with the engine). There are no consumers, streams or buckets here, and no JetStream client. `cryptography` is the optional `crypto` extra, for `aes_gcm_payload_codec` (`pip install 'synadia-durable[crypto]'`).
- **Documentation** is this README, [`docs/FABRIC.md`](https://github.com/synadia-ai/durable-sdk/blob/main/docs/FABRIC.md) (on the fabric), the [examples](https://github.com/synadia-ai/durable-sdk/blob/main/python/examples/README.md) and [`docs/DEVELOPING.md`](https://github.com/synadia-ai/durable-sdk/blob/main/docs/DEVELOPING.md) (the test suites). The engine's operator has the documents shipped with the engine, `QUICKSTART.md` and `API.md`.

**Version:** `0.2.0`, the TypeScript SDK's twin.

## Quickstart

A workflow on your laptop: NATS with JetStream, the engine next to it, and a workflow in a project of your own. You need `nats-server` ≥ 2.14 on `PATH`, Python ≥ 3.11 with [`uv`](https://docs.astral.sh/uv/), and from Synadia **the engine for your platform** (the archive `durable-engine_<version>_<os>_<arch>.tar.gz`, or the container image) and **a licence file**. On the Synadia Agent Fabric you need neither: the engine is already there ([`docs/FABRIC.md`](https://github.com/synadia-ai/durable-sdk/blob/main/docs/FABRIC.md)).

```sh
nats-server -js &                                                    # 1. NATS with JetStream, on 127.0.0.1:4222
tar xzf durable-engine_<version>_<os>_<arch>.tar.gz                  # 2. the engine Synadia sent you
./durable-engine_<version>_<os>_<arch>/durable-engine serve \
  --nats nats://127.0.0.1:4222 --licence durable.licence &           # 3. the engine, with your licence
uv init --bare order && cd order && uv add synadia-durable           # 4. a project of your own
NATS_URL=nats://127.0.0.1:4222 uv run python order.py                # 5. the file below
```

With pip instead of uv, step 4 is `mkdir order && cd order && python3 -m venv .venv && .venv/bin/pip install synadia-durable`, and step 5 runs `.venv/bin/python order.py`.

`order.py`:

```python
import asyncio
import os

import nats

from synadia_durable import Ctx, create_client, create_worker, with_reconnect_defaults, workflow


@workflow(name="order")
async def order(ctx: Ctx, cart: dict) -> dict:
    order_id = await ctx.run("reserve", lambda: f"ord-{cart['cartId']}")  # journaled, never re-run
    payment = await ctx.wait("payment")  # parks until a signal
    if not payment["approved"]:
        return {"status": "abandoned", "orderId": order_id}
    await ctx.sleep("settle", 500)  # a durable timer
    return {"status": "paid", "orderId": order_id}


async def main() -> None:
    nc = await nats.connect(
        os.environ.get("NATS_URL", "nats://127.0.0.1:4222"), **with_reconnect_defaults()
    )
    worker = create_worker(nc, [order])
    try:
        await worker.start()
        client = create_client(nc)
        handle = await client.start(order, {"cartId": "c-42"})
        await client.signal(handle.run_id, "payment", {"approved": True})
        print(await handle.result(20_000))  # {'status': 'paid', 'orderId': 'ord-c-42'}
    finally:
        await worker.stop()  # on any error too: its polls withdrawn, their items handed back
        await nc.drain()


asyncio.run(main())
```

It prints `{'status': 'paid', 'orderId': 'ord-c-42'}`. The engine logs `licence … licensee=<you>` and `serving` at start; it creates its streams and buckets on the first start and answers on `de.api.v1.*` (`nats req de.api.v1.info '{}'` shows it, with the `nats` CLI). Without a valid licence it exits at once and says why. Stop the engine and the server with `kill %2 %1`.

**With the container image** instead of the archive, step 3 is `docker run --rm --network host -v $PWD/durable.licence:/etc/durable-engine/licence:ro durable-engine:<version>` (on Docker Desktop add `--nats nats://host.docker.internal:4222` and drop `--network host`).

**Which engine:** this SDK needs an engine that announces `withdrawn-remembered` in `info.capabilities` (a worker withdrawn by `stop()` is remembered 5 s, so `stop()` sends one `cancelPoll`); the repository's `engine.pin` names the engine version the suites were last run against. `worker.start()` raises `UnsupportedEngineError` on an older one, naming the engine by its `engine` and `commit` as `info` gives them; a worker whose start found no engine checks at its first answered poll and stops there, logged at error (`health()` says stopped). The floor is read from `info.capabilities`, never from the version: every build so far says `0.2.0-dev`.

**A signal's id.** Every `client.signal` carries an id, a generated UUID unless you pass your own: `await client.signal(run_id, "payment", data, id="pay-c-42")` (`[A-Za-z0-9_-]+`, checked before any request). A retry passes the same id: if you send a signal again after an `EngineUnavailableError`, a timeout, or a restart of your own process, pass the id of the first try, and the engine answers `ok` and journals nothing more. Without an id each call is a new signal. The id is looked for in two journals only: the run the request names and the generation current when the request arrives. A signal journaled in any other generation (one that was current at the first try and has continued since) is not recognised, and is journaled again. The id dedups one signal: two signals with the same name and different ids are two signals.

### Installing

```sh
pip install synadia-durable                 # or: uv add synadia-durable
pip install 'synadia-durable[crypto]'       # with aes_gcm_payload_codec, for encryption at rest
```

`nats-py` comes with it; a creds file needs `nkeys` beside it (`pip install 'nats-py[nkeys]'`).

## The examples

From a clone of the repository, each runs with `uv run python examples/<name>.py` in its `python/` directory, against the Quickstart's `nats-server -js` and engine. The examples' [README](https://github.com/synadia-ai/durable-sdk/blob/main/python/examples/README.md) has each one's command line, and what changes on the fabric (the connection, never the workflow).

| Example        | Shows                                                                                                                |
| -------------- | -------------------------------------------------------------------------------------------------------------------- |
| `charge`       | `run`, `wait`, `sleep`, `call`; `start`, `signal`, `result`, `status`                                                |
| `saga`         | compensations in reverse on failure                                                                                  |
| `crash_replay` | a worker killed mid-run; a fresh one resumes without re-running finished steps (run the engine with `--ack-wait 2s`) |
| `fan_out`      | `ctx.start_call` + `asyncio.gather`, and `ctx.race`                                                                  |
| `cancel`       | `client.cancel` and `ctx.on_cancel`                                                                                  |

## What moved to the engine

The worker is the API's loop: one `poll` per concurrency slot; for each item the journal (paged with `load`), the core's `execute_run` with its seams as `append`, `wake` and `child` under the item's lease, `extend` every third of the ack wait, then `done`; every poll names the worker, and `stop()` withdraws its waiting polls (`poll {cancelPoll}`), so a stop takes as long as the items in flight; it returns once every poll has returned, an item one was handed in the instant before handed back (`done retry`), so a process may exit right after it. Stop the worker in a `finally`, as the examples do: a process that exits on an error without `stop()` leaves its polls to the engine for their wait, and an item handed to one waits out its ack wait. A value too large for one request goes up in `put` parts and travels as a ref; a ref the engine hands back, for a value too large for a reply, comes down in `get` parts, its digest checked. Everything else is the engine's: the consumers, provisioning, the inbound event, the run lease, the ack matrix, `child-done`, the cascade of a cancel, the forward across continue-as-new, the parked index and the lost-wake repair, the maintenance pass, offload.

So `create_worker` takes `nc`, `workflows`, `prefix`, `dependencies`, `concurrency` (default 16 poll slots), `poll_wait_ms` (default 30 000), `logger`, `metrics` and `payload_codec`. Older options are accepted and ignored, each named for the `serve` flag it became: `ack_wait_ms` → `--ack-wait`, `max_work_ms` → `--max-work`, `lease=False` → `--no-lease`, `lease_ttl_ms` → `--lease-ttl`, `stream_max_bytes` → `--stream-max-bytes`, `maintenance_interval_ms` → `--maintenance-interval`, `reconcile_grace_ms` → `--reconcile-grace`, and `offload`'s threshold → `--offload-threshold`; `offload=False` keeps its meaning (nothing goes up in parts, a value too large for a request fails loud). `create_client` takes `nc`, `prefix`, `payload_codec`, `logger`, `metrics` (`blob.put`, `blob.get`) and `offload=False`.

### Errors

A refusal from the engine is a typed error, one per code of the engine's closed set (the engine's `API.md`, the document shipped with the engine, "Errors"; mirrored as `ENGINE_ERROR_CODES` in `synadia_durable._engine.errors`, and in the TypeScript SDK):

| `code`                    | Error                                                                                      | What the SDK does with it                                                                                                                                                                                                       |
| ------------------------- | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `invalid`                 | `InvalidRequestError` (inside an execution `SerializationError`)                           | never retried; inside an execution the run fails                                                                                                                                                                                |
| `fenced`                  | `FencedError` (`EngineFencedError`, with `expect_last_seq` and `server_code`)              | the pure core's halt                                                                                                                                                                                                            |
| `expired`                 | `LeaseExpiredError`                                                                        | the worker drops the item; it redelivers                                                                                                                                                                                        |
| `not_found`               | `RunNotFoundError` (also a `LookupError`)                                                  | raised to the caller                                                                                                                                                                                                            |
| `too_large`               | `PayloadTooLargeError`                                                                     | deterministic: raised, or the run fails                                                                                                                                                                                         |
| `blob_gone`               | `BlobMissingError` / `BlobIntegrityError`                                                  | the worker parks the run, loud (`done retry`)                                                                                                                                                                                   |
| `unavailable`, `internal` | `EngineUnavailableError` (also no responders, a timeout)                                   | transient: `result()` asks again, inside an execution `done retry`; `internal` is logged at error                                                                                                                               |
| `tenant_limit`            | `LimitError` (`code`, `limit`: `bytes`, `runs`, `workflows`, `polls`, `rate`)              | **a refusal, never retried**: a client's call raises it; `worker.start()` raises it when its first poll is refused (`rate` aside); a refused poll is logged at error naming the limit and polled again after 30 s (`rate`: 1 s) |
| `licence_limit`           | `LimitError` (`limit`: `workflows`, `workers`, `runs`, `runsPerDay`, `tenants`, `expired`) | the same; inside an execution (a `child` at `runs`, an `append` at `bytes`) the item goes back (`done retry`), logged at error                                                                                                  |

A closed connection is `ConnectionClosedError`. An engine older than this SDK's floor (no `withdrawn-remembered` in `info.capabilities`) is `UnsupportedEngineError` (`code` `unsupported`, this SDK's, with `engine`, `commit`, `capabilities`, `missing`), never retried (Which engine, in the Quickstart). A code the SDK does not know (an engine newer than it) stays `EngineUnavailableError`, logged at warn. A worker whose polls fail (no engine, a slow link) says `poll failed; retrying` once a minute, with the count of failures since.

**Sizes:** a value of any size the blob store holds; one larger than a request (`request_max`, about `max_payload` − 16 KiB) takes a `put` round trip per part of about 6 MiB on an 8 MiB server. After `result()`, `status()` can read the previous state for a round trip of the worker's.

## Running a worker

The user's half of running it; the engine's operator has the other.

| Concern                            | What to do                                                                                                                                                                       | Mechanism                                                                                                                                                                                                        |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| the connection                     | connect with `nats.connect(url, **with_reconnect_defaults())`                                                                                                                    | nats-py gives up after 60 reconnect attempts (~2 min) and closes the connection for good; the helper sets the limit to −1 and never overrides an option you set                                                  |
| a connection closed for good       | restart the process; point a readiness probe at `worker.health()`                                                                                                                | a waiting `result()` raises `ConnectionClosedError` (the run is unaffected); the worker reports `running=False`, `connection_closed=True`                                                                        |
| no engine, or a slow link          | nothing                                                                                                                                                                          | `worker.start()` warns `no engine answered info; polling until one does`, and the first answered poll checks the engine's floor; a failing poll says `poll failed; retrying` once a minute with a count          |
| a crash mid-step                   | make a step's effect idempotent, keyed by `ctx.key()`                                                                                                                            | a finished step is journaled and never re-run; a step in flight runs again on the next worker (at-least-once)                                                                                                    |
| a step longer than the ack wait    | nothing, up to the engine's `--max-work` (5 min by default)                                                                                                                      | the worker `extend`s the lease every third of the ack wait; past `--max-work` the item redelivers. Raising it is the engine operator's flag                                                                      |
| how soon a dead worker's run moves | the engine's `--ack-wait` (30 s by default)                                                                                                                                      | the item redelivers once the lease lapses                                                                                                                                                                        |
| plaintext values in the journal    | `payload_codec=aes_gcm_payload_codec(key)` on **every** worker and client of the prefix (32 bytes, from your secret store; `pip install 'synadia-durable[crypto]'`)              | it transforms the payload slot only; the engine and the console see a run's shape and never a value. Not retroactive, one-way in practice, and the key's rotation is yours (the engine's protocol specification) |
| who may start, signal or cancel    | the account's authorization (standalone), or your tenant's users ([on the fabric](https://github.com/synadia-ai/durable-sdk/blob/main/docs/FABRIC.md#standalone-and-the-fabric)) | every call is a request on `{prefix}.api.v1.<op>`                                                                                                                                                                |

## Contributing

How the SDK works with the engine, the test suites in both languages, the harness's modes and the interop worker: the repository's [`docs/DEVELOPING.md`](https://github.com/synadia-ai/durable-sdk/blob/main/docs/DEVELOPING.md).
