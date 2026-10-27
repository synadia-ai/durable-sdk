# `synadia-durable` (Python)

The thin durable execution SDK for Python, the twin of the TypeScript one at the root of this repo. It keeps the replay core of the retired library `synadia-durable`, copied from it and edited since: workflows as ordinary functions over a `ctx`, every side-effecting or nondeterministic step journaled and replayed after a crash, the determinism guards, the out-of-band halts. Behind the core's seams sits a request/reply client for the engine, `durable-engine`, which owns the streams. A customer's workflow functions run in the customer's process; nothing runs without the engine.

**It runs in two places, with the same code:**

| Where                           | The engine                                                        | Start here                               |
| ------------------------------- | ----------------------------------------------------------------- | ---------------------------------------- |
| **standalone**                  | yours, next to your NATS, with a licence                          | the [Quickstart](#quickstart) below      |
| **on the Synadia Agent Fabric** | ours, in the DE account; your tenant reaches it through an import | [`../docs/FABRIC.md`](../docs/FABRIC.md) |

- **The public API** is the library's: `@workflow` / `define_workflow`, `create_client`, `create_worker`, and `ctx.run/sleep/wait/call/start_call/race/now/random/uuid/key/continue_as_new/on_cancel`.
- **The only dependency is `nats-py`**, used for core request/reply alone: every call is one request to `{prefix}.api.v1.<op>` (the engine's `API.md`, the document delivered with the engine). There are no consumers, streams or buckets here, and no JetStream client. `cryptography` is the optional `crypto` extra, for `aes_gcm_payload_codec`.
- **Documentation** is this README, [`../docs/FABRIC.md`](../docs/FABRIC.md) (on the fabric), the [examples](examples/README.md) and [`../docs/DEVELOPING.md`](../docs/DEVELOPING.md) (the test suites). The engine's operator has the documents shipped with the engine, `QUICKSTART.md` and `API.md`.

**Status:** `0.2.0`, the TypeScript SDK's twin. Unpublished.

## Quickstart

A workflow on your laptop: NATS with JetStream, the engine next to it, and the `charge` example. You need `nats-server` ≥ 2.14 on `PATH`, Python ≥ 3.11 with [`uv`](https://docs.astral.sh/uv/), and from Synadia **the engine for your platform** (the archive `durable-engine_<version>_<os>_<arch>.tar.gz`, or the container image) and **a licence file**. On the Synadia Agent Fabric you need neither: the engine is already there ([`../docs/FABRIC.md`](../docs/FABRIC.md)). From this repo's root:

```sh
nats-server -js &                                                    # 1. NATS with JetStream, on 127.0.0.1:4222
tar xzf durable-engine_<version>_<os>_<arch>.tar.gz                  # 2. the engine Synadia sent you
./durable-engine_<version>_<os>_<arch>/durable-engine serve \
  --nats nats://127.0.0.1:4222 --licence durable.licence &           # 3. the engine, with your licence
cd python && uv sync                                                 # 4. this SDK and its examples
NATS_URL=nats://127.0.0.1:4222 uv run python examples/charge.py      # 5. the charge example
```

The engine logs `licence … licensee=<you>` and `serving` at start; it creates its streams and buckets on the first start and answers on `de.api.v1.*` (`nats req de.api.v1.info '{}'` shows it, with the `nats` CLI). Without a valid licence it exits at once and says why. The example ends with `result: {'status': 'shipped', 'order_id': 'ord-c-42', 'tracking': 'TRK-c-42'}` and `status: RunStatus(state='completed', …)`. Stop the engine and the server with `kill %2 %1`.

**With the container image** instead of the archive, step 3 is `docker run --rm --network host -v $PWD/durable.licence:/etc/durable-engine/licence:ro durable-engine:<version>` (on Docker Desktop add `--nats nats://host.docker.internal:4222` and drop `--network host`).

**Which engine:** this SDK needs the engine at or after its #13 (`16cb3da`; `engine.pin` is `91ac210`), which announces `withdrawn-remembered` in `info.capabilities` (a worker withdrawn by `stop()` is remembered 5 s, so `stop()` sends one `cancelPoll`). `worker.start()` raises `UnsupportedEngineError` on an older one, naming the engine by its `engine` and `commit` as `info` gives them; a worker whose start found no engine checks at its first answered poll and stops there, logged at error (`health()` says stopped). The floor is read from `info.capabilities`, never from the version: every build so far says `0.2.0-dev`.

The same workflow in a file of your own (`NATS_URL` as above):

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

**A signal's id.** Every `client.signal` carries an id, a generated UUID unless you pass your own: `await client.signal(run_id, "payment", data, id="pay-c-42")` (`[A-Za-z0-9_-]+`, checked before any request). A retry passes the same id: if you send a signal again after an `EngineUnavailableError`, a timeout, or a restart of your own process, pass the id of the first try, and the engine answers `ok` and journals nothing more. Without an id each call is a new signal. The id is looked for in two journals only: the run the request names and the generation current when the request arrives. A signal journaled in any other generation (one that was current at the first try and has continued since) is not recognised, and is journaled again. The id dedups one signal: two signals with the same name and different ids are two signals.

### Using it from your own project

The package is unpublished. Until it is, install it from a clone of this repository:

```sh
uv add ../durable-sdk/python            # in your project (pip: pip install -e ../durable-sdk/python)
uv run python order.py                  # the file above
```

`nats-py` comes with it; a creds file needs `nkeys` beside it (`pip install 'nats-py[nkeys]'`).

## The examples

Each runs with `uv run python examples/<name>.py` (in `python/`) against the quickstart's `nats-server -js` and engine. [examples/README.md](examples/README.md) has each one's command line, and what changes on the fabric (the connection, never the workflow).

| Example        | Shows                                                                                                                |
| -------------- | -------------------------------------------------------------------------------------------------------------------- |
| `charge`       | `run`, `wait`, `sleep`, `call`; `start`, `signal`, `result`, `status`                                                |
| `saga`         | compensations in reverse on failure                                                                                  |
| `crash_replay` | a worker killed mid-run; a fresh one resumes without re-running finished steps (run the engine with `--ack-wait 2s`) |
| `fan_out`      | `ctx.start_call` + `asyncio.gather`, and `ctx.race`                                                                  |
| `cancel`       | `client.cancel` and `ctx.on_cancel`                                                                                  |

## What moved to the engine

The worker is the API's loop: one `poll` per concurrency slot; for each item the journal (paged with `load`), the library's `execute_run` with its seams as `append`, `wake` and `child` under the item's lease, `extend` every third of the ack wait, then `done`; every poll names the worker, and `stop()` withdraws its waiting polls (`poll {cancelPoll}`), so a stop takes as long as the items in flight; it returns once every poll has returned, an item one was handed in the instant before handed back (`done retry`), so a process may exit right after it. Stop the worker in a `finally`, as the examples do: a process that exits on an error without `stop()` leaves its polls to the engine for their wait, and an item handed to one waits out its ack wait. A value too large for one request goes up in `put` parts and travels as a ref; a ref the engine hands back, for a value too large for a reply, comes down in `get` parts, its digest checked (`_engine/blobs.py`). Everything else the library's worker did is the engine's: the consumers, provisioning, the inbound event, the run lease, the ack matrix, `child-done`, the cascade of a cancel, the forward across continue-as-new, the parked index and the lost-wake repair, the maintenance pass, offload.

So `create_worker` takes `nc`, `workflows`, `prefix`, `dependencies`, `concurrency` (default 16 poll slots), `poll_wait_ms` (default 30 000), `logger`, `metrics` and `payload_codec`. The library's other options are accepted and ignored, each named for the `serve` flag it became: `ack_wait_ms` → `--ack-wait`, `max_work_ms` → `--max-work`, `lease=False` → `--no-lease`, `lease_ttl_ms` → `--lease-ttl`, `stream_max_bytes` → `--stream-max-bytes`, `maintenance_interval_ms` → `--maintenance-interval`, `reconcile_grace_ms` → `--reconcile-grace`, and `offload`'s threshold → `--offload-threshold`; `offload=False` keeps its meaning (nothing goes up in parts, a value too large for a request fails loud). `create_client` takes `nc`, `prefix`, `payload_codec`, `logger`, `metrics` (`blob.put`, `blob.get`) and `offload=False`.

### Errors

A refusal from the engine is a typed error, one per code of the engine's closed set (the engine's `API.md`, the document shipped with the engine, "Errors"; mirrored as `ENGINE_ERROR_CODES` in `src/synadia_durable/_engine/errors.py` and `../src/engine/errors.ts`; where the engine's source is at hand, a unit test in each language, `tests/test_engine_errors.py` here, fails when the engine's table at `engine.pin` has a code the mirror lacks):

| `code`                    | Error                                                                                      | What the SDK does with it                                                                                                                                                                                                       |
| ------------------------- | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `invalid`                 | `InvalidRequestError` (inside an execution `SerializationError`)                           | never retried; inside an execution the run fails                                                                                                                                                                                |
| `fenced`                  | `FencedError` (`EngineFencedError`, with `expect_last_seq` and `server_code`)              | the pure core's halt                                                                                                                                                                                                            |
| `expired`                 | `LeaseExpiredError`                                                                        | the worker drops the item; it redelivers                                                                                                                                                                                        |
| `not_found`               | `RunNotFoundError` (also a `LookupError`, as the library raised)                           | raised to the caller                                                                                                                                                                                                            |
| `too_large`               | `PayloadTooLargeError`                                                                     | deterministic: raised, or the run fails                                                                                                                                                                                         |
| `blob_gone`               | `BlobMissingError` / `BlobIntegrityError`                                                  | the worker parks the run, loud (`done retry`)                                                                                                                                                                                   |
| `unavailable`, `internal` | `EngineUnavailableError` (also no responders, a timeout)                                   | transient: `result()` asks again, inside an execution `done retry`; `internal` is logged at error                                                                                                                               |
| `tenant_limit`            | `LimitError` (`code`, `limit`: `bytes`, `runs`, `workflows`, `polls`, `rate`)              | **a refusal, never retried**: a client's call raises it; `worker.start()` raises it when its first poll is refused (`rate` aside); a refused poll is logged at error naming the limit and polled again after 30 s (`rate`: 1 s) |
| `licence_limit`           | `LimitError` (`limit`: `workflows`, `workers`, `runs`, `runsPerDay`, `tenants`, `expired`) | the same; inside an execution (a `child` at `runs`, an `append` at `bytes`) the item goes back (`done retry`), logged at error                                                                                                  |

A closed connection is `ConnectionClosedError`. An engine older than this SDK's floor (no `withdrawn-remembered` in `info.capabilities`) is `UnsupportedEngineError` (`code` `unsupported`, this SDK's, with `engine`, `commit`, `capabilities`, `missing`), never retried ([Which engine](#quickstart)). A code the SDK does not know (an engine newer than it) stays `EngineUnavailableError`, logged at warn. A worker whose polls fail (no engine, a slow link) says `poll failed; retrying` once a minute, with the count of failures since.

**Sizes:** a value of any size the blob store holds; one larger than a request (`request_max`, about `max_payload` − 16 KiB) costs a `put` round trip per part of about 6 MiB on an 8 MiB server. After `result()`, `status()` can read the previous state for a round trip of the worker's.

## Running a worker

The user's half of running it; the engine's operator has the other.

| Concern                            | What to do                                                                                                                                                          | Mechanism                                                                                                                                                                                               |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| the connection                     | connect with `nats.connect(url, **with_reconnect_defaults())`                                                                                                       | nats-py gives up after 60 reconnect attempts (~2 min) and closes the connection for good; the helper sets the limit to −1 and never overrides an option you set                                         |
| a connection closed for good       | restart the process; point a readiness probe at `worker.health()`                                                                                                   | a waiting `result()` raises `ConnectionClosedError` (the run is unaffected); the worker reports `running=False`, `connection_closed=True`                                                               |
| no engine, or a slow link          | nothing                                                                                                                                                             | `worker.start()` warns `no engine answered info; polling until one does`, and the first answered poll checks the engine's floor; a failing poll says `poll failed; retrying` once a minute with a count |
| a crash mid-step                   | make a step's effect idempotent, keyed by `ctx.key()`                                                                                                               | a finished step is journaled and never re-run; a step in flight runs again on the next worker (at-least-once)                                                                                           |
| a step longer than the ack wait    | nothing, up to the engine's `--max-work` (5 min by default)                                                                                                         | the worker `extend`s the lease every third of the ack wait; past `--max-work` the item redelivers. Raising it is the engine operator's flag                                                             |
| how soon a dead worker's run moves | the engine's `--ack-wait` (30 s by default)                                                                                                                         | the item redelivers once the lease lapses                                                                                                                                                               |
| plaintext values in the journal    | `payload_codec=aes_gcm_payload_codec(key)` on **every** worker and client of the prefix (32 bytes, from your secret store; `pip install 'synadia-durable[crypto]'`) | it transforms the payload slot only; the engine and the console see a run's shape and never a value. Not retroactive, one-way in practice, and the key's rotation is yours (the protocol's §5.7)        |
| who may start, signal or cancel    | the account's authorization (standalone), or your tenant's users ([on the fabric](../docs/FABRIC.md#standalone-and-the-fabric))                                     | every call is a request on `{prefix}.api.v1.<op>`                                                                                                                                                       |

## Testing

```sh
cd python
uv run ruff format --check . && uv run ruff check . && uv run mypy
uv run pytest                                  # the unit suites, then the integration suites
```

The unit suites need nothing. **The integration suites (`tests/integration/`) need an engine build from Synadia (`DE_ENGINE`, a path) and a licence (`DE_LICENCE`, a file)**; without an engine they skip, saying why. `../engine.pin` names the engine version they were last run against. They spawn a `nats-server` (≥ 2.14, from `PATH`) under the library's account-limits profile (`DE_TEST_PROFILE=ngs|tight|none`, default `ngs`) and one `durable-engine serve` per prefix a test uses, stopped after each test (`tests/engine.py`). Every standalone engine they start runs with `DE_LICENCE`, the existing-server mode's too. `test_limits` needs more: a licence with low limits of its own, and the engine's fabric build for tenancy mode (`DE_ENGINE_FABRIC`, a path); without them its two tests fail. `DE_ENGINE_LOG=1` echoes the engines' logs and `DE_SDK_LOG=1` gives a worker without a logger a console one. `DE_TEST_RTT_MS=<ms>` puts the delay proxy (`tests/netproxy.py`) between every test's connection and the server, the engines on the direct URL. The interop suites also need `npx` and the repo's `node_modules` (`bun install` at the root). Nothing reaches beyond 127.0.0.1 but in the modes below.

### Against an existing server

`DE_NATS_URL` runs the integration suites against a server the run does not own (`tests/existing.py`): no spawn and no account profile, one prefix and one `durable-engine serve` for the run, the test's writes purged after it rather than every stream deleted (a failed test's too, its runs' blobs by name), the leftovers check after every test, waits scaled by the measured round trip and the probed throughput (`existing.moving(n)`), and 19 tests skipped by name (`test_network` 16, `test_the_maintenance_pass_and_pumps_survive_a_deleted_timers_stream` 1, `test_limits` 2). The variables are `docs/DEVELOPING.md`'s ([Against an existing server](../docs/DEVELOPING.md#against-an-existing-server)), the same names and meanings:

| Variable         | Meaning                                                                                                                                       |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `DE_NATS_URL`    | the server; switches the mode on                                                                                                              |
| `DE_NATS_CREDS`  | its creds file (the clients, the engine's `--creds`, the interop children's `NATS_CREDS`)                                                     |
| `DE_PREFIX`      | the run's prefix (default a generated `de-<8 hex>`)                                                                                           |
| `DE_ENGINE_URL`  | an engine already serving the prefix, by its `/metrics` address; then none is started, and a test needing other flags or a restart is skipped |
| `DE_CLEANUP`     | `delete` or `keep` the prefix's eight at the end; by default a generated prefix is deleted and a `DE_PREFIX` one kept                         |
| `DE_TEST_RTT_MS` | as before: the delay proxy in front of the server, for the tests **and the engine**                                                           |
| `DE_TENANT`      | the tests run as this tenant of an engine in tenancy mode (`serve --tenancy`): below                                                          |
| `DE_ADMIN_URL`   | where the harness reads the prefix's streams and consumers (default `DE_NATS_URL`); as a tenant, the engine's account                         |
| `DE_ADMIN_CREDS` | its creds file (default `DE_NATS_CREDS` when `DE_ADMIN_URL` is unset)                                                                         |

`DE_INJECT_FAILURE=1` adds the two tests that fail on purpose (`test_injected_failure.py`). At the end the harness writes the prefix's streams, the engine's `/metrics` and its log to `../.de-run/<prefix>-py/`.

```sh
DE_NATS_URL=nats://127.0.0.1:14222 uv run pytest tests/integration -rs   # 78 passed, 22 skipped, ~60 s
```

Over a WAN, what takes the time and where to run the engine: `docs/DEVELOPING.md`'s [Against an existing server](../docs/DEVELOPING.md#against-an-existing-server).

### As a tenant

`DE_TENANT` runs the suites as a tenant of an engine in tenancy mode (`serve --tenancy`; the engine's `API.md`, "Tenancy"). The tests connect as the tenant's user, which holds no stream, so the harness reads the engine's account through `DE_ADMIN_URL` / `DE_ADMIN_CREDS` and counts and purges only the tenant's subjects and objects, and reports only its durables, never another tenant's or the eight. It needs `DE_PREFIX` and `DE_ENGINE_URL`, and refuses `DE_CLEANUP=delete`. A test that reads the engine's stores itself is skipped as a tenant, saying so (17; 16 beyond the `DE_ENGINE_URL` skips). Locally, on the three-account server of `docs/DEVELOPING.md`'s [As a tenant](../docs/DEVELOPING.md#as-a-tenant) (or [`../docs/FABRIC.md`](../docs/FABRIC.md#walk-it-locally)):

```sh
export DE_NATS_URL=nats://a-client:a-client@127.0.0.1:14322 DE_TENANT=A DE_PREFIX=de
export DE_ADMIN_URL=nats://de:de@127.0.0.1:14322 DE_ENGINE_URL=http://127.0.0.1:19391
uv run pytest tests/integration -rs              # 34 passed, 61 skipped, ~31 s
```

nats-py reads `user:password@` from a URL. A creds file needs `nkeys` next to `nats-py`; it is a dev dependency here.

## The interop worker

`conformance/worker.py` is the library's, on the thin worker, the twin of `../conformance/interop/ts-worker.ts`: `greet`, `approve`, `boom` and the parked `echo_signal`, with `DE_PAYLOAD_KEY` (hex) for encryption at rest. It needs a `durable-engine serve` on the same prefix and prints `READY`:

```sh
NATS_URL=nats://127.0.0.1:4222 DE_PREFIX=de uv run python conformance/worker.py
```

`tests/integration/test_interop.py` drives the TypeScript interop worker with the Python client, and `../test/integration/interop-py.test.ts` drives this one with the TypeScript client, each with and without `DE_PAYLOAD_KEY`.
