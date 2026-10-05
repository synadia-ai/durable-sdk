# `synadia-durable` (Python)

The thin durable execution SDK for Python, the twin of the TypeScript one at the root of this repo. It keeps the replay core of the library `synadia-durable` (`../synadia-durable-execution/python`, frozen at `7456c75`): workflows as ordinary functions over a `ctx`, every side-effecting or nondeterministic step journaled and replayed after a crash, the determinism guards, the out-of-band halts. Behind the core's seams sits a request/reply client for [`durable-engine`](https://github.com/synadia-ai/durable-engine), which owns the streams. A customer's workflow functions run in the customer's process; nothing runs without the engine.

**It runs in two places, with the same code:**

| Where                           | The engine                                                        | Start here                               |
| ------------------------------- | ----------------------------------------------------------------- | ---------------------------------------- |
| **standalone**                  | yours, next to your NATS, with a licence                          | the [Quickstart](#quickstart) below      |
| **on the Synadia Agent Fabric** | ours, in the DE account; your tenant reaches it through an import | [`../docs/FABRIC.md`](../docs/FABRIC.md) |

- **The public API** is the library's: `@workflow` / `define_workflow`, `create_client`, `create_worker`, and `ctx.run/sleep/wait/call/start_call/race/now/random/uuid/key/continue_as_new/on_cancel`.
- **The only dependency is `nats-py`**, used for core request/reply alone: every call is one request to `{prefix}.api.v1.<op>` (`../../durable-engine/docs/API.md`). There are no consumers, streams or buckets here, and no JetStream client. `cryptography` is the optional `crypto` extra, for `aes_gcm_payload_codec`.
- **Documentation** is this README, [`../docs/FABRIC.md`](../docs/FABRIC.md) (on the fabric), the [examples](examples/README.md), [`../docs/findings.md`](../docs/findings.md) (what building it found; F15 onwards are Python's) and [`../docs/ORIGIN.md`](../docs/ORIGIN.md) (which file came from where). The engine's operator has the engine's `docs/QUICKSTART.md` and `docs/OPERATIONS.md`; the protocol stays in the frozen library (`../../synadia-durable-execution/docs/PROTOCOL.md`).

**Status:** `0.7.0a0` (the PEP 440 form of the TypeScript SDK's `0.7.0-alpha`; board row P2-19b). Unpublished.

## Quickstart

A local NATS, the engine next to it with a test licence, and the `charge` example. You need `nats-server` ≥ 2.14 on `PATH`, Go ≥ 1.26 (to build the engine), Python ≥ 3.11 with [`uv`](https://docs.astral.sh/uv/), and [`durable-engine`](https://github.com/synadia-ai/durable-engine) checked out beside this repo. From this repo's root:

```sh
nats-server -js &
(cd ../durable-engine && git checkout $(cat ../durable-sdk/engine.pin) && make build-dev)   # bin/durable-engine-dev, bin/durable-licence
../durable-engine/bin/durable-licence mint -test -name "$USER" -out de.licence           # a test licence: 30 days, generous limits
../durable-engine/bin/durable-engine-dev serve --nats nats://127.0.0.1:4222 --licence de.licence &
cd python && uv sync
NATS_URL=nats://127.0.0.1:4222 uv run python examples/charge.py
```

The `git checkout` leaves the engine's checkout detached at the pin; `git checkout main` there afterwards. A standalone `serve` starts only with a valid licence (`--licence FILE` or `$DE_LICENCE`; the engine's `docs/QUICKSTART.md`, `docs/API.md` "The licence"); a test licence is accepted by a `-dev` build alone. The engine logs `licence … licensee=<you>`, provisions its seven resources at start and answers on `de.api.v1.*`. Check it with `nats req de.api.v1.info '{}'`. The example ends with `result: {'status': 'shipped', 'order_id': 'ord-c-42', 'tracking': 'TRK-c-42'}` and `status: RunStatus(state='completed', …)`. Stop the engine and the server with `kill %2 %1`.

**Which engine:** this SDK needs the engine at or after its #13 (`16cb3da`, the pin), which announces `withdrawn-remembered` in `info.capabilities` (a worker withdrawn by `stop()` is remembered 5 s, so `stop()` sends one `cancelPoll`; the engine's F53, F57). `worker.start()` raises `UnsupportedEngineError` on an older one, naming the engine by its `engine` and `commit` as `info` gives them; a worker whose start found no engine checks at its first answered poll and stops there, logged at error (`health()` says stopped). The floor is read from `info.capabilities`, never from the version: every build so far says `0.2.0-dev` (`../docs/findings.md` F45).

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

### Using it from your own project

The package is unpublished (it publishes with the fabric SDK). Until then, install it from the checkout:

```sh
uv add ../durable-sdk/python            # in your project (pip: pip install -e ../durable-sdk/python)
uv run python order.py                  # the file above
```

`nats-py` comes with it; a creds file needs `nkeys` beside it (`pip install 'nats-py[nkeys]'`; `../docs/findings.md` F30).

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

The worker is the API's loop: one `poll` per concurrency slot; for each item the journal (paged with `load`), the library's `execute_run` with its seams as `append`, `wake` and `child` under the item's lease, `extend` every third of the ack wait, then `done`; every poll names the worker, and `stop()` withdraws its waiting polls (`poll {cancelPoll}`), so a stop takes as long as the items in flight; it returns once every poll has returned, an item one was handed in the instant before handed back (`done retry`), so a process may exit right after it (`docs/findings.md` F44). Stop the worker in a `finally`, as the examples do: a process that exits on an error without `stop()` leaves its polls to the engine for their wait, and an item handed to one waits out its ack wait. A value too large for one request goes up in `put` parts and travels as a ref; a ref the engine hands back, for a value too large for a reply, comes down in `get` parts, its digest checked (`_engine/blobs.py`). Everything else the library's worker did is the engine's: the consumers, provisioning, the inbound event, the run lease, the ack matrix, `child-done`, the cascade of a cancel, the forward across continue-as-new, the parked index and the lost-wake repair, the maintenance pass, offload.

So `create_worker` takes `nc`, `workflows`, `prefix`, `dependencies`, `concurrency` (default 16 poll slots), `poll_wait_ms` (default 30 000), `logger`, `metrics` and `payload_codec`. The library's other options are accepted and ignored, each named for the `serve` flag it became: `ack_wait_ms` → `--ack-wait`, `max_work_ms` → `--max-work`, `lease=False` → `--no-lease`, `lease_ttl_ms` → `--lease-ttl`, `stream_max_bytes` → `--stream-max-bytes`, `maintenance_interval_ms` → `--maintenance-interval`, `reconcile_grace_ms` → `--reconcile-grace`, and `offload`'s threshold → `--offload-threshold`; `offload=False` keeps its meaning (nothing goes up in parts, a value too large for a request fails loud). `create_client` takes `nc`, `prefix`, `payload_codec`, `logger`, `metrics` (`blob.put`, `blob.get`) and `offload=False`.

### Errors

A refusal from the engine is a typed error, one per code of the engine's closed set (`../../durable-engine/docs/API.md` "Errors"; mirrored as `ENGINE_ERROR_CODES` in `src/synadia_durable/_engine/errors.py` and `../src/engine/errors.ts`, and a unit test in each language, `tests/test_engine_errors.py` here, fails when the engine's table at `engine.pin` has a code the mirror lacks):

| `code`                    | Error                                                                                      | What the SDK does with it                                                                                                                                                                                                             |
| ------------------------- | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `invalid`                 | `InvalidRequestError` (inside an execution `SerializationError`)                           | never retried; inside an execution the run fails                                                                                                                                                                                      |
| `fenced`                  | `FencedError` (`EngineFencedError`, with `expect_last_seq` and `server_code`)              | the pure core's halt                                                                                                                                                                                                                  |
| `expired`                 | `LeaseExpiredError`                                                                        | the worker drops the item; it redelivers                                                                                                                                                                                              |
| `not_found`               | `RunNotFoundError` (also a `LookupError`, as the library raised)                           | raised to the caller                                                                                                                                                                                                                  |
| `too_large`               | `PayloadTooLargeError`                                                                     | deterministic: raised, or the run fails                                                                                                                                                                                               |
| `blob_gone`               | `BlobMissingError` / `BlobIntegrityError`                                                  | the worker parks the run, loud (`done retry`)                                                                                                                                                                                         |
| `unavailable`, `internal` | `EngineUnavailableError` (also no responders, a timeout)                                   | transient: `result()` asks again, inside an execution `done retry`; `internal` is logged at error                                                                                                                                     |
| `tenant_limit`            | `LimitError` (`code`, `limit`: `bytes`, `runs`, `workflows`, `polls`, `rate`)              | **a refusal, never retried** (F40): a client's call raises it; `worker.start()` raises it when its first poll is refused (`rate` aside); a refused poll is logged at error naming the limit and polled again after 30 s (`rate`: 1 s) |
| `licence_limit`           | `LimitError` (`limit`: `workflows`, `workers`, `runs`, `runsPerDay`, `tenants`, `expired`) | the same; inside an execution (a `child` at `runs`, an `append` at `bytes`) the item goes back (`done retry`), logged at error                                                                                                        |

A closed connection is `ConnectionClosedError`. An engine older than this SDK's floor (no `withdrawn-remembered` in `info.capabilities`) is `UnsupportedEngineError` (`code` `unsupported`, this SDK's, with `engine`, `commit`, `capabilities`, `missing`), never retried ([Which engine](#quickstart)). A code the SDK does not know (an engine newer than it) stays `EngineUnavailableError`, logged at warn. A worker whose polls fail (no engine, a slow link) says `poll failed; retrying` once a minute, with the count of failures since (F41).

**Sizes:** a value of any size the blob store holds; one larger than a request (`request_max`, about `max_payload` − 16 KiB) costs a `put` round trip per part of about 6 MiB on an 8 MiB server (`../docs/findings.md` F1, F26). After `result()`, `status()` can read the previous state for a round trip of the worker's (F27).

## Running a worker

The user's half of running it; the engine's operator has the other (the engine's `docs/OPERATIONS.md`). The library's `docs/OPERATIONS.md` held both; what is the engine's now is named where it went.

| Concern                            | What to do                                                                                                                                                          | Mechanism                                                                                                                                                                                                                         |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| the connection                     | connect with `nats.connect(url, **with_reconnect_defaults())`                                                                                                       | nats-py gives up after 60 reconnect attempts (~2 min) and closes the connection for good; the helper sets the limit to −1 and never overrides an option you set                                                                   |
| a connection closed for good       | restart the process; point a readiness probe at `worker.health()`                                                                                                   | a waiting `result()` raises `ConnectionClosedError` (the run is unaffected); the worker reports `running=False`, `connection_closed=True`                                                                                         |
| no engine, or a slow link          | nothing                                                                                                                                                             | `worker.start()` warns `no engine answered info; polling until one does`, and the first answered poll checks the engine's floor (F45); a failing poll says `poll failed; retrying` once a minute with a count (F41)               |
| a crash mid-step                   | make a step's effect idempotent, keyed by `ctx.key()`                                                                                                               | a finished step is journaled and never re-run; a step in flight runs again on the next worker (at-least-once)                                                                                                                     |
| a step longer than the ack wait    | nothing, up to the engine's `--max-work` (5 min by default)                                                                                                         | the worker `extend`s the lease every third of the ack wait; past `--max-work` the item redelivers. Raising it is the engine operator's flag                                                                                       |
| how soon a dead worker's run moves | the engine's `--ack-wait` (30 s by default)                                                                                                                         | the item redelivers once the lease lapses                                                                                                                                                                                         |
| plaintext values in the journal    | `payload_codec=aes_gcm_payload_codec(key)` on **every** worker and client of the prefix (32 bytes, from your secret store; `pip install 'synadia-durable[crypto]'`) | it transforms the payload slot only; the engine and the console see a run's shape and never a value. Not retroactive, one-way in practice, and the key's rotation is yours (the library's `docs/OPERATIONS.md` "Payload privacy") |
| who may start, signal or cancel    | the account's authorization (standalone), or your tenant's users ([on the fabric](../docs/FABRIC.md#standalone-and-the-fabric))                                     | every call is a request on `{prefix}.api.v1.<op>`                                                                                                                                                                                 |

## Testing

```sh
cd python
uv run ruff format --check . && uv run ruff check . && uv run mypy
uv run pytest                                  # the unit suites, then the integration suites
```

The unit suites need nothing. The integration suites (`tests/integration/`) spawn a `nats-server` (≥ 2.14, from `PATH`) under the library's account-limits profile (`DE_TEST_PROFILE=ngs|tight|none`, default `ngs`) and one `durable-engine serve` per prefix a test uses, stopped after each test (`tests/engine.py`). The engine binary is `DE_ENGINE` (a path), or is built from `../../durable-engine` (`DE_ENGINE_REPO`) at the commit in `../engine.pin` as the engine's `make build-dev` builds it, into `../.engine-bin/`: `durable-engine-dev-<commit>` (`-tags devlicence`, trusting the test licence issuer) and `durable-licence-<commit>`. A checkout at another commit, or with local changes, is refused unless `DE_ENGINE_ANY=1`. **The licence** (F42): a standalone `serve` starts only with one, so the session mints a test licence once (`durable-licence mint -test -name durable-sdk-tests`, into a temporary directory removed at the end) and starts every standalone engine with `DE_LICENCE` set to it, the existing-server mode's too; `DE_LICENCE` in the caller's environment wins. The tool is the one built beside the engine, or `bin/durable-licence` beside a `DE_ENGINE`, or `DE_LICENCE_TOOL`. An engine in the fabric mode (`serve --tenancy`, `test_limits`'s) gets none. Without the binary or Go the integration suites skip, saying why. `DE_ENGINE_LOG=1` echoes the engines' logs and `DE_SDK_LOG=1` gives a worker without a logger a console one. `DE_TEST_RTT_MS=<ms>` puts the delay proxy (`tests/netproxy.py`) between every test's connection and the server, the engines on the direct URL (`../docs/findings.md` F28). The interop suites also need `npx` and the repo's `node_modules` (`bun install` at the root). Nothing reaches beyond 127.0.0.1 but in the modes below.

**Two builds of the engine since its #9 (`engine.pin` `97a1778`):** the harness builds `-tags devlicence` for the standalone engines it spawns (a test licence, F42) and **`-tags fabric` for a spawn in tenancy mode** (`serve --tenancy`: the fabric mode exists only in that build, the engine's F44, and it takes no licence), both into `../.engine-bin/` from the pinned checkout. With `DE_ENGINE` given, a tenancy-mode spawn needs `DE_ENGINE_FABRIC` beside it, or the checkout at the pin to build from; the tenancy tests fail loud otherwise (F43).

### Against an existing server

`DE_NATS_URL` runs the integration suites against a server the run does not own (`tests/existing.py`; `../docs/findings.md` F29): no spawn and no account profile, one prefix and one `durable-engine serve` for the run, the test's writes purged after it rather than every stream deleted (a failed test's too, its runs' blobs by name), the leftovers check after every test, waits scaled by the measured round trip and the probed throughput (`existing.moving(n)`, F37), and 19 tests skipped by name (`test_network` 16, `test_the_maintenance_pass_and_pumps_survive_a_deleted_timers_stream` 1, `test_limits` 2). The variables are the root README's ([Against an existing server](../README.md#against-an-existing-server)), the same names and meanings:

| Variable         | Meaning                                                                                                                                       |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `DE_NATS_URL`    | the server; switches the mode on                                                                                                              |
| `DE_NATS_CREDS`  | its creds file (the clients, the engine's `--creds`, the interop children's `NATS_CREDS`)                                                     |
| `DE_PREFIX`      | the run's prefix (default a generated `de-<8 hex>`)                                                                                           |
| `DE_ENGINE_URL`  | an engine already serving the prefix, by its `/metrics` address; then none is started, and a test needing other flags or a restart is skipped |
| `DE_CLEANUP`     | `delete` or `keep` the prefix's seven at the end; by default a generated prefix is deleted and a `DE_PREFIX` one kept (F34)                   |
| `DE_TEST_RTT_MS` | as before: the delay proxy in front of the server, for the tests **and the engine**                                                           |
| `DE_TENANT`      | the tests run as this tenant of an engine in tenancy mode (`serve --tenancy`): below                                                          |
| `DE_ADMIN_URL`   | where the harness reads the prefix's streams and consumers (default `DE_NATS_URL`); as a tenant, the engine's account                         |
| `DE_ADMIN_CREDS` | its creds file (default `DE_NATS_CREDS` when `DE_ADMIN_URL` is unset)                                                                         |

`DE_INJECT_FAILURE=1` adds the two tests that fail on purpose (`test_injected_failure.py`). At the end the harness writes the prefix's streams, the engine's `/metrics` and its log to `../.de-run/<prefix>-py/`.

```sh
DE_NATS_URL=nats://127.0.0.1:14222 uv run pytest tests/integration -rs   # 78 passed, 22 skipped, ~60 s
```

The dev NGS run's command lines and the measured times of both languages (a local server, the delay proxy, dev NGS) are in the root README's table ([Against an existing server](../README.md#against-an-existing-server); F33).

### As a tenant

`DE_TENANT` runs the suites as a tenant of an engine in tenancy mode (`serve --tenancy`; the engine's `docs/API.md` "Tenancy"; `../docs/findings.md` F38). The tests connect as the tenant's user, which holds no stream, so the harness reads the engine's account through `DE_ADMIN_URL` / `DE_ADMIN_CREDS` and counts, purges and sweeps only the tenant's subjects, objects and durables, never another tenant's or the seven. It needs `DE_PREFIX` and `DE_ENGINE_URL`, and refuses `DE_CLEANUP=delete`. A test that reads the engine's stores itself is skipped as a tenant, saying so (17; 16 beyond the `DE_ENGINE_URL` skips). Locally, on the three-account server of the root README's [As a tenant](../README.md#as-a-tenant) (or [`../docs/FABRIC.md`](../docs/FABRIC.md#walk-it-locally)):

```sh
export DE_NATS_URL=nats://a-client:a-client@127.0.0.1:14322 DE_TENANT=A DE_PREFIX=de
export DE_ADMIN_URL=nats://de:de@127.0.0.1:14322 DE_ENGINE_URL=http://127.0.0.1:19391
uv run pytest tests/integration -rs              # 34 passed, 61 skipped, ~31 s
```

nats-py reads `user:password@` from a URL. A creds file needs `nkeys` next to `nats-py`; it is a dev dependency here (F30).

## The interop worker

`conformance/worker.py` is the library's, on the thin worker, the twin of `../conformance/interop/ts-worker.ts`: `greet`, `approve`, `boom` and the parked `echo_signal`, with `DE_PAYLOAD_KEY` (hex) for encryption at rest. It needs a `durable-engine serve` on the same prefix and prints `READY`:

```sh
NATS_URL=nats://127.0.0.1:4222 DE_PREFIX=de uv run python conformance/worker.py
```

`tests/integration/test_interop.py` drives the TypeScript interop worker with the Python client, and `../test/integration/interop-py.test.ts` drives this one with the TypeScript client, each with and without `DE_PAYLOAD_KEY`.
