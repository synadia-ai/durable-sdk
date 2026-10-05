# `synadia-durable` (Python)

The thin durable execution SDK for Python, the twin of the TypeScript one at the root of this repo. It keeps the replay core of the library `synadia-durable` (`../synadia-durable-execution/python`, frozen at `7456c75`) and puts a request/reply client for [`durable-engine`](https://github.com/synadia-ai/durable-engine) behind its seams. The engine owns the streams. The public API is the library's: `@workflow` / `define_workflow`, `create_client`, `create_worker`, and `ctx.run/sleep/wait/call/start_call/race/now/random/uuid/continue_as_new/on_cancel`.

- **The only dependency is `nats-py`**, used for core request/reply alone: every call is one request to `{prefix}.api.v1.<op>` (`../../durable-engine/docs/API.md`). There are no consumers, streams or buckets here, and no JetStream client. `cryptography` is the optional `crypto` extra, for `aes_gcm_payload_codec`.
- **Version** `0.7.0a0` (the PEP 440 form of the TypeScript SDK's `0.7.0-alpha`). Unpublished.
- **What came from where:** [`../docs/ORIGIN.md`](../docs/ORIGIN.md). **What building it found:** [`../docs/findings.md`](../docs/findings.md) (F15 onwards are Python's).

## Quickstart

A local NATS, the engine next to it with a test licence, and the `charge` example:

```sh
nats-server -js &
(cd ../durable-engine && git checkout $(cat ../durable-sdk/engine.pin) && make build-dev)   # bin/durable-engine-dev, bin/durable-licence
../durable-engine/bin/durable-licence mint -test -name "$USER" -out de.licence           # a test licence: 30 days, generous limits
../durable-engine/bin/durable-engine-dev serve --nats nats://127.0.0.1:4222 --licence de.licence &
cd python && uv sync
NATS_URL=nats://127.0.0.1:4222 uv run python examples/charge.py
```

A standalone `serve` starts only with a valid licence (`--licence FILE` or `$DE_LICENCE`); a test licence is accepted by a `-dev` build alone (the engine's `docs/QUICKSTART.md`).

```python
import asyncio

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
    nc = await nats.connect("nats://127.0.0.1:4222", **with_reconnect_defaults())
    worker = create_worker(nc, [order])
    await worker.start()
    client = create_client(nc)
    handle = await client.start(order, {"cartId": "c-42"})
    await client.signal(handle.run_id, "payment", {"approved": True})
    print(await handle.result(20_000))
    await worker.stop()
    await nc.drain()


asyncio.run(main())
```

`create_worker` takes `nc`, `workflows`, `prefix`, `dependencies`, `concurrency` (default 16 poll slots), `poll_wait_ms` (default 30 000), `logger`, `metrics` and `payload_codec`. The library's other options are accepted and ignored, each the `serve` flag it became: `ack_wait_ms` → `--ack-wait`, `max_work_ms` → `--max-work`, `lease=False` → `--no-lease`, `lease_ttl_ms` → `--lease-ttl`, `stream_max_bytes` → `--stream-max-bytes`, `maintenance_interval_ms` → `--maintenance-interval`, `reconcile_grace_ms` → `--reconcile-grace`. A refusal from the engine is a typed error, as in TypeScript: `fenced` → `FencedError`, `expired` → `LeaseExpiredError`, `invalid` → `InvalidRequestError` (or `SerializationError` inside an execution), `too_large` → `PayloadTooLargeError`, `not_found` → `RunNotFoundError` (also a `LookupError`, as the library raised), `unavailable` / `internal` / no responders / a timeout → `EngineUnavailableError`, and `tenant_limit` / `licence_limit` → `LimitError` (`code`, and `limit` naming which: the engine's own word). A `LimitError` is a refusal, never retried (`../docs/findings.md` F40): a client's call raises it, `worker.start()` raises it when its first poll is refused (`rate`, which clears within a second, aside), a refused poll is logged at error naming the limit and polled again after 30 s (`rate`: 1 s), and inside an execution the item goes back (`done retry`), logged at error. The codes are mirrored as `ENGINE_ERROR_CODES` (`_engine/errors.py`), checked against the engine's table at `engine.pin` by `tests/test_engine_errors.py`. A worker whose polls fail says `poll failed; retrying` once a minute, with the count of failures since (F41).

`offload`'s threshold is `serve --offload-threshold` now; `offload=False` keeps its meaning (nothing goes up in parts, a value too large for a request fails loud). Every poll names the worker and `stop()` withdraws its waiting polls. **Sizes:** a value larger than one request goes up in `put` parts and one larger than a reply comes down in `get` parts (`_engine/blobs.py`, `docs/findings.md` F1, F26); after `result()`, `status()` can read the previous state for a round trip of the worker's (F27).

## The examples

`charge`, `saga`, `crash_replay`, `fan_out` and `cancel`, the library's, unchanged: [`examples/README.md`](examples/README.md).

## Testing

```sh
cd python
uv run ruff format --check . && uv run ruff check . && uv run mypy
uv run pytest                                  # the unit suites, then the integration suites
```

The unit suites need nothing. The integration suites (`tests/integration/`) spawn a `nats-server` (≥ 2.14, from `PATH`) under the library's account-limits profile (`DE_TEST_PROFILE=ngs|tight|none`, default `ngs`) and one `durable-engine serve` per prefix a test uses, stopped after each test (`tests/engine.py`). The engine binary is `DE_ENGINE` (a path), or is built as the engine's `make build-dev` builds it (`-tags devlicence`, and `durable-licence` beside it) into `../.engine-bin/` from `../../durable-engine` (`DE_ENGINE_REPO`) at the commit in `../engine.pin`; a checkout at another commit, or with local changes, is refused unless `DE_ENGINE_ANY=1`. A standalone `serve` starts only with a licence: the session mints a test licence once (`durable-licence mint -test -name durable-sdk-tests`) and starts every standalone engine with `DE_LICENCE` set to it; `DE_LICENCE` in the caller's environment wins, and the root README has the rest (F42). Without the binary or Go the integration suites skip, saying why. `DE_ENGINE_LOG=1` echoes the engines' logs and `DE_SDK_LOG=1` gives a worker without a logger a console one. `DE_TEST_RTT_MS=<ms>` puts the delay proxy (`tests/netproxy.py`) between every test's connection and the server, the engines on the direct URL (`docs/findings.md` F28). The interop suites also need `npx` and the repo's `node_modules` (`bun install` at the root). Nothing reaches beyond 127.0.0.1 but in the mode below.

`DE_NATS_URL` runs the integration suites against a server the run does not own: one prefix (`DE_PREFIX`, default a generated `de-<8 hex>`) and one `serve` for the run, the creds of `DE_NATS_CREDS`, the test's writes purged after it rather than every stream deleted (a failed test's too, its runs' blobs by name), waits scaled by the measured round trip, a generated prefix deleted at the end unless `DE_CLEANUP=keep` (a `DE_PREFIX` one kept unless `DE_CLEANUP=delete`), and 19 tests skipped by name (`tests/existing.py`; the root README's "Against an existing server" has the variables, the measured times and the dev NGS command lines; `../docs/findings.md` F29, F33, F34):

```sh
DE_NATS_URL=nats://127.0.0.1:14222 uv run pytest tests/integration -rs   # 73 passed, 22 skipped
```

A creds file needs `nkeys` next to `nats-py` (`pip install 'nats-py[nkeys]'`); it is a dev dependency here (F30).

The Python interop worker, the twin of `../conformance/interop/ts-worker.ts`, needs a `serve` on the same prefix and prints `READY`:

```sh
NATS_URL=nats://127.0.0.1:4222 DE_PREFIX=de uv run python conformance/worker.py
```
