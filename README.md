# durable-sdk

The thin durable execution SDKs, TypeScript and Python. Each keeps the replay core of the library `@synadia-ai/durable` (`synadia-ai/durable-ts`, frozen at `7456c75`): workflows as ordinary functions over a `ctx`, every side-effecting or nondeterministic step journaled and replayed after a crash, the determinism guards, the out-of-band halts. Behind the core's three seams (`Journal`, `Scheduler`, `Children`) and the client's five calls sits a request/reply client for [`durable-engine`](https://github.com/synadia-ai/durable-engine), which owns the streams. A customer's workflow functions run in the customer's process; nothing runs without the engine.

Private until published. Created 2026-10-03 on the Synadia Agent Fabric's decision 61 (`synadia-agent-fabric-docs`, `DECISIONS.md`; the research `docs/research/2026-10-03--durable-execution-as-a-product.md`).

- **The public API** is the library's: `defineWorkflow`, `createClient`, `createWorker`, `ctx.run/sleep/await/call/now/random/uuid/continueAsNew/onCancel`. The library's integration suites are this repo's acceptance suites, run against the engine.
- **The only dependency** is `@nats-io/nats-core`: every call is one request to `{prefix}.api.v1.<op>` (`../durable-engine/docs/API.md`). There are no consumers, streams or buckets here, and no JetStream client.
- **Documentation** is this README, the [examples](examples/), [`docs/findings.md`](docs/findings.md) (what building it found) and [`docs/ORIGIN.md`](docs/ORIGIN.md) (which file came from where), kept minimal on purpose. The protocol stays in the frozen library (`../synadia-durable-execution/docs/PROTOCOL.md`).

**Status:** the TypeScript SDK, `0.7.0-alpha` (board row P2-19a), and the Python SDK in [`python/`](python/), `0.7.0a0` (P2-19b), with its own [README](python/README.md).

## Quickstart

A local NATS, the engine next to it with a test licence, and the `charge` example:

```sh
nats-server -js &
(cd ../durable-engine && git checkout $(cat ../durable-sdk/engine.pin) && make build-dev)   # bin/durable-engine-dev, bin/durable-licence
../durable-engine/bin/durable-licence mint -test -name "$USER" -out de.licence           # a test licence: 30 days, generous limits
../durable-engine/bin/durable-engine-dev serve --nats nats://127.0.0.1:4222 --licence de.licence &
bun install
NATS_URL=nats://127.0.0.1:4222 npm run example:charge
```

A standalone `serve` starts only with a valid licence (`--licence FILE` or `$DE_LICENCE`; the engine's `docs/QUICKSTART.md`, `docs/API.md` "The licence"); a test licence is accepted by a `-dev` build alone. The engine provisions its seven resources at start and answers on `de.api.v1.*`. Check it with `nats req de.api.v1.info '{}'`.

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

### Errors

A refusal from the engine is a typed error, one per code of the engine's closed set (`../durable-engine/docs/API.md` "Errors"; mirrored as `ENGINE_ERROR_CODES` in `src/engine/errors.ts` and `python/src/synadia_durable/_engine/errors.py`, and a unit test in each language fails when the engine's table at `engine.pin` has a code the mirror lacks):

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

A closed connection is `ConnectionClosedError`. A code the SDK does not know (an engine newer than it) stays `EngineUnavailableError`, logged at warn. A worker whose polls fail (no engine, a slow link) says `poll failed; retrying` once a minute, with the count of failures since (F41).

**Sizes:** a value of any size the blob store holds; one larger than a request (`request_max`, about `max_payload` − 16 KiB) costs a `put` round trip per part of about 6 MiB on an 8 MiB server (`docs/findings.md` F1, F26). After `result()`, `status()` can read the previous state for a round trip of the worker's (F27).

## Testing

```sh
bun install && npm test            # the 15 unit suites: the pure core, the transport and the errors (no server)
npm run test:integration           # 9 of the library's integration suites, interop-py, metrics and limits, on durable-engine serve
```

The integration suites spawn a `nats-server` (≥ 2.14, from `PATH`) under the library's account-limits profile (`DE_TEST_PROFILE=ngs|tight|none`, default `ngs`), and one `durable-engine serve` per prefix a test uses, stopped after each test. The engine binary is `DE_ENGINE` (a path), or is built from `../durable-engine` (`DE_ENGINE_REPO`) at the commit in `engine.pin` as the engine's `make build-dev` builds it, into this repo: `.engine-bin/durable-engine-dev-<commit>` (`-tags devlicence`, trusting the test licence issuer) and `.engine-bin/durable-licence-<commit>`. A checkout at another commit, or with local changes, is refused unless `DE_ENGINE_ANY=1`. **The licence** (F42): a standalone `serve` starts only with one, so the harness mints a test licence once per run (`durable-licence mint -test -name durable-sdk-tests`, into a temporary directory removed at the end) and starts every standalone engine with `DE_LICENCE` set to it, the existing-server mode's too; `DE_LICENCE` in the caller's environment wins. The tool is the one built beside the engine, or `bin/durable-licence` beside a `DE_ENGINE` (`make build-dev` puts both there), or `DE_LICENCE_TOOL`. An engine in the fabric mode (`serve --tenancy`, the `limits` suite's) gets none: it needs none, and one would bound its tenants. Without the binary or Go the suites skip, saying why. `DE_ENGINE_LOG=1` echoes the engines' logs, and `DE_SDK_LOG=1` gives a worker without a logger a console one. `DE_TEST_RTT_MS=<ms>` puts the library's delay proxy between every test's connection and the server (the engines stay on the direct URL); `docs/findings.md` F28 has the runs at 150 and 300 ms. Nothing reaches beyond 127.0.0.1 but in the mode below.

**Two builds of the engine since its #9 (`engine.pin` `97a1778`):** the harness builds `-tags devlicence` for the standalone engines it spawns (a test licence, F42) and **`-tags fabric` for a spawn in tenancy mode** (`serve --tenancy`: the fabric mode exists only in that build, the engine's F44, and it takes no licence), both into `.engine-bin/` from the pinned checkout. With `DE_ENGINE` given, a tenancy-mode spawn needs `DE_ENGINE_FABRIC` beside it, or the checkout at the pin to build from; the tenancy tests fail loud otherwise.

`npm run check` adds the typecheck, lint and formatting.

### Against an existing server

`DE_NATS_URL` points both suites at a server the run does not own (`test/harness/existing.ts`, `python/tests/existing.py`; `docs/findings.md` F29): no spawn and no account profile, **one prefix and one `durable-engine serve` for the run**, started by the harness on the prefix (it provisions the seven; an account too small for them fails the start with the engine's own message). A test whose engine flags (`ackWaitMs`, `lease: false`, …) differ from the running engine's restarts it with them; `streamMaxBytes` is never applied. After each test, a failed or timed-out one too, the harness stops the workers the test left running, withdraws its workflows' polls (`cancelPoll`), purges what the test changed under the prefix, subject by subject (journal, work, timers, the KV keys, the blobs), deletes the durables made for the test's workflow names, and deletes its runs' blobs by name; at the end, with the engine stopped, it deletes every blob of the run's runs and every durable the run made (`docs/findings.md` F33). It never deletes a stream during the run or touches anything outside the prefix. An assertion over a store's contents counts the test's own runs only. The leftovers check runs after every test, on the prefix's streams, counting only consumers that were not there before the test. Every wait (the test and hook timeouts, a handle's `result()`, the engine's start) is scaled by a factor from the round trip measured at the start, printed once: 1 up to 20 ms, then 1 + RTT/100 (2.52 at the proxy's 150 ms). **A large value's bytes are waited for too** (`docs/findings.md` F37): at the start the harness also measures the throughput, a 4 MiB `put` and `get` through the run's engine on the tests' own link (`throughput 60.55 MiB/s …`, printed next to the round trip), and every test that moves values over the inline limit adds to its waits its bytes over that throughput, × 2 (`moving(n)` / `budget(ms, n)` in `test/harness/mode.ts`, `existing.moving(n)` in Python), n being the `max_payload`s it moves through `put` and `get` in all, from the harness's tally (`.de-run/<prefix>/blob-bytes.tsv`, a line per test). The round-trip factor scales the sum. The whole `offload` suite, the five large `payload-codec` cases, and Python's two large-signal `test_payload_codec_e2e` cases carry it; on a spawned server it is 0. The tests that need a server or a link of their own are skipped, their titles saying why: TypeScript `network` (the 16 of its 17 that run; the 17th was already skipped), `worker-recovery` "a broken timers stream" (1) and `limits` (2: an engine of its own, with its own licence or tenant limits); Python `test_network` (16, likewise), `test_the_maintenance_pass_and_pumps_survive_a_deleted_timers_stream` (1) and `test_limits` (2).

| Variable         | Meaning                                                                                                                                       |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `DE_NATS_URL`    | the server; switches the mode on                                                                                                              |
| `DE_NATS_CREDS`  | its creds file (the clients, the engine's `--creds`, the interop children's `NATS_CREDS`)                                                     |
| `DE_PREFIX`      | the run's prefix (default a generated `de-<8 hex>`)                                                                                           |
| `DE_ENGINE_URL`  | an engine already serving the prefix, by its `/metrics` address; then none is started, and a test needing other flags or a restart is skipped |
| `DE_CLEANUP`     | `delete` or `keep` the prefix's seven at the end; by default a generated prefix is deleted and a `DE_PREFIX` one kept                         |
| `DE_TEST_RTT_MS` | as before: the delay proxy in front of the server, for the tests **and the engine**                                                           |
| `DE_TENANT`      | the tests run as this tenant of an engine in tenancy mode (`serve --tenancy`): below                                                          |
| `DE_ADMIN_URL`   | where the harness reads the prefix's streams and consumers (default `DE_NATS_URL`); as a tenant, the engine's account                         |
| `DE_ADMIN_CREDS` | its creds file (default `DE_NATS_CREDS` when `DE_ADMIN_URL` is unset)                                                                         |

A generated prefix is only ever the run's, so its seven are deleted at the end unless `DE_CLEANUP=keep` (kept, each reserves its `max_bytes`, 1 GiB by default, and a local server's store fills after a handful of runs; F34). A `DE_PREFIX` is kept unless `DE_CLEANUP=delete`. `DE_INJECT_FAILURE=1` adds the tests that fail on purpose (two in each language, `injected-failure.test.ts` and `test_injected_failure.py`, and in TypeScript a passing one after them), to see that a failed or timed-out test leaves nothing behind. At the end the harness prints the prefix's streams (messages, bytes, consumers) and writes them, the engine's `/metrics` and its log to `.de-run/<prefix>/` (Python: `.de-run/<prefix>-py/`).

Locally, against a server started by hand:

```sh
nats-server -js -a 127.0.0.1 -p 14222 -sd /tmp/de-js &
export DE_NATS_URL=nats://127.0.0.1:14222      # a generated prefix each run, deleted at the end
npm run test:integration                        # 120 passed, 23 skipped, ~75 s
DE_TEST_RTT_MS=150 npm run test:integration     # every round trip 150 ms, the engine's too
(cd python && uv run pytest tests/integration -rs)   # 73 passed, 22 skipped, ~56 s
```

**The dev NGS run** (the coordinator's, on Mario's word; nothing here runs it):

```sh
# at the PR's head, e.g. ../wt/durable-sdk; the engine built from a clean checkout of engine.pin
export DE_NATS_URL=tls://connect.ngs.global
export DE_NATS_CREDS=<agent-fabric-tests>/local/ngs/creds/tenant-a-dev/<user>.creds
export DE_PREFIX=de                          # the seven exist there: the engine's start reconciles, creates none
export DE_ENGINE=$PWD/.engine-bin/durable-engine-dev-97a17781cfe3   # its durable-licence-97a17781cfe3 beside it mints the run's licence
npm run test:integration 2>&1 | tee ngs-ts.log
(cd python && uv run pytest tests/integration -rs 2>&1 | tee ../ngs-py.log)
```

No `DE_TEST_RTT_MS` (the latency is real; a byte proxy cannot carry TLS anyway), and no `DE_CLEANUP=delete` (the seven are the tenant's; a given prefix is kept by default). Expect TypeScript 120 passed and 23 skipped, Python 73 passed and 22 skipped (the 3 and 2 skipped above the 20 are the injected failures). The time is the round trip's and, over a WAN, the offload suites': measured, not predicted (`docs/findings.md` F33):

| Run                                                                                         | Round trip | Throughput (moved, up and down)                      | TypeScript (of it `offload` + `payload-codec`)   | Python (of it `test_offload` + `test_payload_codec_e2e`) |
| ------------------------------------------------------------------------------------------- | ---------- | ---------------------------------------------------- | ------------------------------------------------ | -------------------------------------------------------- |
| plain, a local server (`a7c5010`, this branch)                                              | 0.0 ms     | 64.5 MiB/s, probed                                   | 75 s                                             | 56 s                                                     |
| plain, a local server (`d3e741d`)                                                           | 0.0 ms     | 60.6 MiB/s, probed                                   | 70 s (9.5 s)                                     | 56 s                                                     |
| the delay proxy, a local server (`ee81460`)                                                 | 155 ms     | not probed                                           | 22.8 min (8.2 min)                               | 12.6 min (2.7 min)                                       |
| dev NGS, `tenant-a-dev`, from a laptop (`57ae3c9`)                                          | 51 ms      | not probed                                           | 28.4 min (12.4 min), 10 failed                   | 15.6 min, 5 failed                                       |
| dev NGS, `tenant-a-dev`, from a laptop, `offload` alone (the engine's blob path fixed; F37) | 51 ms      | ≈ 1.19 MiB/s (a test's 96 MiB in 80.9 s; not probed) | `offload` 7 min, 11 tests, 1 failed (its budget) | —                                                        |

`offload` and `payload-codec` move each large value as 128 KiB object-store chunks, one dependent round trip of the engine's each, and the value's bytes across the laptop's uplink: on NGS they took 44 % of the TypeScript run, against 13 % of the plain one. On the dev NGS rerun the large-value tests took 23 to 81 s each, their bytes and not their round trips: "a big continue-as-new INPUT crosses generations as refs" (7.5 × `max_payload`, 60 MiB) needed about 51 s at that rate and had 44.7 s of round-trip factor; at a probed 1.19 MiB/s its budget is now (30 s + 108 s) × 1.51 = 208 s, its `result()` wait 201 s (F37). **The engine's position matters:** run from a laptop, the engine's own hop to the store is the WAN as well, and every chunk it writes or reads crosses it; an engine next to the broker (as the fabric runs it) has a short hop, and the same suite's bytes cross the WAN once. The probe measures whichever it is, since it goes through the run's engine. Over the WAN their `put` parts wait longer now (F33), so a slow store is waited for; a store too slow even for that fails the run, loud, as a value too large. The `status()`-after-`result()` race is the engine's to prevent since `ee81460` (its F29): a failure of that shape is the engine's. Afterwards read the printed report, or `.de-run/de/` and `.de-run/de-py/`: `leftovers.txt` should show no consumers but the tenant's own durables, and no messages the run wrote; `metrics.txt` is the engine's `/metrics` (`de_engine_pull_errors_total`, the acks by outcome); `engine.log` is its log.

### As a tenant

`DE_TENANT` runs the suites as a tenant of an engine in tenancy mode (`serve --tenancy`, the engine's PR #7; its `docs/API.md` "Tenancy"; `docs/findings.md` F38). The tests connect as the tenant's user, whose account reaches the engine through a service import and holds no stream, so the harness reads the engine's account through a second connection, `DE_ADMIN_URL` / `DE_ADMIN_CREDS`: its snapshot, the purge after each test, the leftovers check, the end's sweep and report count only the tenant's subjects (`{prefix}.{tenant}.…`, `$KV.<bucket>.{tenant}.…`), its objects (`{tenant}/…`) and its durables (`{prefix}~{tenant}~…`), and never purge or delete another tenant's, or the seven. It needs `DE_PREFIX` (the import's) and `DE_ENGINE_URL` (the engine runs in the other account), and refuses `DE_CLEANUP=delete`. A test that reads the engine's streams, buckets or objects itself is skipped, saying so (`skipped as tenant A: … reads the engine's streams …`): TypeScript 46 (45 of them beyond the `DE_ENGINE_URL` skips), Python 17 (16).

Locally, on a three-account server as the engine's `TestTenancy*` tests set it up (`internal/testserver.TenancyConfig`: accounts `DE`, `A`, `B`; `de.A.api.>` exported to A, imported there as `de.api.>` with `share: true`; users `de`, `a-client`, `b-client`, each password its name):

```sh
nats-server -c tenancy.conf &                                   # the config of F38, port 14322
durable-engine serve --tenancy --nats nats://de:de@127.0.0.1:14322 --prefix de \
  --metrics-addr 127.0.0.1:19391 --tenant-max-bytes 0 &         # the counters never go down (F38)
export DE_NATS_URL=nats://a-client:a-client@127.0.0.1:14322 DE_TENANT=A DE_PREFIX=de
export DE_ADMIN_URL=nats://de:de@127.0.0.1:14322 DE_ENGINE_URL=http://127.0.0.1:19391
npm run test:integration                         # 61 passed, 82 skipped, ~52 s
(cd python && uv run pytest tests/integration -rs)   # 34 passed, 61 skipped, ~31 s
```

The same with a standalone engine (`DE_ENGINE_URL`, no `DE_TENANT`) was TypeScript 106 passed and 35 skipped, Python 50 passed and 43 skipped at `d3e741d` (not rerun at `a7c5010`; `limits` adds 2 skips to each): the tenant's counts are those but for the named skips. A user and password in a URL are the connection's (`user:password@`; nats.js does not read them from a URL, the harness and the interop scripts do). On NGS the tenant's creds are `DE_NATS_CREDS` and the engine account's `DE_ADMIN_CREDS`.

## The interop worker

`conformance/interop/ts-worker.ts` is the library's, on the thin worker: `greet`, `approve`, `boom` and the parked `echo_signal`, with `DE_PAYLOAD_KEY` (hex) for encryption at rest. It needs a `durable-engine serve` on the same prefix:

```sh
NATS_URL=nats://127.0.0.1:4222 DE_PREFIX=de npx tsx conformance/interop/ts-worker.ts   # prints READY
```

The Python suites drive it (`python/tests/integration/test_interop.py`), and `test/integration/interop-py.test.ts` drives the Python interop worker (`python/conformance/worker.py`) with the TypeScript client, each with and without `DE_PAYLOAD_KEY`. `conformance/interop/ts-client.ts` is the library's TypeScript client for the Python reverse suite.
