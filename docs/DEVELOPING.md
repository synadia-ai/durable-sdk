# Developing

For contributors to this repo: the test suites, the harness's modes, and the interop worker. A user of the package needs none of it; the [README](../README.md) is theirs.

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
npm run test:integration                        # 125 passed, 23 skipped, ~69 s
DE_TEST_RTT_MS=150 npm run test:integration     # every round trip 150 ms, the engine's too
(cd python && uv run pytest tests/integration -rs)   # 78 passed, 22 skipped, ~60 s
```

**The dev NGS run** (the coordinator's, on Mario's word; nothing here runs it):

```sh
# at the PR's head, e.g. ../wt/durable-sdk; the engine built from a clean checkout of engine.pin
export DE_NATS_URL=tls://connect.ngs.global
export DE_NATS_CREDS=<agent-fabric-tests>/local/ngs/creds/tenant-a-dev/<user>.creds
export DE_PREFIX=de                          # the seven exist there: the engine's start reconciles, creates none
export DE_ENGINE=$PWD/.engine-bin/durable-engine-dev-5047d7943c31   # its durable-licence-5047d7943c31 beside it mints the run's licence
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
