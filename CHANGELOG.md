# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.7.0-alpha] — unreleased

### Follow-up 4 (2026-10-04): the tenancy pin, bandwidth in the budgets, the harness as a tenant

`engine.pin` moves from `ee81460` to `d3e741d` (the engine's PR #7, tenancy). Both languages unless said; nothing in `src/` or `python/src/` changed.

- **The large-value budgets learn bandwidth** (`docs/findings.md` F37): in the existing-server mode the harness probes the throughput once (a 4 MiB `put` and `get` through the run's engine on the tests' link) and prints it next to the round trip; every test that moves values over the inline limit adds n × `max_payload` over it, × 2, to its waits (`moving(n)`, `budget(ms, n)`; Python `existing.moving(n)`), n from a per-test tally of `put` / `get` bytes (`.de-run/<prefix>/blob-bytes.tsv`). The whole `offload` suite, the large `payload-codec` cases, Python's two large-signal cases. At the dev NGS's 51 ms and ≈ 1.19 MiB/s the continue-as-new test's budget is 208 s (was 45 s).
- **The harness as a tenant** (F38): `DE_TENANT`, `DE_ADMIN_URL`, `DE_ADMIN_CREDS`. The streams are read through the admin connection, and every check and cleanup counts the tenant's subjects, objects and durables alone, never another tenant's or a stream. A test that reads the engine's stores itself is skipped as a tenant, saying so (TypeScript 46, Python 17). Proven on a local three-account server (the engine's `TenancyConfig`) with tenant B's state left untouched: TypeScript 61 passed, 80 skipped; Python 34 passed, 59 skipped.
- **Fixes the tenant runs found:** a test vitest skips part-way gets its cleanup at the next test's start (vitest runs no `afterEach` after a skip), and `interop-py` stops a worker such a test left. Under `DE_ENGINE_URL` the end's sweep repeats until quiet, and a test that reads the engine's log is skipped (it read an empty one). A server URL's `user:password@` is the connection's for nats.js (the harness, the interop scripts).
- The README's time table gains the dev NGS `offload` rerun and the engine's position; F32 is handed on to the engine (F39). New findings F36 to F39.

### Follow-up 3 (2026-10-04): `blob_gone`, the new pin, and the cleanup default

`engine.pin` moves from `57ae3c9` to `ee81460` (the engine's PR #5: `status` never lags `result`, `blob_gone`, a cluster harness). Both languages unless said.

- **`blob_gone` is the library's missing-blob error** (`docs/findings.md` F31; the engine's F30): the transport maps the code, with its `blob` name, to `BlobMissingError`, or to `BlobIntegrityError` when the engine says the bytes are not the digest's. Inside an execution the worker parks the run as the library did (`blob-unresolvable`, loud, `done retry`, never a terminal), also when the pure core halted on it under a step-end `load`; a `poll` that answers it is logged and polled again; a client's `result` raises it. `get`'s `internal` is no longer read as "gone" (F24 closed).
- **A `put` part's timeout scales with what the engine stores for it** (F33): the margin, plus 4 round trips (`nc.rtt()`) per 128 KiB store chunk and 1 s per MiB, the last part counting the whole value twice. A `put` the engine does not answer within that, or refuses as `invalid`, is `PayloadTooLargeError` / `SerializationError`, which the pure core fails the run on, never a redelivery loop. The engine's `unavailable`, no responders and `expired` keep their meaning.
- **A refusal is an `error` member with a string `code`** (the engine's F31): both transports already read it so; a unit test each (`test/unit/engine-transport.test.ts`, `python/tests/test_engine_transport.py`).
- **The harness's mode** (F33, F34): a generated prefix deletes its seven at the end by default (`DE_CLEANUP=keep` keeps them; a `DE_PREFIX` one is kept unless `DE_CLEANUP=delete`); after each test, failed or timed out too, the workers it left running are stopped, its workflows' polls withdrawn (`cancelPoll`) before their durables are deleted, and its runs' blobs deleted by name; at the end, with the engine stopped, every blob of the run's runs and every durable the run made. Assertions over a store's contents count the test's own runs in the mode (`offload`, `payload-codec`; Python `test_offload`). `DE_INJECT_FAILURE=1` runs two tests that fail on purpose, to prove it. The README's time formula is a measured table.
- **New tests:** `offload` "blob_gone: …" and Python's twin, and the twin of "a missing blob PARKS the run", which now reads the engine's `parked` count, not its log. The `status`-after-`result` waits stay, noted as the engine's guarantee now.
- New findings F31 to F35; F24 closed.

### Follow-up 2 (2026-10-04): the harness's mode for an existing server

`engine.pin` moves from `1efd2ec` to `57ae3c9` (the engine's PR #4, pull on demand). Both languages unless said; nothing in `src/` or `python/src/` changed.

- **`DE_NATS_URL`** runs the integration suites against a server the run does not own (`docs/findings.md` F29; the README's "Against an existing server"): no spawn; `DE_NATS_CREDS`; one prefix (`DE_PREFIX`) and one `durable-engine serve` for the run (or `DE_ENGINE_URL`'s), restarted only for a test's flags; after each test a purge of what it changed under the prefix and of the durables made for its workflow names, never a stream; the leftovers check on the prefix's streams; every wait scaled by the measured round trip; `DE_CLEANUP=delete`; the prefix's leftovers, the engine's `/metrics` and its log in `.de-run/`. Under `DE_TEST_RTT_MS` the engine goes through the proxy too.
- **Skipped in the mode, by name:** the 16 network tests that run (a proxy or a server of their own) and the deleted-`DE_TIMERS` test, in each language.
- The interop worker and client read `NATS_CREDS`; Python's dev dependencies gain `nkeys` (F30).
- Test fixes the 150 ms run found: the work durable is bound at the first poll on `57ae3c9` (TypeScript `e2e`), F27's race in `interop-py`, two Python sleeps that stood for "the step is executing".
- New findings F29, F30.

### Follow-up (2026-10-04): the SDK halves of the engine's PR #3

`engine.pin` moves from `d8c0d73` to `1efd2ec`. Both languages unless said.

- **Chunked upload and download** (`docs/findings.md` F1, F26): `src/engine/blobs.ts`, `python/src/synadia_durable/_engine/blobs.py`. A value whose request would pass `request_max` goes up in `put` parts and travels as a ref (`append`, `start`, `signal`, `child`); a ref handed back by `poll`, `load` or `result` comes down in `get` parts, its digest checked. Through the library's `offloadEntry` / `storePayloadBlob` / `resolveEntry` with the engine as the blob store. `offload: false` keeps its meaning. The client's `metrics` now sees `blob.put` / `blob.get`.
- **`stop()` withdraws its polls** (F5): every poll names the worker, `stop()` sends `poll {cancelPoll}`; 16 polls released in under 2 ms, against ~30 s.
- **Metrics twins** (F21): the TypeScript worker emits `journal.load` once per item with its `bytes`, and `journal.append` with the size sent; Python's `journal.load` gains `bytes`. New `test/integration/metrics.test.ts`.
- **Skips removed:** F6, F7, F8, F9 (TypeScript 4, Python 2) and F1, F2 (TypeScript 16, Python 11). One skip is left in each language, the dropped "LEASE at RTT 500 ms" kept for the diff.
- Python's harness maps `offload=OffloadOptions(threshold_bytes=…)` to `serve --offload-threshold`.
- New findings F22 to F28: an `internal` that means "gone" on `get` (F24, ask), `status` lagging `result` (F27, ask), the suites over 150 and 300 ms of RTT (F28).

### Python SDK (`python/`, `synadia-durable` `0.7.0a0`)

The thin Python SDK (board row P2-19b, decision 61), the twin of the TypeScript one: the replay core of the library's `python/` at `7456c75`, copied unchanged, behind a request/reply client for `durable-engine` at `d8c0d73`.

- `synadia_durable/_engine/`: `transport.py`, `payload.py`, `client.py`, `worker.py`, `errors.py`, `names.py`, op for op the twins of `src/engine/`. The only dependency is `nats-py`, for core request/reply.
- The library's unit suites (12 unchanged, 4 adapted: `docs/findings.md` F15) and 13 of its 21 integration suites, run against `durable-engine serve` (F20 for the eight left behind); the 34 golden vectors.
- The three-way interop: the Python client drives the thin TypeScript worker, the TypeScript client drives the Python interop worker (`python/conformance/worker.py`, new, and `test/integration/interop-py.test.ts`, new), each plain and under `DE_PAYLOAD_KEY`.
- The examples `charge`, `saga`, `crash_replay`, `fan_out`, `cancel`. `durable_agent` is left out (F18).
- Where Python differs from TypeScript: F17, F21.

### Removed

- `upgrade-compat` in both languages: `test/integration/upgrade-compat.test.ts` and the README's `NATS_SERVER_OLD`. No compatibility with older brokers is needed; recent versions only. The library's Python tree had no twin of it (F19).

### TypeScript SDK

The thin TypeScript SDK (the fabric's board row P2-19a, decision 61): the replay core of `@synadia-ai/durable` at `7456c75`, copied unchanged, behind a request/reply client for `durable-engine` at `d8c0d73` (`engine.pin`).

### Added

- `src/engine/`: the transport (one request to `{prefix}.api.v1.<op>`, the closed set of error codes as typed errors), the two payload forms, `createClient` (`start`, `signal`, `cancel`, `get`, `result`, `status`) and `createWorker` (the API's worker loop: `poll` per slot, `load` pages, `append`, `wake` and `child` under the lease, `extend`, `done`).
- New errors: `EngineError`, `LeaseExpiredError`, `InvalidRequestError`, `EngineUnavailableError`, `RunNotFoundError`, `EngineFencedError` (a `FencedError` carrying `expectLastSeq` and `serverCode`).
- The library's 13 unit suites (10 unchanged, 3 adapted: `docs/findings.md` F4) and its 10 integration suites, run against `durable-engine serve`; the 34 golden vectors.
- The examples `charge`, `saga`, `crash-replay`, `fan-out`, `cancel`, and the interop worker `conformance/interop/ts-worker.ts`.

### Changed, from the library

- The only dependency is `@nats-io/nats-core`. Provisioning, consumers, the run lease, the obligations after a terminal, the maintenance pass and offload are the engine's. `provision`, `deprovision`, `resourceStreams` and `names` are gone from the exports.
- The worker options that became `serve` flags (`ackWaitMs`, `maxWorkMs`, `lease`, `leaseTtlMs`, `streamMaxBytes`, `maintenanceIntervalMs`, `reconcileGraceMs`, `versionWaitMs`, `reconcileTimers`, `offload`) are accepted and ignored.
- `health().pumps` lists the worker's poll slots.
- Under a payload codec, signal data and inputs are encrypted before they leave the process; nothing on the work queue is plaintext (F14).

### Known limits

- ~~A single value must fit one request (about `max_payload` − 16 KiB); API v1 has no chunked upload (F1).~~ Lifted by the follow-up above.
