# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.7.0-alpha] — unreleased

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

- A single value must fit one request (about `max_payload` − 16 KiB); API v1 has no chunked upload (F1).
