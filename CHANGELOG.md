# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.7.0-alpha] — unreleased

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
