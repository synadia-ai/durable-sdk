# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] — 2026-10-06

The first published version of the thin TypeScript SDK, continuing the frozen library's `0.7.0-alpha` line.

- **The library's API on durable-engine:** `defineWorkflow`, `createClient`, `createWorker` and the whole `ctx` (`run`, `sleep`, `await`, `call`, `startCall`, `race`, `now`, `random`, `uuid`, `key`, `continueAsNew`, `onCancel`), the replay core unchanged. Every call is one request to the engine's API (`{prefix}.api.v1.<op>`); the only dependency is `@nats-io/nats-core`. Standalone or on the Synadia Agent Fabric, with the same code.
- **The engine-version floor:** `worker.start()` reads the engine's `info.capabilities` and rejects with `UnsupportedEngineError` (`engine`, `commit`, `capabilities`, `missing`) when the engine is older than this SDK needs.
- **Payload offload through the engine:** a value too large for one request goes up in `put` parts and travels as a ref; one too large for a reply comes down in `get` parts, its digest checked. `offload: false` makes a value too large for a request fail loud instead.
- **Typed engine errors:** one class per refusal code of the engine's API (`InvalidRequestError`, `LeaseExpiredError`, `RunNotFoundError`, `EngineUnavailableError`, `LimitError` for `tenant_limit` and `licence_limit`, `EngineFencedError`), all `EngineError`s.
- **`aesGcmPayloadCodec`:** payloads encrypted before they leave the process; the engine sees a run's shape and never a value.

The development before this version: [`docs/HISTORY.md`](https://github.com/synadia-ai/durable-sdk/blob/main/docs/HISTORY.md).
