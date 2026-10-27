# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.2.0] — 2026-10-17

- **Works with the current engine:** the engine keeps one pair of consumers per tenant for all its workflow names, drops a work item that has no journal after a time limit, and journals a signal when it arrives, once per `id`.
- **`client.signal` takes an `id`:** TypeScript `client.signal(runId, name, data, { id })`, Python `await client.signal(run_id, name, data, id=...)`. Without one the SDK generates a UUID, so every signal carries an id. The id is checked with the token rule (`[A-Za-z0-9_-]+`) before any request. A caller that retries a signal after `EngineUnavailableError`, a timeout or its own restart passes the id of the first try, and the engine journals it once.
- **`ctx.attempt` is documented as the delivery count** of the current frontier: it counts every delivery, an item handed back between engine instances too, so it is not a count of failures.
- **Removed `ConsumerLimitError` and `ProvisionQuotaError`** (both languages): exported, never raised; they described the retired library's provisioning, which the engine does.
- **The READMEs and the package metadata no longer link to source repositories.**
- **Docs:** the comments shipped in the package cite the durable execution protocol by section ("PROTOCOL §8") and name the engine's `API.md`, the document delivered with the engine, instead of file paths. No behaviour changed.

## [0.1.1] — 2026-10-06

- **The README is a user's:** the Quickstart runs the engine Synadia delivers (the archive's `durable-engine` binary or the container image, with your licence file) and a workflow in a project of your own installed from npm, with no engine checkout and no build; "Which engine", "Errors" and "Running a worker" say what a user needs and nothing more. How the SDK works with the engine moved to `docs/DEVELOPING.md`. No code changed.

## [0.1.0] — 2026-10-06

The first published version of the thin TypeScript SDK, continuing the frozen library's `0.7.0-alpha` line.

- **The library's API on durable-engine:** `defineWorkflow`, `createClient`, `createWorker` and the whole `ctx` (`run`, `sleep`, `await`, `call`, `startCall`, `race`, `now`, `random`, `uuid`, `key`, `continueAsNew`, `onCancel`), the replay core unchanged. Every call is one request to the engine's API (`{prefix}.api.v1.<op>`); the only dependency is `@nats-io/nats-core`. Standalone or on the Synadia Agent Fabric, with the same code.
- **The engine-version floor:** `worker.start()` reads the engine's `info.capabilities` and rejects with `UnsupportedEngineError` (`engine`, `commit`, `capabilities`, `missing`) when the engine is older than this SDK needs.
- **Payload offload through the engine:** a value too large for one request goes up in `put` parts and travels as a ref; one too large for a reply comes down in `get` parts, its digest checked. `offload: false` makes a value too large for a request fail loud instead.
- **Typed engine errors:** one class per refusal code of the engine's API (`InvalidRequestError`, `LeaseExpiredError`, `RunNotFoundError`, `EngineUnavailableError`, `LimitError` for `tenant_limit` and `licence_limit`, `EngineFencedError`), all `EngineError`s.
- **`aesGcmPayloadCodec`:** payloads encrypted before they leave the process; the engine sees a run's shape and never a value.
