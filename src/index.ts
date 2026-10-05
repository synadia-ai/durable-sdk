// @synadia-ai/durable — the thin durable execution SDK: the library's replay core in your process,
// durable-engine behind it (its request/reply API, `{prefix}.api.v1.<op>`).
//
// Public API (the library's at 7456c75, minus the shell's provision/deprovision/names):
//   - {@link defineWorkflow}  — declare a durable workflow (a normal async function over ctx).
//   - {@link createClient}    — start / observe / signal / cancel runs, through the engine.
//   - {@link createWorker}    — register workflows and run the engine's poll/replay loop.
//   - connection helpers, journal wire types, errors (also at "@synadia-ai/durable/errors").

// Core
export { defineWorkflow } from "./core/workflow.js";

// Public types
export type {
  Workflow,
  AnyWorkflow,
  WorkflowFn,
  Ctx,
  StepOptions,
  RetryPolicy,
  CallOptions,
  DurableFuture,
  Handle,
  RunStatus,
  RunState,
  StartOptions,
} from "./types.js";

// The engine client (the library's NATS shell is the engine's now)
export { createClient, type Client, type ClientOptions } from "./engine/client.js";
export {
  createWorker,
  type Worker,
  type WorkerOptions,
  type WorkerHealth,
} from "./engine/worker.js";
export { type WorkItem } from "./core/work-item.js";

// Connection helpers (BYO NatsConnection)
export { withReconnectDefaults, parseNatsUrl, RECONNECT_DEFAULTS } from "./nats/connect.js";

// Journal (for advanced users / future language SDKs — the wire layout is frozen at v:1;
// v:2 = the same layout with a payload slot offloaded to a content-addressed blob ref)
export type { JournalEntry, EntryKind, LoadedEntry, BlobRef } from "./journal/entry.js";
export { jsonCodec, type Codec } from "./journal/codec.js";
export { aesGcmPayloadCodec, type PayloadCodec } from "./journal/payload-codec.js";
export type { Journal, JournalAppend, AppendResult } from "./journal/store.js";
export { InMemoryBlobStore, type BlobStore, type OffloadOptions } from "./journal/blobs.js";

// Logging, metrics + version
export { SILENT_LOGGER, type Logger } from "./internal/logger.js";
export {
  SILENT_METRICS,
  type Metrics,
  type DurableEventName,
  type DurableEventFields,
} from "./internal/metrics.js";
export { VERSION, WIRE_VERSION, WIRE_VERSION_BLOBS, WIRE_VERSION_ENCODED } from "./version.js";

// Errors (also available from the "@synadia-ai/durable/errors" subpath)
export {
  DurableError,
  FencedError,
  NonDeterminismError,
  WorkflowFailure,
  StepNestingError,
  SerializationError,
  PayloadCodecError,
  PayloadTooLargeError,
  BlobMissingError,
  BlobIntegrityError,
  NotImplementedError,
  ConnectionClosedError,
  DurableTimeoutError,
  CancelledError,
  ConsumerLimitError,
  ProvisionQuotaError,
} from "./errors.js";
export {
  EngineError,
  EngineFencedError,
  EngineUnavailableError,
  InvalidRequestError,
  LeaseExpiredError,
  LimitError,
  RunNotFoundError,
  type EngineErrorCode,
} from "./engine/errors.js";
