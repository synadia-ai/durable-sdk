// @synadia-ai/durable — error hierarchy.
//
// All library errors extend {@link DurableError}. Re-exported from the "@synadia-ai/durable/errors"
// subpath for `instanceof` checks without importing the whole library.
//
//   - {@link FencedError}         — a writer lost the single-writer CAS race (NATS code 10071).
//   - {@link NonDeterminismError} — replay diverged from the recorded journal (fail loud).
//   - {@link WorkflowFailure}     — a workflow function threw; the run is terminally failed.
//   - {@link StepNestingError}    — a ctx command was called inside a ctx.run step fn (fail loud).
//   - {@link SerializationError}  — a journal value could not be encoded (deterministic; fails the run).
//   - {@link PayloadTooLargeError} — a journal entry exceeds the server max_payload (deterministic; fails the run).
//   - {@link BlobMissingError}   — a journaled blob ref points at an object that is gone (parked, not failed).
//   - {@link BlobIntegrityError} — a fetched blob failed digest verification / a ref is malformed.
//   - {@link NotImplementedError} — reserved for a not-yet-wired feature (no thrower in v0.1).
//   - {@link DurableTimeoutError} — a bounded wait elapsed (e.g. ctx.await timeoutMs).
//   - {@link ConnectionClosedError} — the NATS connection closed for good under a wait (the run is unaffected).
//   - {@link ConsumerLimitError} — the account's per-stream consumer cap refused a worker's durable (start fails whole).
//   - {@link ProvisionQuotaError} — the account's storage or stream quota refused one of a prefix's eight resources.

export class DurableError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "DurableError";
  }
}

/**
 * Raised when a journal append or KV CAS is rejected because the subject advanced underneath us
 * (NATS `JetStreamApiError` code 10071, "wrong last sequence"). It means another worker owns this
 * run — the losing worker is a zombie and must stop. Surfaced out-of-band to the worker; never
 * thrown through user workflow code (so a user `try/catch` cannot swallow it).
 */
export class FencedError extends DurableError {
  readonly runId: string;
  constructor(runId: string, message?: string, options?: ErrorOptions) {
    super(
      message ?? `run ${runId}: fenced — lost single-writer CAS (another worker owns this run)`,
      options,
    );
    this.name = "FencedError";
    this.runId = runId;
  }
}

/**
 * Raised when replay diverges from the recorded journal (e.g. a different step name at a given
 * sequence). Indicates nondeterministic workflow code. We fail loud rather than corrupt the run.
 */
export class NonDeterminismError extends DurableError {
  readonly runId: string;
  readonly seq: number;
  constructor(runId: string, seq: number, detail: string, options?: ErrorOptions) {
    super(`run ${runId}: non-determinism at seq ${seq}: ${detail}`, options);
    this.name = "NonDeterminismError";
    this.runId = runId;
    this.seq = seq;
  }
}

/** A workflow function threw an error; the run is terminally failed and the error is journaled. */
export class WorkflowFailure extends DurableError {
  readonly runId: string;
  constructor(runId: string, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "WorkflowFailure";
    this.runId = runId;
  }
}

/**
 * Raised when a `ctx.*` command (run/now/random/uuid/sleep/await/call/continueAsNew) is invoked
 * INSIDE a `ctx.run` step function. Nesting interleaves command sequence numbers, so the run would
 * complete once and then fail every replay — we fail loud at the first call instead. (`ctx.key`,
 * `ctx.get`, and `ctx.assert` are not commands and ARE allowed inside a step.)
 */
export class StepNestingError extends DurableError {
  readonly runId: string;
  constructor(runId: string, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "StepNestingError";
    this.runId = runId;
  }
}

/**
 * Raised when a journal entry cannot be encoded by the codec — a step result, sub-workflow input, or
 * continue-as-new input that is not serializable (a `BigInt`, a non-finite number like `NaN`/`Infinity`,
 * a circular object, …). This is a
 * DETERMINISTIC programming error: re-encoding the same value always fails, so the run is failed
 * terminally rather than retried as a transient infrastructure error (which would loop forever).
 */
export class SerializationError extends DurableError {
  readonly runId: string;
  constructor(runId: string, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "SerializationError";
    this.runId = runId;
  }
}

/**
 * A journal entry's ENCODED payload exceeds the server's `max_payload` (1 MiB default on a
 * self-hosted nats-server; 8 MiB on NGS). Deterministic for a given value — re-publishing the same
 * bytes can never succeed — so it extends {@link SerializationError} and terminally fails the run,
 * instead of being classified as transient infrastructure and NAK-looping forever (which would also
 * head-of-line-block the workflow's work consumer). Large values need object-store offload
 * (PROTOCOL §5.6).
 */
export class PayloadTooLargeError extends SerializationError {
  constructor(runId: string, message: string, options?: ErrorOptions) {
    super(runId, message, options);
    this.name = "PayloadTooLargeError";
  }
}

/**
 * A journal entry references an offloaded payload blob that no longer exists in the blob store
 * (the object was GC'd while the run was still live, the bucket was deleted, or offload is
 * disabled on this reader while the journal carries refs). This is an OPERATIONAL error, not a
 * workflow failure, and it is potentially recoverable (restore the bucket from backup, re-enable
 * offload) — so the worker PARKS the run (NAK-with-backoff, loud) instead of writing a terminal.
 * The runbook: restore the blob, or `client.cancel` the run. See docs/OPERATIONS.md.
 */
export class BlobMissingError extends DurableError {
  readonly runId: string;
  readonly seq: number;
  readonly blobName: string;
  /**
   * `absent`: the store is reachable but the object is definitively gone (restorable — parkers
   * keep waiting). `no-store`: this READER has no blob store at all (offload disabled /
   * unprovisioned) — a configuration error that waiting never heals; surfaced loud.
   */
  readonly reason: "absent" | "no-store";
  constructor(
    runId: string,
    seq: number,
    blobName: string,
    detail: string,
    reason: "absent" | "no-store" = "absent",
  ) {
    super(`run ${runId}: blob "${blobName}" (entry seq ${seq}) cannot be resolved — ${detail}`);
    this.name = "BlobMissingError";
    this.runId = runId;
    this.seq = seq;
    this.blobName = blobName;
    this.reason = reason;
  }
}

/**
 * A fetched blob's content does not match the digest recorded in its journal ref (or the ref
 * itself is malformed). Content-addressed names make this impossible under normal operation —
 * it indicates out-of-band modification of the blob store. Fail loud; the worker parks the run
 * (same recovery posture as {@link BlobMissingError}: restore the object, or cancel the run).
 */
export class BlobIntegrityError extends DurableError {
  readonly runId: string;
  readonly seq: number;
  readonly blobName: string;
  constructor(runId: string, seq: number, blobName: string, detail: string) {
    super(`run ${runId}: blob "${blobName}" (entry seq ${seq}) failed verification — ${detail}`);
    this.name = "BlobIntegrityError";
    this.runId = runId;
    this.seq = seq;
    this.blobName = blobName;
  }
}

/**
 * A payload slot could not be put through (or taken back out of) the configured `PayloadCodec`:
 * the journal carries transformed payloads (`v:3`) and this reader has no codec, the codec itself
 * threw (wrong key, corrupt ciphertext), or the encoded form is malformed.
 *
 * Treated exactly like {@link BlobMissingError}: the worker PARKS the run with NAK-backoff rather
 * than writing a terminal, because every cause is operational and heals the moment the right codec
 * is configured. Failing the run instead would destroy recoverable work over a config mistake.
 */
export class PayloadCodecError extends DurableError {
  readonly runId?: string;
  readonly seq?: number;
  constructor(detail: string, ctx?: { runId?: string; seq?: number }, opts?: { cause?: unknown }) {
    const where =
      ctx?.runId !== undefined
        ? `run ${ctx.runId}${ctx.seq !== undefined ? ` (entry seq ${ctx.seq})` : ""}: `
        : "";
    super(`${where}${detail}`, opts);
    this.name = "PayloadCodecError";
    if (ctx?.runId !== undefined) this.runId = ctx.runId;
    if (ctx?.seq !== undefined) this.seq = ctx.seq;
  }
}

/** Reserved: a not-yet-wired feature was reached. No thrower in v0.1 — the `ctx` surface is complete. */
/**
 * Raised from `worker.start()` when the server refused to create one of the worker's durable
 * consumers because the ACCOUNT's per-stream consumer limit is reached (JetStream code 10026,
 * "maximum consumers limit reached" — a managed service caps consumers per stream; 250 on NGS's
 * R1 tier). A worker binds one durable per registered workflow NAME on `DE_WORK` and one on
 * `DE_TIMERS`, so the number of workflow names a prefix can carry is exactly that limit. The
 * worker starts NOTHING in this case: it binds every consumer before it consumes from any, because
 * a worker running k−1 of its k names looks healthy from outside and is deaf to one workflow.
 */
export class ConsumerLimitError extends DurableError {
  readonly stream: string;
  readonly durable: string;
  constructor(stream: string, durable: string, options?: ErrorOptions) {
    super(
      `${stream}: the account's per-stream consumer limit was reached creating "${durable}" — a ` +
        `worker binds one durable consumer per registered workflow name on this stream, so a prefix ` +
        `can carry at most as many workflow names as the account allows consumers per stream; ` +
        `nothing was started. Retire unused workflow names (delete their durables) or spread names ` +
        `across prefixes or accounts (OPERATIONS.md, "What the library leaves on the server")`,
      options,
    );
    this.name = "ConsumerLimitError";
    this.stream = stream;
    this.durable = durable;
  }
}

/**
 * Raised from `provision()` — so from `worker.start()` and `client.start()` — when the ACCOUNT
 * refused to create one of a prefix's eight resources: its storage cannot reserve the resource's
 * `max_bytes` (JetStream code 10047; a prefix reserves max_bytes on each of its three streams,
 * four KV buckets and object store — ~5.2 GiB at the 1 GiB default), or its stream count is
 * reached (10027; a prefix is eight streams, and NGS's R1 tier allows 16). The server's own text
 * says neither what was being reserved nor what to change; this names the resource, the bytes and
 * the option.
 */
export class ProvisionQuotaError extends DurableError {
  readonly resource: string;
  readonly code: number;
  constructor(resource: string, code: number, maxBytes: number, options?: ErrorOptions) {
    super(
      code === 10047
        ? `${resource}: the account has insufficient storage to reserve max_bytes=${String(maxBytes)} — ` +
            `a prefix reserves its max_bytes on each of its eight resources (three streams, four KV ` +
            `buckets, one object store; ~5.2 GiB at the 1 GiB default). Set streamMaxBytes (a worker/` +
            `client option) to what this prefix needs, or raise the account's storage ` +
            `(OPERATIONS.md, "Sizing quick sheet"). Resources created before the refusal are kept ` +
            `(provisioning never deletes) and re-provisioning with the corrected sizing reconciles them`
        : `${resource}: the account's stream limit is reached — a prefix is eight streams (three ` +
            `streams, four KV buckets, one object store). Retire a prefix (deprovision) or raise the ` +
            `limit (OPERATIONS.md, "What the library leaves on the server"). Resources created before ` +
            `the refusal are kept and re-provisioning completes them`,
      options,
    );
    this.name = "ProvisionQuotaError";
    this.resource = resource;
    this.code = code;
  }
}

export class NotImplementedError extends DurableError {
  constructor(feature: string) {
    super(`${feature} is not implemented yet`);
    this.name = "NotImplementedError";
  }
}

/** A bounded wait (e.g. handle.result(timeoutMs)) elapsed before the run reached a terminal state. */
export class DurableTimeoutError extends DurableError {
  constructor(message: string) {
    super(message);
    this.name = "DurableTimeoutError";
  }
}

/**
 * The NATS connection this client was given has CLOSED for good (closed by the application, or the
 * client library gave up reconnecting — `maxReconnectAttempts`, 10 by default in `@nats-io`), so a
 * wait on it can never be answered. Thrown by `Handle.result()` at once rather than after its
 * timeout: the RUN is unaffected — the workers own it — and `client.get(runId).result()` on a live
 * connection picks the wait up again. A connection that is merely RECONNECTING does not raise this;
 * the wait rides it out.
 */
export class ConnectionClosedError extends DurableError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "ConnectionClosedError";
  }
}

/**
 * A run was cancelled (via `client.cancel`). Surfaced by `Handle.result()` of a cancelled run, and as a
 * cancelled child's failure to its parent's `ctx.call`. NOT thrown through the main workflow flow — a
 * cancel is delivered as an out-of-band, uncatchable hard-stop at the next `ctx.*` command boundary
 * (recorded commands replay; no fresh command executes or appends once the cancel is journaled; cleanup
 * is opt-in via `ctx.onCancel`); a user `try/catch` cannot swallow it.
 */
export class CancelledError extends DurableError {
  readonly runId: string;
  constructor(runId: string, message?: string, options?: ErrorOptions) {
    super(message ?? `run ${runId}: cancelled`, options);
    this.name = "CancelledError";
    this.runId = runId;
  }
}
