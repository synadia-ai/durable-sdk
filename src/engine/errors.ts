// @synadia-ai/durable — the errors the engine's API adds to the library's (src/errors.ts, copied
// unchanged). A refusal is `{error: {code, message}}` from a closed set (durable-engine docs/API.md
// "Errors"); the transport turns each code into one of these, or into a library error where the
// library already has the class: `fenced` → FencedError, `too_large` → PayloadTooLargeError.

import { DurableError, FencedError } from "../errors.js";

/** The closed set of refusal codes of API v1. */
export type EngineErrorCode =
  "invalid" | "fenced" | "expired" | "not_found" | "too_large" | "unavailable" | "internal";

/** A refusal (or a failure to reach the engine) on one op. */
export class EngineError extends DurableError {
  readonly op: string;
  /** The API's code, or `"unavailable"` for no responders and a request timeout. */
  readonly code: EngineErrorCode;
  constructor(op: string, code: EngineErrorCode, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "EngineError";
    this.op = op;
    this.code = code;
  }
}

/**
 * The item's lease is not the engine's any more (`expired`): the item is done, past `maxWorkMs`,
 * minted by an instance that is gone, or (for `extend`) lapsed. The worker stops executing that
 * item and sends nothing more under its lease; the item redelivers to a poll.
 */
export class LeaseExpiredError extends EngineError {
  constructor(op: string, message: string, options?: ErrorOptions) {
    super(op, "expired", message, options);
    this.name = "LeaseExpiredError";
  }
}

/** A malformed request (`invalid`) outside a journal append: a bug or a bad argument. Never retried. */
export class InvalidRequestError extends EngineError {
  constructor(op: string, message: string, options?: ErrorOptions) {
    super(op, "invalid", message, options);
    this.name = "InvalidRequestError";
  }
}

/**
 * Transient: no engine answered (NATS no responders), the request timed out, or the engine said
 * `unavailable` or `internal`. Inside an execution the pure core halts with `retry` on it and the
 * worker reports `done retry`; a client call may simply be repeated.
 */
export class EngineUnavailableError extends EngineError {
  constructor(
    op: string,
    code: "unavailable" | "internal",
    message: string,
    options?: ErrorOptions,
  ) {
    super(op, code, message, options);
    this.name = "EngineUnavailableError";
  }
}

/** `signal` or `cancel` of a run that was never started (`not_found`). */
export class RunNotFoundError extends EngineError {
  readonly runId: string;
  constructor(op: string, runId: string, options?: ErrorOptions) {
    super(op, "not_found", `client.${op}: run "${runId}" not found`, options);
    this.name = "RunNotFoundError";
    this.runId = runId;
  }
}

/**
 * The library's FencedError, as an `append` refusal carries it: `expectLastSeq` is what the append
 * said, `serverCode` what the broker answered underneath (10071; 10164 or 10158 on a replicated
 * journal; absent when the engine fenced a duplicate ack itself). Both are for logs only: a fence
 * is a reply, and this package maps no broker code of its own.
 */
export class EngineFencedError extends FencedError {
  readonly expectLastSeq: number | undefined;
  readonly serverCode: number | undefined;
  constructor(runId: string, message: string, expectLastSeq?: number, serverCode?: number) {
    super(runId, message);
    this.expectLastSeq = expectLastSeq;
    this.serverCode = serverCode;
  }
}
