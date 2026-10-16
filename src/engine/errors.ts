// @synadia-ai/durable — the errors the engine's API adds to the library's (src/errors.ts, copied
// from it and edited since). A refusal is `{error: {code, message}}` from a closed set (the
// engine's API.md "Errors"); the transport turns each code into one of these, or into a library error where the
// library already has the class: `fenced` → FencedError, `too_large` → PayloadTooLargeError,
// `blob_gone` → BlobMissingError (or BlobIntegrityError when the bytes are not the digest's).
// `tenant_limit` and `licence_limit` are refusals of their own (`LimitError`), never retried.
// One is this SDK's alone, no engine's code: `UnsupportedEngineError`, an engine older than the
// floor (`ENGINE_FLOOR`), read from `info.capabilities`.

import { DurableError, FencedError } from "../errors.js";

/** The closed set of refusal codes of API v1. */
export type EngineErrorCode =
  | "invalid"
  | "fenced"
  | "expired"
  | "not_found"
  | "too_large"
  | "unavailable"
  | "blob_gone"
  | "internal"
  | "tenant_limit"
  | "licence_limit";

/**
 * The codes table of the engine's API.md "Errors", mirrored: each code and the class a
 * refusal with it becomes (`invalid` on an `append` or a `child` is a SerializationError instead).
 * test/unit/engine-errors.test.ts checks it against the table at engine.pin, so a code the engine
 * adds fails a test here rather than reaching a caller as `unavailable`.
 */
export const ENGINE_ERROR_CODES: Readonly<Record<EngineErrorCode, string>> = {
  invalid: "InvalidRequestError",
  fenced: "FencedError",
  expired: "LeaseExpiredError",
  not_found: "RunNotFoundError",
  too_large: "PayloadTooLargeError",
  unavailable: "EngineUnavailableError",
  blob_gone: "BlobMissingError",
  internal: "EngineUnavailableError",
  tenant_limit: "LimitError",
  licence_limit: "LimitError",
};

/** A refusal (or a failure to reach the engine) on one op. */
export class EngineError extends DurableError {
  readonly op: string;
  /**
   * The API's code, or `"unavailable"` for no responders and a request timeout; `"unsupported"`
   * (this SDK's, never the engine's) for an engine older than the floor.
   */
  readonly code: EngineErrorCode | "unsupported";
  constructor(
    op: string,
    code: EngineErrorCode | "unsupported",
    message: string,
    options?: ErrorOptions,
  ) {
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
  /** No answer within the request's timeout (rather than no responders or a refusal). */
  readonly timedOut: boolean;
  constructor(
    op: string,
    code: "unavailable" | "internal",
    message: string,
    options?: ErrorOptions & { timedOut?: boolean },
  ) {
    super(op, code, message, options);
    this.name = "EngineUnavailableError";
    this.timedOut = options?.timedOut === true;
  }
}

/**
 * A refusal at a limit: `tenant_limit` (the fabric mode: the caller's tenant is at one of its
 * limits) or `licence_limit` (standalone: the engine's licence bounds what was asked). `limit`
 * names which one, as the engine says it: `workflows`, `workers`, `runs`, `runsPerDay`, `tenants`,
 * `expired` (the licence's), `bytes`, `runs`, `workflows`, `polls`, `rate` (the tenant's).
 *
 * Not a fault of the request and not transient: it clears when runs end, an operator raises the
 * limit or installs a renewed licence (`rate` within a second). So no call of this package retries
 * it: a client's call rejects with it, a worker's refused poll is logged at error naming the limit
 * and backs off, `worker.start()` rejects with it when its first poll is refused, and inside an
 * execution the item goes back (`done retry`), logged at error.
 */
export class LimitError extends EngineError {
  declare readonly code: "tenant_limit" | "licence_limit";
  /** Which limit, as the engine names it (`""` if a refusal named none). */
  readonly limit: string;
  constructor(
    op: string,
    code: "tenant_limit" | "licence_limit",
    limit: string,
    message: string,
    options?: ErrorOptions,
  ) {
    super(op, code, message, options);
    this.name = "LimitError";
    this.limit = limit;
  }
}

/**
 * What this SDK needs of the engine, read from `info.capabilities` (the engine's API.md
 * `info`, its F57), never from `info.engine`: the repo has no tag, so every build says `0.2.0-dev`.
 * `withdrawn-remembered` (its F53): a worker withdrawn by `cancelPoll` is remembered 5 s on every
 * instance, so stop() sends one `cancelPoll`. The engine announces it from its #13 (`16cb3da`).
 */
export const ENGINE_FLOOR = {
  capabilities: ["withdrawn-remembered"],
  since: "the engine's #13 (16cb3da)",
} as const;

/**
 * The engine that answered `info` is older than this SDK's floor (`ENGINE_FLOOR`): its
 * `capabilities` are absent or lack one this SDK needs. `worker.start()` rejects with it; a worker
 * whose start found no engine checks at its first answered poll, and stops, logged at error.
 * `engine` and `commit` are as `info` gave them (`""` when absent). Not retried: the engine must
 * be upgraded.
 */
export class UnsupportedEngineError extends EngineError {
  declare readonly code: "unsupported";
  /** `info.engine`, the build's version (`0.2.0-dev` for every build so far, so not the floor). */
  readonly engine: string;
  /** `info.commit`, the build's commit. */
  readonly commit: string;
  /** `info.capabilities`, or `undefined` when the reply had none. */
  readonly capabilities: readonly string[] | undefined;
  /** The capabilities this SDK needs that the engine lacks. */
  readonly missing: readonly string[];
  constructor(
    prefix: string,
    info: { engine?: unknown; commit?: unknown; capabilities?: unknown },
  ) {
    const engine = typeof info.engine === "string" ? info.engine : "";
    const commit = typeof info.commit === "string" ? info.commit : "";
    const caps = Array.isArray(info.capabilities) ? info.capabilities.filter((c): c is string => typeof c === "string") : undefined; // prettier-ignore
    const missing = ENGINE_FLOOR.capabilities.filter((c) => !(caps ?? []).includes(c));
    super(
      "info",
      "unsupported",
      `the engine on prefix "${prefix}" (engine ${engine || "?"}, commit ${commit || "?"}) ` +
        `${caps === undefined ? "announces no capabilities" : `lacks ${missing.join(", ")}`}: ` +
        `this SDK needs the engine at or after ${ENGINE_FLOOR.since}, which announces ` +
        `${ENGINE_FLOOR.capabilities.join(", ")} in info.capabilities`,
    );
    this.name = "UnsupportedEngineError";
    this.engine = engine;
    this.commit = commit;
    this.capabilities = caps;
    this.missing = missing;
  }
}

/** The engine's `info` reply meets the floor, or the error that says why not. */
export function checkEngineFloor(
  prefix: string,
  info: { engine?: unknown; commit?: unknown; capabilities?: unknown },
): UnsupportedEngineError | undefined {
  const caps = Array.isArray(info.capabilities) ? (info.capabilities as unknown[]) : undefined;
  if (caps !== undefined && ENGINE_FLOOR.capabilities.every((c) => caps.includes(c))) return undefined; // prettier-ignore
  return new UnsupportedEngineError(prefix, info);
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
