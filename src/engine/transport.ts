// @synadia-ai/durable — the one way this package speaks to the engine: a JSON request on
// `{prefix}.api.v1.<op>` and its parsed reply (durable-engine docs/API.md "Transport", "Encoding",
// "Errors"). A refusal `{error: {code, message}}` becomes a typed error here and nowhere else.
//
// No broker error code exists in this package. A fence is a reply (`fenced`), whatever the journal's
// replication answered underneath (the engine's findings F3 and F16 are about the library's
// 10071-only mapping on a replicated stream; the thin SDK never inherits it).

import {
  ClosedConnectionError,
  DrainingConnectionError,
  NoRespondersError,
  RequestError,
  TimeoutError,
  type NatsConnection,
} from "@nats-io/nats-core";
import { ConnectionClosedError, PayloadTooLargeError, SerializationError } from "../errors.js";
import { type Logger, SILENT_LOGGER } from "../internal/logger.js";
import {
  EngineFencedError,
  EngineUnavailableError,
  InvalidRequestError,
  LeaseExpiredError,
  RunNotFoundError,
} from "./errors.js";
import { apiSubject, type Op } from "./names.js";

/** The margin over `waitMs` for an op that waits (`result`, `poll`), and the timeout of every other op. */
export const REQUEST_MARGIN_MS = 10_000;

interface Refusal {
  error: { code: string; message?: string; expectLastSeq?: number; serverCode?: number };
}

export interface RequestOptions {
  /** For `result` and `poll`: the request's own `waitMs`; the timeout is this plus the margin. */
  waitMs?: number;
}

export type Transport = <R>(op: Op, body: object, opts?: RequestOptions) => Promise<R>;

const enc = new TextEncoder();
const dec = new TextDecoder();

export function createTransport(
  nc: NatsConnection,
  prefix: string,
  logger: Logger = SILENT_LOGGER,
): Transport {
  return async <R>(op: Op, body: object, opts?: RequestOptions): Promise<R> => {
    const timeout = (opts?.waitMs ?? 0) + REQUEST_MARGIN_MS;
    const payload = enc.encode(JSON.stringify(body));
    // A request past the server's max_payload can never be sent, so it is deterministic for this
    // value: fail it here, as the engine fails one past its request_max (`too_large`). A value
    // that would make it so has gone up in `put` parts already (blobs.ts); what is left over is
    // metadata no upload can shrink.
    const max = nc.info?.max_payload;
    if (max !== undefined && payload.byteLength > max) {
      const runId = runOf(body);
      throw new PayloadTooLargeError(runId, `${op}${runId ? ` (run ${runId})` : ""}: the request is ${payload.byteLength} bytes, over the server max_payload of ${max}`); // prettier-ignore
    }
    let data: string;
    try {
      data = dec.decode((await nc.request(apiSubject(prefix, op), payload, { timeout })).data);
    } catch (e) {
      throw fromNats(op, body, e);
    }
    const reply = JSON.parse(data) as R | Refusal;
    if (typeof reply === "object" && reply !== null && "error" in reply && isRefusal(reply)) {
      throw fromRefusal(op, body, reply, logger);
    }
    return reply;
  };
}

function isRefusal(r: object): r is Refusal {
  const e = (r as { error?: unknown }).error;
  return typeof e === "object" && e !== null && typeof (e as { code?: unknown }).code === "string";
}

function runOf(body: object): string {
  const b = body as { runId?: unknown; childRunId?: unknown };
  if (typeof b.runId === "string") return b.runId;
  if (typeof b.childRunId === "string") return b.childRunId;
  return "";
}

function fromRefusal(op: Op, body: object, r: Refusal, logger: Logger): Error {
  const runId = runOf(body);
  const message = `engine refused ${op}${runId ? ` (run ${runId})` : ""}: ${r.error.message ?? r.error.code}`;
  switch (r.error.code) {
    case "fenced":
      return new EngineFencedError(runId, message, r.error.expectLastSeq, r.error.serverCode);
    case "expired":
      return new LeaseExpiredError(op, message);
    case "invalid":
      // Inside an execution a refused append or child is a value or an identifier the journal
      // cannot hold: deterministic, so the pure core fails the run (its SerializationError path)
      // rather than retry it forever. Anywhere else it is a bad request.
      return op === "append" || op === "child"
        ? new SerializationError(runId, message)
        : new InvalidRequestError(op, message);
    case "too_large":
      return new PayloadTooLargeError(runId, message);
    case "not_found":
      return new RunNotFoundError(op, runId);
    case "internal":
      logger.error("durable: the engine answered internal — an operator must look", { op, runId, message }); // prettier-ignore
      return new EngineUnavailableError(op, "internal", message);
    default:
      return new EngineUnavailableError(op, "unavailable", message);
  }
}

function fromNats(op: Op, body: object, e: unknown): Error {
  if (e instanceof Error && e.message.includes("max_payload")) {
    return new PayloadTooLargeError(runOf(body), `${op}: ${e.message}`, { cause: e });
  }
  if (e instanceof ClosedConnectionError || e instanceof DrainingConnectionError) {
    return new ConnectionClosedError(`${op}: the NATS connection is closed`, { cause: e });
  }
  const noResponders =
    e instanceof NoRespondersError ||
    (e instanceof RequestError && e.isNoResponders()) ||
    (e instanceof Error && e.cause instanceof NoRespondersError);
  if (noResponders) {
    return new EngineUnavailableError(op, "unavailable", `${op}: no engine answered (no responders) — is durable-engine serve running on this prefix?`, { cause: e }); // prettier-ignore
  }
  if (e instanceof TimeoutError || (e instanceof Error && e.cause instanceof TimeoutError)) {
    return new EngineUnavailableError(op, "unavailable", `${op}: the engine did not answer in time`, { cause: e }); // prettier-ignore
  }
  return new EngineUnavailableError(op, "unavailable", `${op}: ${String(e)}`, { cause: e });
}
