// @synadia-ai/durable — the client: start runs, observe them, signal and cancel them, through the
// engine's five client ops (durable-engine docs/API.md: start, result, status, signal, cancel).
// The library's Client and Handle, unchanged in shape; what was the shell's (the journal reads,
// the status hint subscription, provisioning, offload) is the engine's now.

import { type NatsConnection } from "@nats-io/nats-core";
import {
  CancelledError,
  ConnectionClosedError,
  DurableTimeoutError,
  WorkflowFailure,
} from "../errors.js";
import { type Logger, SILENT_LOGGER } from "../internal/logger.js";
import { type Metrics } from "../internal/metrics.js";
import { assertToken } from "../internal/token.js";
import { type OffloadOptions } from "../journal/blobs.js";
import { type PayloadCodec } from "../journal/payload-codec.js";
import {
  type Handle,
  type RunState,
  type RunStatus,
  type StartOptions,
  type Workflow,
} from "../types.js";
import { EngineUnavailableError } from "./errors.js";
import { DEFAULT_PREFIX } from "./names.js";
import { slot, unslot } from "./payload.js";
import { createTransport } from "./transport.js";

/** How long one `result` request asks the engine to wait (the engine caps it at its `max_poll_wait_ms`). */
const RESULT_WAIT_MS = 30_000;

/** Back-off between `result` requests after a transient refusal (no engine, a timeout). */
const RESULT_RETRY_MS = 500;

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

export interface Client {
  start<I, O>(workflow: Workflow<I, O>, input: I, opts?: StartOptions): Promise<Handle<O>>;
  get<O = unknown>(runId: string): Handle<O>;
  signal<T>(runId: string, name: string, data: T): Promise<void>;
  /**
   * Request cancellation of a run (idempotent; no-op on an already-terminal run). Cancellation is an
   * uncatchable hard-stop at the run's next COMMAND BOUNDARY: recorded commands still replay, but the
   * next fresh `ctx.*` op — or the resume of a step that began and never finished — halts before
   * executing; `ctx.onCancel` handlers then run (LIFO) and the run reaches the `cancelled` terminal,
   * cascading to in-flight children and forwarding across continue-as-new. Pure compute BETWEEN
   * commands is not preempted: a run that returns without crossing a stop point completes instead
   * (completion wins). See PROTOCOL.md §11.7.
   */
  cancel(runId: string, opts?: { reason?: string }): Promise<void>;
}

export interface ClientOptions {
  nc: NatsConnection;
  /** The engine's prefix (default `de`): requests go to `{prefix}.api.v1.<op>`. */
  prefix?: string;
  /**
   * Payload encryption at rest (default: none), as the library's: every worker and client on a
   * prefix must hold the SAME codec. Under one, inputs and signal data travel as `inputEnc` /
   * `dataEnc`, results come back as `valueEnc` / `errorEnc` and are decoded here, and `status()`
   * carries no payload (the engine cannot read one).
   */
  payloadCodec?: PayloadCodec;
  /** Optional logger (default: silent). */
  logger?: Logger;
  /** Accepted for the library's API; the client emits no events of its own (the engine counts). */
  metrics?: Metrics;
  /** The library's; provisioning is the engine's now (`durable-engine serve --stream-max-bytes`). Ignored. */
  streamMaxBytes?: number;
  /** The library's; offload is the engine's now (`info`'s `offload_threshold`). Ignored. */
  offload?: OffloadOptions;
}

interface ResultReply {
  pending?: true;
  runId?: string;
  kind?: "result" | "fail" | "cancelled";
  value?: unknown;
  valueEnc?: string;
  error?: { name: string; message: string };
  errorEnc?: string;
  reason?: string;
}

interface StatusReply {
  status: RunState;
  wf?: string;
  version?: number;
  updatedMs?: number;
  next?: string;
  detail?: string;
  dueMs?: number;
}

export function createClient(opts: ClientOptions): Client {
  const prefix = opts.prefix ?? DEFAULT_PREFIX;
  const logger = opts.logger ?? SILENT_LOGGER;
  const request = createTransport(opts.nc, prefix, logger);
  const codec = opts.payloadCodec;

  // A `result` request in flight across a disconnect loses its reply (the engine answers an inbox
  // nobody is subscribed to for that moment): the wait is cut short at the next reconnect and the
  // engine asked again, instead of waiting out the request's timeout. One status watch per client,
  // started by the first result() and ended by the connection's close.
  let reconnected: Promise<void> | undefined;
  function nextReconnect(): Promise<void> {
    reconnected ??= (async () => {
      for await (const s of opts.nc.status()) if (s.type === "reconnect") break;
    })().finally(() => {
      reconnected = undefined;
    });
    return reconnected;
  }

  function closedError(runId: string): ConnectionClosedError {
    return new ConnectionClosedError(
      `run ${runId}: the NATS connection closed while waiting for the result — the run ` +
        `itself is unaffected; call client.get("${runId}").result() on a live connection`,
    );
  }

  function makeHandle<O>(runId: string): Handle<O> {
    return {
      runId,
      async result(timeoutMs = 30_000): Promise<O> {
        const deadline = Date.now() + timeoutMs;
        // The engine follows `continued` itself and answers the generation that returned: the
        // client re-asks with `waitMs` until a terminal or the caller's deadline, nothing more.
        for (;;) {
          if (opts.nc.isClosed()) throw closedError(runId);
          const remaining = deadline - Date.now();
          const waitMs = Math.max(0, Math.min(RESULT_WAIT_MS, remaining));
          let r: ResultReply | undefined;
          try {
            // The request may outlive the caller's deadline by its margin when the engine is
            // unresponsive; the race below keeps result() to the deadline it was given.
            r = await raceDeadline(request<ResultReply>("result", { runId, waitMs }, { waitMs }), deadline, nextReconnect()); // prettier-ignore
          } catch (e) {
            if (e instanceof ConnectionClosedError) throw closedError(runId);
            if (!(e instanceof EngineUnavailableError)) throw e;
            logger.debug("durable: result() request failed; retrying", { runId, error: String(e) }); // prettier-ignore
            if (Date.now() < deadline) await delay(Math.min(RESULT_RETRY_MS, deadline - Date.now())); // prettier-ignore
          }
          if (r !== undefined && r.pending !== true && r.kind !== undefined) {
            const gen = r.runId ?? runId;
            if (r.kind === "cancelled") throw new CancelledError(gen);
            if (r.kind === "result") {
              if (r.valueEnc !== undefined) return (await unslot(gen, r.valueEnc, codec)) as O;
              return r.value as O;
            }
            const err =
              r.errorEnc !== undefined
                ? ((await unslot(gen, r.errorEnc, codec)) as { name: string; message: string })
                : (r.error ?? { name: "Error", message: "" });
            throw new WorkflowFailure(gen, `${err.name}: ${err.message}`);
          }
          if (Date.now() >= deadline) {
            if (opts.nc.isClosed()) throw closedError(runId);
            throw new DurableTimeoutError(`run ${runId}: result timed out after ${timeoutMs}ms`);
          }
        }
      },
      async status(): Promise<RunStatus> {
        const r = await request<StatusReply>("status", { runId });
        if (r.status === "pending" && r.wf === undefined) {
          return { state: "pending", workflow: "", version: 0, updatedMs: 0 };
        }
        return {
          state: r.status,
          workflow: r.wf ?? "",
          version: r.version ?? 0,
          updatedMs: r.updatedMs ?? 0,
          ...(r.next !== undefined ? { next: r.next } : {}),
          ...(r.detail !== undefined ? { detail: r.detail } : {}),
          ...(r.dueMs !== undefined ? { dueMs: r.dueMs } : {}),
        };
      },
    };
  }

  return {
    async start<I, O>(workflow: Workflow<I, O>, input: I, o?: StartOptions): Promise<Handle<O>> {
      // Identifiers are checked here, so a bad one fails before a round trip. Without a runId
      // the engine mints one.
      if (o?.runId !== undefined) assertToken("runId", o.runId);
      assertToken("workflow name", workflow.name);
      const r = await request<{ runId: string }>("start", {
        ...(o?.runId !== undefined ? { runId: o.runId } : {}),
        wf: workflow.name,
        version: workflow.version,
        ...(await slot("input", input, codec, o?.runId ?? "")),
      });
      return makeHandle<O>(r.runId);
    },

    get<O = unknown>(runId: string): Handle<O> {
      return makeHandle<O>(runId);
    },

    async signal<T>(runId: string, name: string, data: T): Promise<void> {
      assertToken("runId", runId);
      await request("signal", { runId, name, ...(await slot("data", data, codec, runId)) });
    },

    async cancel(runId: string, o?: { reason?: string }): Promise<void> {
      assertToken("runId", runId);
      await request("cancel", { runId, ...(o?.reason !== undefined ? { reason: o.reason } : {}) });
    },
  };
}

/** Settle with `p`, or with `undefined` at `deadline` or at `cut` (the caller asks again or times out). */
function raceDeadline<T>(
  p: Promise<T>,
  deadline: number,
  cut: Promise<void>,
): Promise<T | undefined> {
  const ms = deadline - Date.now();
  let t: ReturnType<typeof setTimeout> | undefined;
  const timer = new Promise<undefined>((r) => {
    t = setTimeout(() => r(undefined), Math.max(0, ms) + 50);
  });
  return Promise.race([p, timer, cut.then(() => undefined)]).finally(() => clearTimeout(t));
}
