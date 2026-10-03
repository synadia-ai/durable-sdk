// @synadia-ai/durable — connection convenience helpers. The library is BYO-connection: you pass a
// NatsConnection to createClient/createWorker. These helpers just shape ConnectionOptions, matching
// the synadia-agents conventions (generic, un-prefixed NATS_URL/NATS_CONTEXT/NATS_CREDS).

import { type ConnectionOptions } from "@nats-io/nats-core";

/** Reconnect defaults: never give up, retry through an unreachable-at-startup server. */
export const RECONNECT_DEFAULTS = Object.freeze({
  maxReconnectAttempts: -1,
  reconnectTimeWait: 2000,
  reconnectJitter: 200,
  waitOnFirstConnect: true,
});

/** Fill reconnect defaults as fallbacks without overwriting any caller-set field. */
export function withReconnectDefaults(opts: ConnectionOptions = {}): ConnectionOptions {
  return { ...RECONNECT_DEFAULTS, ...opts };
}

/**
 * Parse a (comma-separated) NATS URL into ConnectionOptions.
 * v0.1: extracts the server list. TODO: userinfo creds + nats-context file loading (loadContextOptions).
 */
export function parseNatsUrl(url: string): ConnectionOptions {
  const servers = url
    .split(",")
    .map((s) => s.trim())
    .filter((s): s is string => s.length > 0);
  return { servers };
}
