// @synadia-ai/durable — the engine's API subjects. The SDK knows the prefix and one subject pattern,
// `{prefix}.api.v1.<op>` (the engine's API.md "Transport"); the streams, buckets and
// consumers behind it are the engine's, and their names appear nowhere in this package.

/** The default prefix: the engine's and the library's (`de`). */
export const DEFAULT_PREFIX = "de";

/** The 15 ops of API v1. */
export type Op =
  | "start"
  | "result"
  | "status"
  | "signal"
  | "cancel"
  | "poll"
  | "load"
  | "append"
  | "wake"
  | "child"
  | "done"
  | "extend"
  | "info"
  | "put"
  | "get";

/** The subject a request for `op` is published to. */
export function apiSubject(prefix: string, op: Op): string {
  return `${prefix}.api.v1.${op}`;
}
