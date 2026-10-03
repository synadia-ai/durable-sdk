// @synadia-ai/durable — injected logging. The library never calls console.* directly; callers
// pass a Logger (default: silent), mirroring the synadia-agents convention.

export interface Logger {
  debug(message: string, fields?: Record<string, unknown>): void;
  info(message: string, fields?: Record<string, unknown>): void;
  warn(message: string, fields?: Record<string, unknown>): void;
  error(message: string, fields?: Record<string, unknown>): void;
}

export const SILENT_LOGGER: Logger = Object.freeze({
  debug() {},
  info() {},
  warn() {},
  error() {},
});
