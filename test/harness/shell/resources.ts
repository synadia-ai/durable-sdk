// From the library's src/nats/resources.ts at 7456c75: the one helper the test harness used from
// it. Provisioning itself is the engine's (engine.ts `provision`).

/** The JetStream API error code on an error, if it carries one. */
export function jsErrorCode(e: unknown): number | undefined {
  if (typeof e === "object" && e !== null && "code" in e) {
    const code: unknown = e.code;
    if (typeof code === "number") return code;
  }
  return undefined;
}
