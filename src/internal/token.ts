// @synadia-ai/durable — the subject-token ∩ KV-key guard. Pure (no transport imports) so BOTH the
// core (defineWorkflow) and the NATS shell (client/names) can fail loud at the earliest boundary.

// A runId / workflow name is used as BOTH a subject token (`de.journal.{runId}`) AND a KV key
// (`DE_RUNS`). KV keys reject characters that subjects allow (notably `~`), so require the safe
// intersection: ASCII alphanumerics, `-`, `_`. (continue-as-new chains use `_g{n}`; `ctx.call`
// children use `_{seq}` — both within this set.)
const SAFE_TOKEN = /^[A-Za-z0-9_-]+$/;

/** Throw unless `value` is safe as a subject token AND a KV key (runIds / workflow names). */
export function assertToken(kind: string, value: string): void {
  if (!SAFE_TOKEN.test(value)) {
    throw new Error(
      `${kind} must be ASCII alphanumeric with '-' or '_' (a valid subject token and KV key): got "${value}"`,
    );
  }
}
