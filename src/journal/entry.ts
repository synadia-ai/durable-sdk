// @synadia-ai/durable — the on-stream journal wire layout (language-neutral, frozen at v:1).
// See DESIGN.md §3. One ordered log per run on subject `de.journal.{runId}`.
//
// `seq` is the COMMAND position (monotonic per run, 0,1,2,…). A two-phase step shares one `seq`
// across its `step-begin` and `step-end`. `signal` and `resolve` entries do not consume a command
// `seq` (signal is matched to an await by name+order; resolve references `resolvesSeq`); they carry
// seq = NON_COMMAND_SEQ.

import { WIRE_VERSION } from "../version.js";

export const NON_COMMAND_SEQ = -1;

/**
 * A content-addressed reference to a payload blob offloaded to the blob object store (`DE_BLOBS`).
 * When an encoded entry would exceed the offload threshold, its single payload slot (`input` /
 * `value` / `data` / `error`) is written to the blob store FIRST (blob-before-pointer: a published
 * ref always resolves) and the entry carries this ref in the matching `*Ref` field instead, stamped
 * `v:2` (WIRE_VERSION_BLOBS). Names are content-addressed (`{runId}/{digest}`) so two CAS racers
 * holding DIFFERENT bytes for the same seq can never overwrite each other — the journal CAS picks
 * the surviving ref, the loser's blob is an inert orphan (writers MUST NOT delete on a lost CAS:
 * an identical-bytes racer shares the winner's object). Readers MUST verify `digest` on fetch.
 */
export interface BlobRef {
  /** Object name in the blob bucket: `{runId}/{digest}` (content-addressed; runId prefix = GC unit). */
  name: string;
  /** Content digest of the encoded value, `sha256:<64 lowercase hex>`. Readers MUST verify. */
  digest: string;
  /** Size in bytes of the encoded value blob. */
  size: number;
}

export type EntryKind =
  | "start"
  | "step-begin"
  | "step-end"
  | "now"
  | "random"
  | "uuid"
  | "sleep"
  | "await"
  | "signal"
  | "call"
  | "resolve"
  | "result"
  | "fail"
  | "continued"
  | "cancel-requested"
  | "cancelled";

interface BaseEntry {
  /** Wire-format version. */
  v: number;
  /** Command position (monotonic per run); shared by step-begin/step-end; NON_COMMAND_SEQ for signal/resolve. */
  seq: number;
  kind: EntryKind;
  /** Wall-clock ms at write time. Informational only — never used for control decisions. */
  ts: number;
}

export interface StartEntry extends BaseEntry {
  kind: "start";
  wf: string;
  wfVersion: number;
  input: unknown;
  /** Offloaded form (v:2): `input` is absent on the wire and this ref locates it. Resolved on load. */
  inputRef?: BlobRef;
  /** Transformed form (v:3): `input` is absent and this holds the base64 `PayloadCodec` output. */
  inputEnc?: string;
  /** Set when this run is a sub-workflow: where to deliver its result on completion. */
  parent?: { runId: string; seq: number; wf: string };
  /** continue-as-new chain: this run's generation (0 for the original). Default 0. */
  generation?: number;
  /** continue-as-new chain: the logical (generation-0) runId all generations share. Default = runId. */
  base?: string;
}
export interface StepBeginEntry extends BaseEntry {
  kind: "step-begin";
  name: string;
  /** Idempotency key for the side effect: `${runId}:${seq}` (exposed via ctx.key()). */
  key: string;
}
export interface StepEndEntry extends BaseEntry {
  kind: "step-end";
  name: string;
  value: unknown;
  /** Offloaded form (v:2): `value` is absent on the wire and this ref locates it. Resolved on load. */
  valueRef?: BlobRef;
  /** Transformed form (v:3): `value` is absent and this holds the base64 `PayloadCodec` output. */
  valueEnc?: string;
}
export interface ValueEntry extends BaseEntry {
  kind: "now" | "random" | "uuid";
  value: unknown;
}
export interface SleepEntry extends BaseEntry {
  kind: "sleep";
  name: string;
  /** Absolute deadline (ms since epoch), deterministic once recorded. */
  dueMs: number;
  /** Subject of the native schedule that will fire the wake (for atomic cancel). */
  schedSubj?: string;
}
export interface AwaitEntry extends BaseEntry {
  kind: "await";
  name: string;
  /** Absolute deadline (ms since epoch) when ctx.await was given a timeoutMs; a timer races the signal. */
  dueMs?: number;
}
export interface SignalEntry extends BaseEntry {
  kind: "signal";
  name: string;
  data: unknown;
  /** Offloaded form (v:2): `data` is absent on the wire and this ref locates it. Resolved on load. */
  dataRef?: BlobRef;
  /** Transformed form (v:3): `data` is absent and this holds the base64 `PayloadCodec` output. */
  dataEnc?: string;
  /** Dedup id so a redelivered signal is appended at most once. */
  signalId: string;
}
export interface CallEntry extends BaseEntry {
  kind: "call";
  name: string;
  childRunId: string;
  wf: string;
  input: unknown;
  /** Offloaded form (v:2): `input` is absent on the wire and this ref locates it. Resolved on load. */
  inputRef?: BlobRef;
  /** Transformed form (v:3): `input` is absent and this holds the base64 `PayloadCodec` output. */
  inputEnc?: string;
}
export interface ResolveEntry extends BaseEntry {
  kind: "resolve";
  /** The command `seq` (sleep/call) this completion resolves. */
  resolvesSeq: number;
  value?: unknown;
  /** Offloaded form (v:2): `value` is absent on the wire and this ref locates it. Resolved on load. */
  valueRef?: BlobRef;
  /** Transformed form (v:3): `value` is absent and this holds the base64 `PayloadCodec` output. */
  valueEnc?: string;
  /** For a failed sub-workflow (ctx.call): the child's error. */
  error?: { name: string; message: string };
  /** Offloaded form (v:2): `error` is absent on the wire and this ref locates it. Resolved on load. */
  errorRef?: BlobRef;
  /** Transformed form (v:3): `error` is absent and this holds the base64 `PayloadCodec` output. */
  errorEnc?: string;
}
export interface ResultEntry extends BaseEntry {
  kind: "result";
  value: unknown;
  /** Offloaded form (v:2): `value` is absent on the wire and this ref locates it. Resolved on load. */
  valueRef?: BlobRef;
  /** Transformed form (v:3): `value` is absent and this holds the base64 `PayloadCodec` output. */
  valueEnc?: string;
}
export interface FailEntry extends BaseEntry {
  kind: "fail";
  error: { name: string; message: string };
  /** Offloaded form (v:2): `error` is absent on the wire and this ref locates it. Resolved on load. */
  errorRef?: BlobRef;
  /** Transformed form (v:3): `error` is absent and this holds the base64 `PayloadCodec` output. */
  errorEnc?: string;
}
/** Terminal for this generation: continue-as-new started a fresh generation `nextRunId`. */
export interface ContinuedEntry extends BaseEntry {
  kind: "continued";
  /** The next generation's runId (deterministic: `${base}_g${generation}`). */
  nextRunId: string;
  /** Input handed to the next generation. */
  input: unknown;
  /** Offloaded form (v:2): `input` is absent on the wire and this ref locates it. Resolved on load. */
  inputRef?: BlobRef;
  /** Transformed form (v:3): `input` is absent and this holds the base64 `PayloadCodec` output. */
  inputEnc?: string;
  /** The next generation's number (this run's generation + 1). */
  generation: number;
  /** The chain's logical (generation-0) runId. */
  base: string;
  /** The next generation's workflow version. Optional/additive: absent on pre-versioning entries, which
   *  coalesce to the current generation's version (= same version) on re-drive. See DESIGN-versioning.md. */
  version?: number;
}

/** Records that cancellation was requested for this run (non-command; matched by stream-seq, like a signal). */
export interface CancelRequestedEntry extends BaseEntry {
  kind: "cancel-requested";
  /** Optional human-readable reason, carried onto the terminal `cancelled` entry. */
  reason?: string;
}
/** Terminal: the run was cancelled (an uncatchable hard-stop at a command boundary). */
export interface CancelledEntry extends BaseEntry {
  kind: "cancelled";
  reason?: string;
}

export type JournalEntry =
  | StartEntry
  | StepBeginEntry
  | StepEndEntry
  | ValueEntry
  | SleepEntry
  | AwaitEntry
  | SignalEntry
  | CallEntry
  | ResolveEntry
  | ResultEntry
  | FailEntry
  | ContinuedEntry
  | CancelRequestedEntry
  | CancelledEntry;

/** A journal entry together with its JetStream stream sequence (used to chain CAS appends). */
export interface LoadedEntry {
  entry: JournalEntry;
  streamSeq: number;
}

/** Stamp the wire version + timestamp onto an entry body. */
export function stamp<E extends Omit<JournalEntry, "v" | "ts">>(
  body: E,
  nowMs: number,
): E & { v: number; ts: number } {
  return { ...body, v: WIRE_VERSION, ts: nowMs };
}

/** Per-entry NATS msgID conventions (dedup) — see DESIGN.md §3 table. */
export const msgId = {
  start: (run: string): string => `${run}:start`,
  stepBegin: (run: string, seq: number): string => `${run}:${seq}:b`,
  stepEnd: (run: string, seq: number): string => `${run}:${seq}:e`,
  value: (run: string, seq: number): string => `${run}:${seq}`,
  command: (run: string, seq: number): string => `${run}:${seq}`,
  signal: (run: string, signalId: string): string => `${run}:sig:${signalId}`,
  resolve: (run: string, seq: number): string => `${run}:${seq}:r`,
  cancel: (run: string): string => `${run}:cancel`,
  terminal: (run: string): string => `${run}:terminal`,
};
