// @synadia-ai/durable — resource + subject naming (prefix configurable, default "de").
// See DESIGN.md §5. runIds and workflow names must be single NATS subject tokens (no "." " " "*" ">").

import { inject } from "vitest";

export interface Names {
  prefix: string;
  journalStream: string;
  workStream: string;
  runsKv: string;
  leasesKv: string;
  /** Index of runs asleep on a timer (runId → the wake that must arrive). See `parked.ts`. */
  parkedKv: string;
  /** Object-store bucket for offloaded payload blobs (backing stream `OBJ_<bucket>`). */
  blobsBucket: string;
  journalSubjectAll: string;
  workSubjectAll: string;
  journalSubject(runId: string): string;
  /**
   * The core subject a run's status-KV record is published on (`$KV.{bucket}.{runId}`). A plain
   * subscription on it is the wake-up hint `result()` uses — no consumer, no JetStream API call.
   */
  runsHintSubject(runId: string): string;
  workSubject(workflow: string): string;
  timersStream: string;
  timerSubjectAll: string;
  timerSchedSubject(runId: string, seq: number): string;
  timerWakeSubject(workflow: string): string;
}

export function names(given = "de"): Names {
  // durable-sdk: against an existing server every prefix is the run's one (harness/existing.ts).
  const prefix = inject("runPrefix") ?? given;
  const up = prefix.toUpperCase();
  return {
    prefix,
    journalStream: `${up}_JOURNAL`,
    workStream: `${up}_WORK`,
    runsKv: `${up}_RUNS`,
    leasesKv: `${up}_LEASES`,
    parkedKv: `${up}_PARKED`,
    blobsBucket: `${up}_BLOBS`,
    journalSubjectAll: `${prefix}.journal.*`,
    workSubjectAll: `${prefix}.work.*`,
    journalSubject: (runId) => `${prefix}.journal.${runId}`,
    runsHintSubject: (runId) => `$KV.${up}_RUNS.${runId}`,
    workSubject: (workflow) => `${prefix}.work.${workflow}`,
    timersStream: `${up}_TIMERS`,
    timerSubjectAll: `${prefix}.timer.>`,
    timerSchedSubject: (runId, seq) => `${prefix}.timer.sched.${runId}.${seq}`,
    timerWakeSubject: (workflow) => `${prefix}.timer.wake.${workflow}`,
  };
}

// The token guard lives in internal/ (pure) so defineWorkflow can apply it too; re-exported here
// because the shell is its historical home and the natural place shell code imports it from.
export { assertToken } from "../../../src/internal/token.js";
