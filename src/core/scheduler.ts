// @synadia-ai/durable — the Scheduler seam: how ctx.sleep arranges a future wake. The pure core
// depends only on this interface; the NATS shell implements it with native JetStream scheduling.

export interface Scheduler {
  /**
   * Schedule a wake for `runId` at absolute `dueMs`. When it fires, a "timer" work item resolving
   * command `seq` must reach the worker(s) for `workflow`. Must be idempotent: re-scheduling the same
   * (runId, seq) should not create a second wake within the dedup window.
   */
  scheduleWake(runId: string, workflow: string, seq: number, dueMs: number): Promise<void>;
}

/** Default used when no scheduler is wired (e.g. a client-only setup); errors if ctx.sleep is used. */
export const UNAVAILABLE_SCHEDULER: Scheduler = {
  scheduleWake() {
    return Promise.reject(new Error("ctx.sleep requires a worker-provided scheduler"));
  },
};
