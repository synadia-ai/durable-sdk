// @synadia-ai/durable — the sub-workflow seam: how ctx.call starts a child run and links it back to
// the parent. The pure core depends only on this interface; the NATS shell implements it.

export interface ChildStart {
  childRunId: string;
  workflow: string;
  version: number;
  input: unknown;
  /** Where to deliver the child's result when it finishes. */
  parent: { runId: string; seq: number; wf: string };
}

export interface Children {
  /** Start a child run (idempotently). When it finishes, the worker delivers a `child-done` to `parent`. */
  start(spec: ChildStart): Promise<void>;
}

/** Default used when no child starter is wired; errors if ctx.call is used. */
export const UNAVAILABLE_CHILDREN: Children = {
  start() {
    return Promise.reject(new Error("ctx.call requires a worker-provided child starter"));
  },
};
