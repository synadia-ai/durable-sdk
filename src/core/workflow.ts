// @synadia-ai/durable — workflow definition.

import { assertToken } from "../internal/token.js";
import { type Workflow, type WorkflowFn } from "../types.js";

/**
 * Define a workflow. Returns a typed token used both to register the function on a worker and to
 * start/call it with full input/output inference.
 *
 *   const charge = defineWorkflow("charge", async (ctx, input: CartInput): Promise<Receipt> => { … });
 */
export function defineWorkflow<I, O>(
  name: string,
  fn: WorkflowFn<I, O>,
  opts?: { version?: number },
): Workflow<I, O> {
  if (!name) throw new Error("defineWorkflow: name is required");
  // The name becomes a subject token (`de.work.{name}`), a consumer name, and part of KV records —
  // an unsafe one would fail confusingly at the consumer on first dispatch. Fail loud at definition.
  assertToken("workflow name", name);
  const version = opts?.version ?? 1;
  if (!Number.isInteger(version) || version < 1) {
    throw new Error("defineWorkflow: version must be a positive integer");
  }
  return Object.freeze({ name, version, __fn: fn });
}
