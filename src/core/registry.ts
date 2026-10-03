// @synadia-ai/durable — the workflow registry: name → version → workflow. Pure (no NATS imports); a
// worker builds one to dispatch each run to the EXACT (name, version) its journal recorded on `start`.
// Version-pinned dispatch: a run always runs the code it began with. See docs/DESIGN-versioning.md.

import { type AnyWorkflow } from "../types.js";

export interface WorkflowRegistry {
  /** Distinct workflow names — one work + one timer consumer is bound per name. */
  names(): string[];
  /** The exact (name, version) workflow, or undefined if that version isn't registered here. */
  resolve(name: string, version: number): AnyWorkflow | undefined;
}

/**
 * Build a registry from a worker's workflow list. Multiple versions of the same name may coexist (that
 * is the whole point — old runs keep running their version while new code is deployed). A duplicate
 * `(name, version)` throws: it would make dispatch ambiguous, so we fail loud at worker construction.
 */
export function buildRegistry(workflows: AnyWorkflow[]): WorkflowRegistry {
  const byName = new Map<string, Map<number, AnyWorkflow>>();
  for (const wf of workflows) {
    let versions = byName.get(wf.name);
    if (!versions) {
      versions = new Map<number, AnyWorkflow>();
      byName.set(wf.name, versions);
    }
    if (versions.has(wf.version)) {
      throw new Error(
        `createWorker: duplicate workflow "${wf.name}" version ${wf.version} ` +
          `(each (name, version) must be registered at most once)`,
      );
    }
    versions.set(wf.version, wf);
  }
  return {
    names: (): string[] => [...byName.keys()],
    resolve: (name, version): AnyWorkflow | undefined => byName.get(name)?.get(version),
  };
}
