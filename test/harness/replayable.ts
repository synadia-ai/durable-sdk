// `executeRun` never executes a journal that already holds a terminal — it reports the recorded
// outcome and runs nothing. A test that wants to watch a FINISHED run REPLAY (recorded steps not
// re-run, recorded values returned) therefore hands it the history the terminal was computed from.
// The replay then ends by losing the terminal CAS to the entry that is really there and adopting
// it, so the outcome is still the journal's.

import { type LoadedEntry } from "../../src/journal/entry.js";

const TERMINAL = new Set(["result", "fail", "continued", "cancelled"]);

export function beforeTerminal(loaded: LoadedEntry[]): LoadedEntry[] {
  const at = loaded.findIndex((l) => TERMINAL.has(l.entry.kind));
  return at < 0 ? loaded : loaded.slice(0, at);
}
