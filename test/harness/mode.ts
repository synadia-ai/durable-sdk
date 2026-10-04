// The existing-server mode (existing.ts) as a test sees it. `WAN`: what a test's own timeout is scaled by, the
// factor from the round trip measured at the start (vitest fixes a test's timeout when it collects
// it, so the harness cannot reach one given in the test). And what the mode cannot run, skipped
// by name: a test that kills or
// restarts the server, cuts or stalls its connections from the outside (a proxy of its own between
// it and the server), or deletes the prefix's streams. The suite's title says why in the mode, so
// the reporter's skipped line names the mode and the reason.

import { describe, inject } from "vitest";

/** True against an existing server. */
export const EXISTING = inject("runPrefix") != null;

/** The run's round-trip factor (1 but against an existing server over a WAN): `60_000 * WAN`. */
export const WAN = inject("rttFactor");

export const OWN_SERVER = "kills and restarts a server of its own";
export const OWN_LINK = "a proxy of its own between it and the server, to add round trips, cut or stall the link (a server the run does not own may be TLS, which a byte proxy cannot carry)"; // prettier-ignore
export const DELETES_STREAMS = "deletes one of the prefix's streams";

/** `title`, and in the existing-server mode why it is skipped there. */
export function unlessExisting(title: string, why: string): string {
  return EXISTING ? `${title} [skipped against an existing server (DE_NATS_URL): ${why}]` : title;
}

/** `describe.skipIf(!available)`, skipped in the mode too, with `why` in its title there. */
export function describeUnlessExisting(why: string, available: boolean) {
  return (title: string, fn: () => void): void => {
    describe.skipIf(!available || EXISTING)(unlessExisting(title, why), fn);
  };
}
