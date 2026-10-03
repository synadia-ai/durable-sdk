// Payload sizes relative to THIS server's max_payload — never a literal MiB. The test server runs
// under a profile (nats-server.ts): 8 MiB on `ngs`, 64 KiB on `tight`, 1 MiB on `none`. A test
// that wrote "1.5 MiB is over the limit" was true on exactly one of them, and under `ngs` every
// offload test quietly stopped offloading — the assertions that checked for a ref caught it, the
// ones that only checked the value would have passed on an inline payload.

import { type NatsConnection } from "@nats-io/nats-core";

/** The connected server's max_payload, which the library also reads to place its offload threshold. */
export function maxPayload(nc: NatsConnection): number {
  const mp = nc.info?.max_payload;
  if (mp === undefined) throw new Error("the connection has not reported max_payload");
  return mp;
}

/** A byte count `factor` × max_payload — comfortably over the limit at 1.5, twice it at 2. */
export function over(nc: NatsConnection, factor = 1.5): number {
  return Math.ceil(maxPayload(nc) * factor);
}
