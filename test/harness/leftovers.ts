// The footprint check every integration test ends with: the library must leave NO ephemeral
// consumer on the server. Durable pulls (a worker's work + timers consumers, named and kept by
// design) are the only consumers the library creates; a journal load, a blob read and a waiting
// result() are direct gets or core subscriptions. Two client-library abstractions used to leak one
// consumer per call (a stopped-not-deleted KV watch and object-store read — LEARNINGS #56, #59),
// invisible on a default local server and fatal against a managed service's per-stream cap. This
// makes a fifth instance fail the gate instead of a customer's account — after the TEST that made
// it (setup-each.ts), so the failure names its author, and once more after the whole run.

import { jetstreamManager } from "@nats-io/jetstream";
import { type NatsConnection } from "@nats-io/nats-core";
import { connect } from "@nats-io/transport-node";
import { jsErrorCode } from "./shell/resources.js";

export interface LeftoverConsumer {
  stream: string;
  name: string;
}

/** Every consumer on the server that has no durable name — i.e. one the library did not mean to keep. */
export async function listEphemeralConsumersOn(nc: NatsConnection): Promise<LeftoverConsumer[]> {
  const jsm = await jetstreamManager(nc);
  const out: LeftoverConsumer[] = [];
  for await (const s of jsm.streams.list()) {
    for await (const c of jsm.consumers.list(s.config.name)) {
      if (c.config.durable_name === undefined || c.config.durable_name === "") {
        out.push({ stream: s.config.name, name: c.name });
      }
    }
  }
  return out;
}

export async function listEphemeralConsumers(url: string): Promise<LeftoverConsumer[]> {
  const nc = await connect({ servers: url });
  try {
    return await listEphemeralConsumersOn(nc);
  } finally {
    await nc.drain();
  }
}

export function leftoverMessage(left: LeftoverConsumer[], where: string): string {
  const sample = left
    .slice(0, 12)
    .map((l) => `${l.stream}/${l.name}`)
    .join(", ");
  return (
    `${String(left.length)} ephemeral consumer(s) left on the test server ${where} — the library ` +
    `must not leak consumers (LEARNINGS #56/#59). First: ${sample}`
  );
}

export async function assertNoEphemeralConsumers(url: string): Promise<void> {
  const left = await listEphemeralConsumers(url);
  if (left.length > 0) throw new Error(leftoverMessage(left, "after the suite"));
}

/**
 * Delete every stream on the server (KV buckets and object stores are streams too). The owner
 * of a test prefix's seven streams: the shared server is the suite's own, and under an account
 * profile (nats-server.ts) the stream cap is 16 — two prefixes — so what a test provisions must
 * be gone before the next test starts, not at the end of the run.
 */
export async function deleteAllStreams(nc: NatsConnection): Promise<number> {
  const jsm = await jetstreamManager(nc);
  const streams: string[] = [];
  for await (const s of jsm.streams.list()) streams.push(s.config.name);
  for (const name of streams) {
    try {
      await jsm.streams.delete(name);
    } catch (e) {
      if (jsErrorCode(e) !== 10059) throw e; // already gone (a KV's backing stream went with it)
    }
  }
  return streams.length;
}
