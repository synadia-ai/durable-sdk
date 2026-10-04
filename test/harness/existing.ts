// The harness's mode for an EXISTING server (docs/findings.md F28, F29): `DE_NATS_URL` names a
// server the run does not own, instead of the nats-server the harness spawns. Everything here is
// vitest-free, for global-setup.ts (the main process) and the test side alike.
//
//   DE_NATS_URL     the server (a URL the client and the engine both reach); switches the mode on
//   DE_NATS_CREDS   a creds file for it, when it needs one (read by the client library and the
//                   engine's `--creds`, never by this harness)
//   DE_PREFIX       the run's one prefix (default a generated `de-<run id>`)
//   DE_ENGINE_URL   an engine that already serves the prefix, by its /metrics address
//                   (`http://host:port`); without it the harness starts one for the run
//   DE_CLEANUP      `delete`: delete the prefix's seven at the run's end (default: keep them)
//
// No spawn, no account profile (the server's own limits apply), one prefix and one engine for the
// run, a cleanup that purges what each test wrote under the prefix and never deletes a stream, the
// leftovers check on the prefix's streams only, and every wait scaled by the measured round trip.

import { randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { jetstreamManager, type JetStreamManager } from "@nats-io/jetstream";
import {
  credsAuthenticator,
  type ConnectionOptions,
  type NatsConnection,
} from "@nats-io/nats-core";
import { connect as tcpConnect } from "@nats-io/transport-node";
import { jsErrorCode } from "./shell/resources.js";

export interface ExistingServer {
  url: string;
  creds: string | undefined;
  prefix: string;
  engineUrl: string | undefined;
  deleteAtEnd: boolean;
}

/** The mode's settings from the environment, or null (the spawned server, as before). */
export function existingFromEnv(): ExistingServer | null {
  const url = process.env.DE_NATS_URL;
  if (url === undefined || url === "") return null;
  const creds = process.env.DE_NATS_CREDS;
  const cleanup = process.env.DE_CLEANUP ?? "";
  if (cleanup !== "" && cleanup !== "delete" && cleanup !== "keep") {
    throw new Error(`DE_CLEANUP="${cleanup}": expected delete or keep`);
  }
  const prefix = process.env.DE_PREFIX || `de-${randomBytes(4).toString("hex")}`;
  if (!/^[A-Za-z0-9_-]+$/.test(prefix)) throw new Error(`DE_PREFIX="${prefix}": one subject token, no dots`); // prettier-ignore
  return {
    url,
    creds: creds === undefined || creds === "" ? undefined : creds,
    prefix,
    engineUrl: process.env.DE_ENGINE_URL || undefined,
    deleteAtEnd: cleanup === "delete",
  };
}

/** True in the mode (the env is the same in every test worker). */
export const inExistingMode = (): boolean => (process.env.DE_NATS_URL ?? "") !== "";

/** The connection options a test needs for the server: the creds, when the mode names any. */
export function withCreds(opts: ConnectionOptions): ConnectionOptions {
  const creds = process.env.DE_NATS_CREDS;
  if (!inExistingMode() || creds === undefined || creds === "") return opts;
  return { ...opts, authenticator: credsAuthenticator(readFileSync(creds)) };
}

/** `connect` with the mode's creds (every test and the harness connect through this). */
export function connect(opts: ConnectionOptions): Promise<NatsConnection> {
  return tcpConnect(withCreds(opts));
}

/**
 * The factor every wait is scaled by, from the round trip measured at the start: 1 up to 20 ms
 * (a local server), then one more for every 100 ms, so 2.5 at the proxy's 150 ms. The fabric's
 * test repo used a flat WANFactor of 2 on NGS; this one follows the link it measured.
 */
export function rttFactor(rttMs: number): number {
  if (rttMs <= 20) return 1;
  return Math.round((1 + rttMs / 100) * 100) / 100;
}

/** The median of five round trips to the server, in ms. */
export async function measureRtt(nc: NatsConnection): Promise<number> {
  const samples: number[] = [];
  for (let i = 0; i < 5; i++) samples.push(await nc.rtt());
  return samples.sort((a, b) => a - b)[2]!;
}

/** The prefix's seven backing streams (KV buckets and the object store are streams). */
export function prefixStreams(prefix: string): string[] {
  const up = prefix.toUpperCase();
  return [
    `${up}_JOURNAL`,
    `${up}_WORK`,
    `${up}_TIMERS`,
    `KV_${up}_RUNS`,
    `KV_${up}_LEASES`,
    `KV_${up}_PARKED`,
    `OBJ_${up}_BLOBS`,
  ];
}

export interface LeftoverConsumer {
  stream: string;
  name: string;
}

/**
 * The consumers without a durable name on the prefix's streams: the leftovers check, scoped. Those
 * in `known` (`stream/name`, there before the test or the run) are not the run's: on a prefix the
 * run shares, another client's watch is not a leak of this one's.
 */
export async function prefixEphemeralConsumers(
  nc: NatsConnection,
  prefix: string,
  known: Set<string> = new Set(),
): Promise<LeftoverConsumer[]> {
  const jsm = await jetstreamManager(nc);
  const out: LeftoverConsumer[] = [];
  for (const stream of prefixStreams(prefix)) {
    try {
      for await (const c of jsm.consumers.list(stream)) {
        if (
          (c.config.durable_name === undefined || c.config.durable_name === "") &&
          !known.has(`${stream}/${c.name}`)
        ) {
          // prettier-ignore
          out.push({ stream, name: c.name });
        }
      }
    } catch (e) {
      if (jsErrorCode(e) !== 10059) throw e; // not provisioned (yet)
    }
  }
  return out;
}

/** Each of the prefix's streams: subject → message count (absent streams are empty); and the
 * consumers on them, by `stream/name`. */
export interface Snapshot {
  subjects: Map<string, Map<string, number>>;
  consumers: Set<string>;
}

async function consumersOf(jsm: JetStreamManager, stream: string): Promise<string[]> {
  const out: string[] = [];
  try {
    for await (const c of jsm.consumers.list(stream)) out.push(`${stream}/${c.name}`);
  } catch (e) {
    if (jsErrorCode(e) !== 10059) throw e;
  }
  return out;
}

async function subjectsOf(jsm: JetStreamManager, stream: string): Promise<Map<string, number>> {
  try {
    const si = await jsm.streams.info(stream, { subjects_filter: ">" });
    return new Map(Object.entries(si.state.subjects ?? {}));
  } catch (e) {
    if (jsErrorCode(e) === 10059) return new Map();
    throw e;
  }
}

/** What the prefix holds before a test: the cleanup purges only what changed after it. */
export async function snapshot(nc: NatsConnection, prefix: string): Promise<Snapshot> {
  const jsm = await jetstreamManager(nc);
  const streams = prefixStreams(prefix);
  const subjects = await Promise.all(streams.map((s) => subjectsOf(jsm, s)));
  const consumers = await Promise.all(streams.map((s) => consumersOf(jsm, s)));
  return {
    subjects: new Map(streams.map((s, i) => [s, subjects[i]!])),
    consumers: new Set(consumers.flat()),
  };
}

/**
 * The cleanup after a test: every subject of the prefix's streams that is new or grew since
 * `before` is purged — the journal, work and timer subjects of the test's runs, their KV keys
 * (`$KV.<bucket>.<runId>`) and their blobs' chunks and meta. Never a stream, never a subject
 * outside the prefix, never one the test did not touch. And the durables the engine made for the
 * test's workflow names (`de-worker-<wf>`, `de-timer-<wf>`): they would pile up past an account's
 * per-stream consumer cap over a suite of ~150 names; a durable that was there before stays.
 * Returns how many subjects were purged.
 */
export async function purgeSince(
  nc: NatsConnection,
  prefix: string,
  before: Snapshot,
): Promise<number> {
  const jsm = await jetstreamManager(nc);
  let purged = 0;
  for (const stream of prefixStreams(prefix)) {
    const was = before.subjects.get(stream) ?? new Map<string, number>();
    const now = await subjectsOf(jsm, stream);
    const touched = [...now].filter(([s, n]) => was.get(s) !== n).map(([s]) => s);
    // Pipelined: one round trip for the lot, not one per subject.
    await Promise.all(touched.map((filter) => jsm.streams.purge(stream, { filter })));
    purged += touched.length;
  }
  for (const stream of prefixStreams(prefix).slice(0, 3)) {
    const fresh = (await consumersOf(jsm, stream)).filter((c) => !before.consumers.has(c));
    await Promise.all(
      fresh.map(async (c) => {
        try {
          await jsm.consumers.delete(stream, c.slice(stream.length + 1));
        } catch (e) {
          if (jsErrorCode(e) !== 10014) throw e; // already gone
        }
      }),
    );
  }
  return purged;
}

/** `DE_CLEANUP=delete`: the prefix's seven, at the run's end, and nothing else. */
export async function deletePrefixStreams(nc: NatsConnection, prefix: string): Promise<string[]> {
  const jsm = await jetstreamManager(nc);
  const deleted: string[] = [];
  for (const name of prefixStreams(prefix)) {
    try {
      await jsm.streams.delete(name);
      deleted.push(name);
    } catch (e) {
      if (jsErrorCode(e) !== 10059) throw e;
    }
  }
  return deleted;
}
