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
//   DE_CLEANUP      `delete` or `keep` the prefix's seven at the run's end; by default a generated
//                   prefix is deleted (only this run can have made it) and a DE_PREFIX one kept
//   DE_TENANT       the tests run as this tenant of an engine in tenancy mode (`serve --tenancy`,
//                   the engine's PR #7): the token the tenant's import places, `{prefix}.{tenant}.…`
//   DE_ADMIN_URL    where the harness reads the engine's streams and consumers (default DE_NATS_URL):
//   DE_ADMIN_CREDS  as a tenant the tests' own connection sees none (default DE_NATS_CREDS)
//
// No spawn, no account profile (the server's own limits apply), one prefix and one engine for the
// run, a cleanup that purges what each test wrote under the prefix and never deletes a stream
// during the run, the leftovers check on the prefix's streams only, and every wait scaled by the
// measured round trip. A test that failed or timed out is cleaned like any other, and its runs'
// blobs are deleted by name, after it and again at the run's end (docs/findings.md F33).
//
// As a tenant (DE_TENANT, docs/findings.md F36) the seven are the engine account's, shared by every
// tenant, and the tests reach the engine through an import: everything above is read through the
// admin connection and counts the tenant's subjects (`{prefix}.{tenant}.…`, `$KV.<bucket>.{tenant}.…`),
// objects (`{tenant}/…`) and durables (`{prefix}~{tenant}~…`) alone. Another tenant's are never
// purged or deleted, and the seven never are.

import { randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { jetstreamManager, type JetStreamManager } from "@nats-io/jetstream";
import {
  credsAuthenticator,
  type ConnectionOptions,
  type NatsConnection,
} from "@nats-io/nats-core";
import { connect as tcpConnect } from "@nats-io/transport-node";
import { EngineBlobs, sizesOf } from "../../src/engine/blobs.js";
import { createTransport } from "../../src/engine/transport.js";
import { jsErrorCode } from "./shell/resources.js";

export interface ExistingServer {
  url: string;
  creds: string | undefined;
  prefix: string;
  engineUrl: string | undefined;
  /** The prefix is the run's own (no DE_PREFIX): generated here, so nobody else's. */
  generated: boolean;
  deleteAtEnd: boolean;
  /** DE_TENANT: the tests run as this tenant of an engine in tenancy mode. */
  tenant: string | undefined;
  /** DE_ADMIN_URL / DE_ADMIN_CREDS: the harness's view of the engine's streams. */
  adminUrl: string;
  adminCreds: string | undefined;
}

/** What the harness's checks and cleanups count: the prefix, and in tenancy mode one tenant of it. */
export interface Scope {
  prefix: string;
  tenant?: string | undefined;
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
  const given = process.env.DE_PREFIX ?? "";
  const prefix = given || `de-${randomBytes(4).toString("hex")}`;
  if (!/^[A-Za-z0-9_-]+$/.test(prefix)) throw new Error(`DE_PREFIX="${prefix}": one subject token, no dots`); // prettier-ignore
  const tenant = process.env.DE_TENANT || undefined;
  if (tenant !== undefined) {
    if (!/^[A-Za-z0-9_-]+$/.test(tenant)) throw new Error(`DE_TENANT="${tenant}": one subject token, no dots`); // prettier-ignore
    // The tenant's import names the prefix, and the engine in the other account serves it: the run
    // cannot make it up, and the seven are every tenant's, never the run's to delete.
    if (given === "") throw new Error("DE_TENANT needs DE_PREFIX: the prefix the tenant's import and the engine use"); // prettier-ignore
    if (process.env.DE_ENGINE_URL === undefined || process.env.DE_ENGINE_URL === "") throw new Error("DE_TENANT needs DE_ENGINE_URL: the engine in tenancy mode runs in the engine's account, not the harness's"); // prettier-ignore
    if (cleanup === "delete") throw new Error("DE_CLEANUP=delete with DE_TENANT: the seven are every tenant's"); // prettier-ignore
  }
  return {
    url,
    creds: creds === undefined || creds === "" ? undefined : creds,
    prefix,
    engineUrl: process.env.DE_ENGINE_URL || undefined,
    generated: given === "",
    // A generated prefix is deleted unless DE_CLEANUP=keep; a given one kept unless =delete.
    deleteAtEnd: cleanup === "delete" || (cleanup === "" && given === ""),
    tenant,
    ...adminFromEnv(),
  };
}

/** DE_ADMIN_URL and DE_ADMIN_CREDS, each defaulting to the tests' own (DE_NATS_URL, DE_NATS_CREDS). */
export function adminFromEnv(): { adminUrl: string; adminCreds: string | undefined } {
  const adminUrl = process.env.DE_ADMIN_URL || process.env.DE_NATS_URL || "";
  const adminCreds = process.env.DE_ADMIN_URL
    ? process.env.DE_ADMIN_CREDS || undefined
    : process.env.DE_ADMIN_CREDS || process.env.DE_NATS_CREDS || undefined;
  return { adminUrl, adminCreds };
}

/** The scope of the run's checks, the same in every test worker (the env is). */
export function scopeOf(prefix: string): Scope {
  return { prefix, tenant: process.env.DE_TENANT || undefined };
}

/** The harness's connection for the engine's streams (DE_ADMIN_URL, DE_ADMIN_CREDS). */
export function connectAdmin(opts: ConnectionOptions = {}): Promise<NatsConnection> {
  const { adminUrl, adminCreds } = adminFromEnv();
  return tcpConnect(
    userInfo({
      servers: adminUrl,
      ...opts,
      ...(adminCreds !== undefined ? { authenticator: credsAuthenticator(readFileSync(adminCreds)) } : {}), // prettier-ignore
    }),
  );
}

/**
 * A server URL's `user:password@`, as the connection's `user` and `pass`: nats.js ignores it in a
 * URL (the engine's Go client and nats-py read it), and a config-file server's users, the
 * engine's tenancy test setup among them, are names and passwords (docs/findings.md F38).
 */
export function userInfo(opts: ConnectionOptions): ConnectionOptions {
  const servers = typeof opts.servers === "string" ? [opts.servers] : (opts.servers ?? []);
  let user: string | undefined;
  let pass: string | undefined;
  const bare = servers.map((u) => {
    const m = /^([a-z]+:\/\/)([^@/]*)@(.*)$/i.exec(u);
    if (m === null) return u;
    const [name, pw] = m[2]!.split(":", 2);
    user = decodeURIComponent(name ?? "");
    pass = pw === undefined ? undefined : decodeURIComponent(pw);
    return m[1]! + m[3]!;
  });
  if (user === undefined || opts.user !== undefined || opts.authenticator !== undefined) return opts; // prettier-ignore
  return { ...opts, servers: bare, user, ...(pass !== undefined ? { pass } : {}) };
}

/** True in the mode (the env is the same in every test worker). */
export const inExistingMode = (): boolean => (process.env.DE_NATS_URL ?? "") !== "";

/** The connection options a test needs for the server: the creds, when the mode names any. */
export function withCreds(opts: ConnectionOptions): ConnectionOptions {
  const creds = process.env.DE_NATS_CREDS;
  if (!inExistingMode()) return opts;
  if (creds === undefined || creds === "") return userInfo(opts);
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

/** The filter of `stream`'s subjects that are the scope's: all of them standalone; as a tenant the
 * tenant's token (the three streams), its keys (the buckets) or, for the object store, all of them
 * (an object's meta subject and its chunks do not carry the tenant: `tenantObjects` sorts them). */
function filterOf(scope: Scope, stream: string): string {
  const t = scope.tenant;
  if (t === undefined) return ">";
  if (stream.startsWith("KV_")) return `$KV.${stream.slice(3)}.${t}.>`;
  if (stream.startsWith("OBJ_")) return ">";
  return `${scope.prefix}.${t}.>`;
}

/** A durable is the scope's: any standalone; as a tenant one named `{prefix}~{tenant}~…`. */
export function durableOf(scope: Scope, name: string): boolean {
  return scope.tenant === undefined || name.startsWith(`${scope.prefix}~${scope.tenant}~`);
}

/** An ephemeral consumer is the scope's: any standalone; as a tenant one filtered on its subjects. */
function ephemeralOf(
  scope: Scope,
  cfg: { filter_subject?: string; filter_subjects?: string[] },
): boolean {
  const t = scope.tenant;
  if (t === undefined) return true;
  const filters = [cfg.filter_subject ?? "", ...(cfg.filter_subjects ?? [])].filter(
    (f) => f !== "",
  );
  return filters.some((f) => f.startsWith(`${scope.prefix}.${t}.`) || new RegExp(`^\\$KV\\.[^.]+\\.${t}\\.`).test(f)); // prettier-ignore
}

interface StoredObject {
  /** The name as the ref says it, `{runId}/sha256:<hex>` (the tenant's token taken off). */
  name: string;
  meta: string;
  chunks: string;
}

/** The scope's objects in the prefix's blob store, from their meta records' last messages: every
 * one standalone, as a tenant those named `{tenant}/…`. */
async function scopeObjects(jsm: JetStreamManager, scope: Scope): Promise<StoredObject[]> {
  const stream = prefixStreams(scope.prefix)[6]!;
  const bucket = stream.slice("OBJ_".length);
  const head = scope.tenant === undefined ? "" : `${scope.tenant}/`;
  const out: StoredObject[] = [];
  try {
    const it = await jsm.direct.getLastMessagesFor(stream, { multi_last: [`$O.${bucket}.M.>`] });
    for await (const m of it) {
      const info = JSON.parse(new TextDecoder().decode(m.data)) as { name?: string; nuid?: string }; // prettier-ignore
      if (typeof info.name !== "string" || !info.name.startsWith(head)) continue;
      const nuid = info.nuid ?? "";
      out.push({ name: info.name.slice(head.length), meta: m.subject, chunks: nuid === "" ? "" : `$O.${bucket}.C.${nuid}` }); // prettier-ignore
    }
  } catch (e) {
    if (jsErrorCode(e) === 10059 || (e instanceof Error && /not found|no results/i.test(e.message))) return []; // prettier-ignore
    throw e;
  }
  return out;
}

/**
 * The consumers without a durable name on the prefix's streams: the leftovers check, scoped. Those
 * in `known` (`stream/name`, there before the test or the run) are not the run's: on a prefix the
 * run shares, another client's watch is not a leak of this one's.
 */
export async function prefixEphemeralConsumers(
  nc: NatsConnection,
  scope: Scope,
  known: Set<string> = new Set(),
): Promise<LeftoverConsumer[]> {
  const jsm = await jetstreamManager(nc);
  const out: LeftoverConsumer[] = [];
  for (const stream of prefixStreams(scope.prefix)) {
    try {
      for await (const c of jsm.consumers.list(stream)) {
        if (
          (c.config.durable_name === undefined || c.config.durable_name === "") &&
          !known.has(`${stream}/${c.name}`) &&
          ephemeralOf(scope, c.config)
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

async function subjectsOf(
  jsm: JetStreamManager,
  stream: string,
  filter = ">",
): Promise<Map<string, number>> {
  try {
    const si = await jsm.streams.info(stream, { subjects_filter: filter });
    return new Map(Object.entries(si.state.subjects ?? {}));
  } catch (e) {
    if (jsErrorCode(e) === 10059) return new Map();
    throw e;
  }
}

/** What the prefix holds before a test: the cleanup purges only what changed after it. */
export async function snapshot(nc: NatsConnection, scope: Scope): Promise<Snapshot> {
  const jsm = await jetstreamManager(nc);
  const streams = prefixStreams(scope.prefix);
  const subjects = await Promise.all(streams.map((s) => subjectsOf(jsm, s, filterOf(scope, s))));
  const consumers = await Promise.all(streams.map((s) => consumersOf(jsm, s)));
  return {
    subjects: new Map(streams.map((s, i) => [s, subjects[i]!])),
    consumers: new Set(consumers.flat()),
  };
}

/** The run ids whose journals a test touched: `{prefix}.journal.<runId>` (as a tenant
 * `{prefix}.{tenant}.journal.<runId>`) new or grown since `before`. */
export async function runIdsSince(
  nc: NatsConnection,
  scope: Scope,
  before: Snapshot,
): Promise<string[]> {
  // prettier-ignore
  const jsm = await jetstreamManager(nc);
  const stream = prefixStreams(scope.prefix)[0]!;
  const was = before.subjects.get(stream) ?? new Map<string, number>();
  const head = scope.tenant === undefined ? `${scope.prefix}.journal.` : `${scope.prefix}.${scope.tenant}.journal.`; // prettier-ignore
  return [...(await subjectsOf(jsm, stream, filterOf(scope, stream)))]
    .filter(([s, n]) => was.get(s) !== n && s.startsWith(head))
    .map(([s]) => s.slice(head.length));
}

/** A blob of one of `runIds`: its name is `{runId}/…` (a staged upload part too), a child's or a
 * generation's run id begins with its parent's and `_`. */
function ofRuns(name: string, runIds: Set<string>): boolean {
  const run = name.slice(0, Math.max(0, name.indexOf("/")));
  if (run === "") return false;
  if (runIds.has(run)) return true;
  for (let i = run.indexOf("_"); i > 0; i = run.indexOf("_", i + 1)) {
    if (runIds.has(run.slice(0, i))) return true;
  }
  return false;
}

/**
 * Delete the blobs of `runIds` from the prefix's blob store by name: each object's meta record and
 * its chunks, purged. By name, not by the snapshot's diff: a test that timed out leaves its worker
 * and the engine writing after its cleanup (the dev NGS run left 17 blobs so), and those land in
 * the next test's snapshot, out of every diff's reach. Returns how many objects were deleted.
 */
export async function deleteRunBlobs(
  nc: NatsConnection,
  scope: Scope,
  runIds: Iterable<string>,
): Promise<number> {
  const ids = new Set(runIds);
  if (ids.size === 0) return 0;
  const jsm = await jetstreamManager(nc);
  const stream = prefixStreams(scope.prefix)[6]!;
  const doomed = (await scopeObjects(jsm, scope)).filter((o) => ofRuns(o.name, ids));
  await Promise.all(
    doomed.flatMap((d) => [
      jsm.streams.purge(stream, { filter: d.meta }),
      ...(d.chunks === "" ? [] : [jsm.streams.purge(stream, { filter: d.chunks })]),
    ]),
  );
  return doomed.length;
}

/**
 * The run's end, after its engine stopped (nothing re-creates a durable or writes a blob then):
 * the blobs of every run the run's tests touched, by name, and every durable on the work and
 * timers streams that was not there when the run began. What a test that failed or timed out left
 * after its own cleanup is gone with them.
 */
export async function sweepRun(
  nc: NatsConnection,
  scope: Scope,
  runIds: Iterable<string>,
  atStart: Snapshot,
): Promise<{ blobs: number; durables: number }> {
  const blobs = await deleteRunBlobs(nc, scope, runIds);
  const jsm = await jetstreamManager(nc);
  let durables = 0;
  for (const stream of prefixStreams(scope.prefix).slice(1, 3)) {
    const fresh = (await consumersOf(jsm, stream)).filter((c) => !atStart.consumers.has(c) && durableOf(scope, c.slice(stream.length + 1))); // prettier-ignore
    for (const c of fresh) {
      try {
        await jsm.consumers.delete(stream, c.slice(stream.length + 1));
        durables += 1;
      } catch (e) {
        if (jsErrorCode(e) !== 10014) throw e;
      }
    }
  }
  return { blobs, durables };
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
  scope: Scope,
  before: Snapshot,
): Promise<number> {
  const jsm = await jetstreamManager(nc);
  let purged = 0;
  for (const stream of prefixStreams(scope.prefix)) {
    const was = before.subjects.get(stream) ?? new Map<string, number>();
    const now = await subjectsOf(jsm, stream, filterOf(scope, stream));
    let touched = [...now].filter(([s, n]) => was.get(s) !== n).map(([s]) => s);
    if (scope.tenant !== undefined && stream.startsWith("OBJ_")) {
      // The object store's subjects carry no tenant: only the metas and chunks of the tenant's
      // objects. A staged upload the tenant left half-way is the engine's to drop (--upload-ttl).
      const mine = new Set((await scopeObjects(jsm, scope)).flatMap((o) => [o.meta, o.chunks]));
      touched = touched.filter((s) => mine.has(s));
    }
    // Pipelined: one round trip for the lot, not one per subject.
    await Promise.all(touched.map((filter) => jsm.streams.purge(stream, { filter })));
    purged += touched.length;
  }
  for (const stream of prefixStreams(scope.prefix).slice(0, 3)) {
    const fresh = (await consumersOf(jsm, stream)).filter((c) => !before.consumers.has(c) && durableOf(scope, c.slice(stream.length + 1))); // prettier-ignore
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

/** `DE_CLEANUP=delete` (the default for a generated prefix): the prefix's seven, at the run's
 * end, and nothing else. */
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

/** The throughput the run's large values meet, measured once at the start (docs/DEVELOPING.md "Against an
 * existing server"): `bytes` of random data up through the engine's `put` and back down through its
 * `get`, on the tests' own link, so the engine's own hops to the store are in it. */
export interface Throughput {
  bytes: number;
  putMs: number;
  getMs: number;
  /** Bytes moved per second, up and down together: 2 × `bytes` over the two. */
  bps: number;
}

export async function measureThroughput(
  nc: NatsConnection,
  prefix: string,
  runId: string,
  bytes = 4 * 1024 * 1024,
): Promise<Throughput> {
  const request = createTransport(nc, prefix);
  const blobs = new EngineBlobs({ request, sizes: sizesOf(request, () => nc.info?.max_payload), rtt: () => nc.rtt() }); // prettier-ignore
  const data = new Uint8Array(bytes);
  for (let at = 0; at < bytes; at += 65_536) globalThis.crypto.getRandomValues(data.subarray(at, at + 65_536)); // prettier-ignore
  const t0 = performance.now();
  const ref = await blobs.upload(runId, data);
  const t1 = performance.now();
  const back = await blobs.get(ref.name);
  const t2 = performance.now();
  if (back?.byteLength !== bytes) throw new Error(`the throughput probe: ${ref.name} came back ${String(back?.byteLength ?? "absent")}, not ${String(bytes)} bytes`); // prettier-ignore
  return { bytes, putMs: t1 - t0, getMs: t2 - t1, bps: (2 * bytes * 1000) / (t2 - t0) };
}
