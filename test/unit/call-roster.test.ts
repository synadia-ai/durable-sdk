// The call-level roster: every method the library invokes on the broker's client libraries, by
// file, and the rules those calls obey. Increment 2 of docs/plans/test-axes.md; the same list in
// prose, with what each call creates on the server and who deletes it, is
// docs/IMPLEMENTATION-GUIDE.md §9.
//
// The five consumer leaks of LEARNINGS #56–#59 had one shape: a client-library convenience — a KV
// watch, a key walk, an object-store read — that creates a consumer on the library's behalf and
// stops it without deleting it. The runtime guard (test/harness/leftovers.ts) catches such a call
// the moment a test exercises its path; this catches the CALL before it is on any path, by reading
// the source. Like payload-paths.test.ts it is a roster, not a restriction: a new call fails here
// and whoever added it comes and says what it creates and who deletes it — the conversation that
// did not happen five times.
//
// Two mechanisms, deliberately redundant. The RECEIVER rosters pin the exact set of methods called
// on every `jsm`, `js`, `kv`, `kvm`, `os` and `nc` handle per file, so a new method on a known
// handle fails whatever it is called. The NAME blacklist catches the known consumer-creating
// conveniences on a handle named anything else.

import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const repo = fileURLToPath(new URL("../..", import.meta.url));

interface Source {
  path: string;
  text: string;
}

function sources(root: string, dir = root, out: Source[] = []): Source[] {
  for (const e of readdirSync(dir, { withFileTypes: true })) {
    const full = join(dir, e.name);
    if (e.isDirectory()) sources(root, full, out);
    else if (e.name.endsWith(".ts") && !e.name.endsWith(".d.ts")) {
      out.push({ path: full.slice(root.length + 1), text: readFileSync(full, "utf8") });
    }
  }
  return out.sort((a, b) => a.path.localeCompare(b.path));
}

const lib = sources(join(repo, "src"));
// The roster is not a subject of itself: this file quotes every forbidden name.
const tests = sources(join(repo, "test")).filter((f) => f.path !== "unit/call-roster.test.ts");

/** Source with comments stripped: a comment MENTIONING `kv.watch()` is not a call. */
function code(text: string): string {
  return text.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:"'`])\/\/[^\n]*/g, "$1");
}

/** The distinct method paths called on a receiver, e.g. `streams.info` for `jsm.streams.info(`. */
function methodsOn(text: string, receiver: RegExp): string[] {
  const out = new Set<string>();
  for (const m of code(text).matchAll(receiver)) out.add(m[1]!.replace(/\s+/g, ""));
  return [...out].sort();
}

/** `file: a.b, c.d` lines for every file that calls anything on the receiver. */
function roster(files: Source[], receiver: RegExp): string[] {
  return files
    .map((f) => ({ path: f.path, methods: methodsOn(f.text, receiver) }))
    .filter((f) => f.methods.length > 0)
    .map((f) => `${f.path}: ${f.methods.join(", ")}`);
}

function count(text: string, pattern: RegExp): number {
  return [...code(text).matchAll(new RegExp(pattern.source, "g"))].length;
}

// One receiver each. `(?<![\w$])` keeps `jsm.` from matching inside `xjsm.`, and `js\s*\.` keeps
// `js` from matching `jsm`/`json`. A path may span a line break (`this.jsm.streams\n.info(`).
const JSM = /(?<![\w$])(?:this\.)?jsm\s*\.\s*(\w+(?:\s*\.\s*\w+)?)\s*\(/g;
const JS = /(?<![\w$])(?:this\.)?js\s*\.\s*(\w+(?:\s*\.\s*\w+)?)\s*\(/g;
const KV = /(?<![\w$])(?:this\.)?kv\s*\.\s*(\w+)\s*\(/g;
const KVM = /(?:(?<![\w$])kvm|new\s+Kvm\([^)]*\))\s*\.\s*(\w+)\s*\(/g;
const OS = /(?:(?<![\w$])os|new\s+Objm\([^)]*\))\s*\.\s*(\w+)\s*\(/g;
const NC = /(?<![\w$])(?:opts\.)?nc\s*\.\s*(\w+)\s*\(/g;

describe("where the broker is spoken to at all", () => {
  it("only the NATS shell imports @nats-io/* — the core and the journal layer are transport-free", () => {
    const importers = lib.filter((f) => /from "@nats-io\//.test(f.text)).map((f) => f.path);
    expect(importers).toEqual([
      "nats/blob-store.ts",
      "nats/children.ts",
      "nats/client.ts",
      "nats/connect.ts",
      "nats/direct.ts",
      "nats/journal-store.ts",
      "nats/lease.ts",
      "nats/parked.ts",
      "nats/resources.ts",
      "nats/runs.ts",
      "nats/scheduler.ts",
      "nats/worker.ts",
    ]);
  });

  it("every read of a stream's messages goes through the one consumer-free reader (direct.ts)", () => {
    // journal loads, blob chunks, the parked-index sweep — and nothing reads a stream any other way.
    const callers = lib
      .filter((f) => f.path !== "nats/direct.ts" && /\breadSubject\s*\(/.test(code(f.text)))
      .map((f) => f.path);
    expect(callers).toEqual(["nats/blob-store.ts", "nats/journal-store.ts", "nats/parked.ts"]);
  });
});

describe("the consumer roster", () => {
  it("the library creates exactly ONE kind of consumer: a named durable, from one call site", () => {
    // `consumers.add` is the only consumer-creating call. It is a NAMED durable — never an
    // ephemeral — and lives in ensureConsumer, which binds two per workflow name (PROTOCOL §6.3).
    const adders = lib.filter((f) => /consumers\s*\.\s*add\s*\(/.test(code(f.text)));
    expect(adders.map((f) => f.path)).toEqual(["nats/worker.ts"]);
    const worker = adders[0]!.text;
    expect(count(worker, /consumers\s*\.\s*add\s*\(/)).toBe(1);
    expect(worker).toMatch(/consumers\s*\.\s*add\s*\(\s*stream,\s*\{\s*durable_name:/);
  });

  it("the pump BINDS to that durable by name — consumers.get(stream, durable), never a config object", () => {
    // `consumers.get(stream, { filter_subjects })` creates an ORDERED consumer; the string form
    // binds to an existing one. The shape is pinned so the difference cannot be introduced by a
    // refactor that looks like a no-op.
    const getters = lib.filter((f) => /consumers\s*\.\s*get\s*\(/.test(code(f.text)));
    expect(getters.map((f) => f.path)).toEqual(["nats/worker.ts"]);
    const worker = getters[0]!.text;
    expect(count(worker, /consumers\s*\.\s*get\s*\(/)).toBe(1);
    expect(worker).toMatch(/consumers\s*\.\s*get\s*\(\s*stream,\s*durable\s*\)/);
    // …and consume() is the only delivery loop; fetch()/next() would be a second consumer protocol.
    expect(lib.filter((f) => /\.consume\s*\(/.test(code(f.text))).map((f) => f.path)).toEqual(["nats/worker.ts"]); // prettier-ignore
    expect(count(worker, /\.consume\s*\(/)).toBe(1);
  });

  it("no other route to a consumer exists anywhere in the library", () => {
    const routes: Array<[string, RegExp]> = [
      ["ordered/push consumer", /getPushConsumer|getPullConsumer|getBoundPushConsumer/],
      ["legacy pull/push subscribe", /\.(?:pullSubscribe|subscribe)\s*\(/],
      ["consumer fetch/next", /\.(?:fetch|next)\s*\(\s*\{/],
    ];
    const offenders: string[] = [];
    for (const f of lib) {
      // The one core subscription (client.ts, the result() hint) is not a consumer — pinned below.
      if (f.path === "nats/client.ts") continue;
      for (const [what, re] of routes)
        if (re.test(code(f.text))) offenders.push(`${f.path}: ${what}`);
    }
    expect(offenders).toEqual([]);
  });
});

describe("the receiver rosters — every method the library calls on each handle, by file", () => {
  it("JetStreamManager", () => {
    expect(roster(lib, JSM)).toEqual([
      "nats/direct.ts: direct.getBatch",
      "nats/journal-store.ts: streams.info",
      "nats/resources.ts: direct.getBatch, streams.add, streams.delete, streams.info, streams.update",
      "nats/worker.ts: consumers.add, consumers.info, consumers.update, streams.info, streams.purge",
    ]);
  });

  it("JetStreamClient", () => {
    expect(roster(lib, JS)).toEqual([
      "nats/blob-store.ts: jetstreamManager",
      "nats/children.ts: publish",
      "nats/client.ts: publish",
      "nats/journal-store.ts: publish",
      "nats/scheduler.ts: publish",
      "nats/worker.ts: consumers.get, publish",
    ]);
  });

  it("KV buckets: get/put/create/update/delete/purge — and never watch, keys or history", () => {
    expect(roster(lib, KV)).toEqual([
      "nats/lease.ts: create, delete, get, update",
      "nats/parked.ts: get, purge, put",
      "nats/runs.ts: get, put",
    ]);
    // Kvm: `create` opens-or-creates, and provisioning is the one place that calls it — the one
    // place that may CREATE a bucket. The worker and the client BIND (`open`: no request at all)
    // the buckets provision() has just verified: an open through `create` is an API check plus a
    // STREAM.INFO each — six of a worker's start round trips (F25), two of a client's first call.
    expect(roster(lib, KVM)).toEqual([
      "nats/client.ts: open",
      "nats/resources.ts: create",
      "nats/worker.ts: open",
    ]);
  });

  it("the object store: info + putBlob to write, and the chunk subject read directly — never get/getBlob/list", () => {
    expect(lib.filter((f) => /from "@nats-io\/obj"/.test(f.text)).map((f) => f.path)).toEqual([
      "nats/blob-store.ts",
      "nats/resources.ts",
    ]);
    expect(roster(lib, OS)).toEqual([
      "nats/blob-store.ts: info, open, putBlob",
      "nats/resources.ts: create",
    ]);
  });

  it("the connection: one core subscription, for the result() hint, and it is unsubscribed", () => {
    // `isClosed` is a local read, not a request: result() and the worker's pumps ask it so that a
    // connection closed FOR GOOD is reported by name instead of waited on (LEARNINGS #63).
    expect(roster(lib, NC)).toEqual([
      "nats/client.ts: isClosed, subscribe",
      "nats/worker.ts: isClosed",
    ]);
    const client = lib.find((f) => f.path === "nats/client.ts")!.text;
    expect(count(client, /\.subscribe\s*\(/)).toBe(1);
    expect(client).toMatch(/sub\.unsubscribe\(\)/);
  });
});

describe("the forbidden conveniences — each one is a consumer the client library never deletes", () => {
  const FORBIDDEN: Array<[string, RegExp]> = [
    ["KV watch (LEARNINGS #56)", /\.watch\s*\(/],
    ["KV watchAll", /\.watchAll\s*\(/],
    ["KV history (LEARNINGS #59)", /\.history\s*\(/],
    // KV keys() is a watch underneath. Map/Set/Object keys() are synchronous and never awaited;
    // the KV one returns a promise, so an AWAITED `.keys(` is the tell.
    ["KV keys (a watch underneath)", /await\s+[^;\n]*?\.keys\s*\(/],
    ["object-store get/getBlob (LEARNINGS #59)", /\.getBlob\s*\(|(?<![\w$])os\.get\s*\(/],
    // streams.list / consumers.list are paged API requests, not consumers; anything else .list() is
    // the object store's, which is a watch.
    ["object-store list (a watch underneath)", /(?<!streams|consumers)\.list\s*\(/],
  ];

  it("none of them appears in the library", () => {
    const offenders: string[] = [];
    for (const f of lib) {
      for (const [what, re] of FORBIDDEN)
        if (re.test(code(f.text))) offenders.push(`${f.path}: ${what}`);
    }
    expect(offenders).toEqual([]);
  });

  it("none of them appears in the tests either, except where a test says why and who deletes it", () => {
    // Increment 1 found three test-side sweeps leaking consumers the end-of-run guard never saw
    // (LEARNINGS #61). The harness reads through the library's direct-get reader (harness/blobs.ts)
    // and multi-subject last-message gets, so a test needs none of these. A test that creates a
    // consumer on purpose lists itself here with its owner, and the owner must be in the file.
    const ALLOWED = new Map<string, { calls: RegExp; owner: RegExp; why: string }>([
      [
        "integration/payload-codec.test.ts",
        {
          calls: /consumers\s*\.\s*(?:add|get)\s*\(|\.fetch\s*\(\s*\{/,
          owner: /consumers\s*\.\s*delete\s*\(/,
          why: "an ephemeral pull consumer sweeps DE_WORK/DE_TIMERS (no allow_direct there), deleted in finally",
        },
      ],
    ]);
    const CONSUMER_CALLS: Array<[string, RegExp]> = [
      ["consumers.add", /consumers\s*\.\s*add\s*\(/],
      ["consumers.get", /consumers\s*\.\s*get\s*\(/],
      ["ordered/push consumer", /getPushConsumer|getPullConsumer|getBoundPushConsumer/],
      ["legacy pull subscribe", /\.pullSubscribe\s*\(/],
      ["consume/fetch", /\.(?:consume|fetch)\s*\(\s*\{/],
    ];
    const offenders: string[] = [];
    for (const f of tests) {
      const c = code(f.text);
      const hits = [...FORBIDDEN, ...CONSUMER_CALLS].filter(([, re]) => re.test(c));
      if (hits.length === 0) continue;
      const allowed = ALLOWED.get(f.path);
      for (const [what, re] of hits) {
        // Permitted only if EVERY occurrence is one the entry names.
        const permitted =
          allowed !== undefined &&
          [...c.matchAll(new RegExp(re.source, "g"))].every((m) => allowed.calls.test(m[0]));
        if (!permitted) offenders.push(`${f.path}: ${what}`);
      }
      if (allowed && !allowed.owner.test(c)) offenders.push(`${f.path}: owner missing (${allowed.why})`); // prettier-ignore
    }
    expect(offenders).toEqual([]);
  });

  it("object-store handles in tests are only ever used to delete/inspect/write, never to read or list", () => {
    // `store.get(...)` in a test is the LIBRARY's reader (NatsBlobStore). The client's own handle
    // comes from `new Objm(...).open(...)`; every method called on one is pinned here.
    const offenders: string[] = [];
    for (const f of tests) {
      const c = code(f.text);
      // `new Objm(jetstream(nc))` — one level of nested parentheses is enough for a test.
      const objm = String.raw`new\s+Objm\((?:[^()]|\([^()]*\))*\)\s*\.\s*(?:open|create)\s*\(`;
      const handles = new Set<string>();
      for (const m of c.matchAll(new RegExp(String.raw`(\w+)\s*=\s*await\s+${objm}`, "g"))) handles.add(m[1]!); // prettier-ignore
      const methods = new Set<string>();
      for (const h of handles) for (const m of c.matchAll(new RegExp(String.raw`(?<![\w$])${h}\s*\.\s*(\w+)\s*\(`, "g"))) methods.add(m[1]!); // prettier-ignore
      for (const m of c.matchAll(new RegExp(String.raw`${objm}(?:[^()]|\([^()]*\))*\)\s*\)\s*\.\s*(\w+)\s*\(`, "g"))) methods.add(m[1]!); // prettier-ignore
      for (const m of methods)
        if (["get", "getBlob", "list", "watch"].includes(m)) offenders.push(`${f.path}: ${m}`);
    }
    expect(offenders).toEqual([]);
  });
});

describe("every object the library creates has an owner in the same file — or is documented as GC'd", () => {
  const OWNERS: Array<{ file: string; creates: RegExp; owner: RegExp | string }> = [
    { file: "nats/client.ts", creates: /nc\.subscribe\s*\(/, owner: /sub\.unsubscribe\(\)/ },
    { file: "nats/worker.ts", creates: /\.consume\s*\(/, owner: /ms\.stop\(\)/ },
    { file: "nats/lease.ts", creates: /kv\.create\s*\(/, owner: /kv\.delete\s*\(/ },
    { file: "nats/parked.ts", creates: /kv\.put\s*\(/, owner: /kv\.purge\s*\(/ },
    {
      file: "nats/resources.ts",
      creates: /streams\.add\s*\(/,
      owner: /export async function deprovision/,
    },
    // No in-library owner, by design — the runbook's GC procedure (OPERATIONS.md) is the owner:
    {
      file: "nats/runs.ts",
      creates: /kv\.put\s*\(/,
      owner: "operator GC: DE_RUNS keys live with the run's journal",
    },
    {
      file: "nats/blob-store.ts",
      creates: /putBlob\s*\(/,
      owner: "operator GC: blobs live with the run's journal",
    },
    {
      file: "nats/journal-store.ts",
      creates: /js\.publish\s*\(/,
      owner: "operator GC: the journal is the truth, never deleted by the library",
    },
    // Durables are fleet-shared and outlive every process; deprovision deletes them with the stream.
    {
      file: "nats/worker.ts",
      creates: /consumers\.add\s*\(/,
      owner:
        "fleet-shared durables: deleted with the stream by deprovision, or by hand when a name is retired",
    },
  ];

  it.each(OWNERS)("$file: $creates", ({ file, creates, owner }) => {
    const text = code(lib.find((f) => f.path === file)!.text);
    expect(text).toMatch(creates);
    if (owner instanceof RegExp) expect(text).toMatch(owner);
    else expect(typeof owner).toBe("string"); // documented, not deleted — see IMPLEMENTATION-GUIDE §9
  });
});
