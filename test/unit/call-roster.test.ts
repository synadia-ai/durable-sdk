// The call-level roster, re-expressed for the thin SDK (durable-sdk docs/findings.md F4).
//
// The library's suite of this name (7456c75 test/unit/call-roster.test.ts) pins every method the
// library invokes on the broker's client libraries, by file, under src/nats/. That directory is
// the engine's now and is not in this repo, so the frozen suite cannot run here unchanged: every
// one of its rosters names a file that does not exist. This is the same idea for what replaced it.
// The SDK speaks to the broker in exactly one way, a request to `{prefix}.api.v1.<op>`, from one
// file, through @nats-io/nats-core alone. It creates no consumer, stream, bucket or subscription,
// publishes nothing, and names no JetStream subject. A new call fails here, and whoever added it
// comes and says why — the conversation the library's roster exists to force.
//
// It is also the brief's grep: package.json lists no @nats-io/jetstream, kv or obj among the
// dependencies, and no file under src/ imports them.

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
// The tests' own broker access, minus this file (it quotes every forbidden name) and the
// library's shell copied for staging and reading the streams (test/harness/shell/, the library's
// code, rostered by the library's own suite).
const tests = sources(join(repo, "test")).filter(
  (f) => f.path !== "unit/call-roster.test.ts" && !f.path.startsWith("harness/shell/"),
);

/** Source with comments stripped: a comment MENTIONING `kv.watch()` is not a call. */
function code(text: string): string {
  return text.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:"'`])\/\/[^\n]*/g, "$1");
}

function methodsOn(text: string, receiver: RegExp): string[] {
  const out = new Set<string>();
  for (const m of code(text).matchAll(receiver)) out.add(m[1]!.replace(/\s+/g, ""));
  return [...out].sort();
}

function roster(files: Source[], receiver: RegExp): string[] {
  return files
    .map((f) => ({ path: f.path, methods: methodsOn(f.text, receiver) }))
    .filter((f) => f.methods.length > 0)
    .map((f) => `${f.path}: ${f.methods.join(", ")}`);
}

const NC = /(?<![\w$])(?:opts\.)?nc\s*\.\s*(\w+)\s*\(/g;

describe("where the broker is spoken to at all", () => {
  it("package.json depends on @nats-io/nats-core alone — no jetstream, kv or obj", () => {
    const pkg = JSON.parse(readFileSync(join(repo, "package.json"), "utf8")) as {
      dependencies?: Record<string, string>;
      peerDependencies?: Record<string, string>;
      optionalDependencies?: Record<string, string>;
    };
    expect(Object.keys(pkg.dependencies ?? {})).toEqual(["@nats-io/nats-core"]);
    expect(pkg.peerDependencies ?? {}).toEqual({});
    expect(pkg.optionalDependencies ?? {}).toEqual({});
  });

  it("no file under src/ imports @nats-io/jetstream, kv, obj or a transport", () => {
    const hits = lib
      .filter((f) =>
        /from\s+"@nats-io\/(?!nats-core")[^"]+"|import\(\s*"@nats-io\/(?!nats-core")/.test(f.text),
      ) // prettier-ignore
      .map((f) => f.path);
    expect(hits).toEqual([]);
  });

  it("only the engine client and the connection helper import @nats-io/nats-core", () => {
    const importers = lib.filter((f) => /from "@nats-io\//.test(f.text)).map((f) => f.path);
    expect(importers).toEqual([
      "engine/client.ts",
      "engine/transport.ts",
      "engine/worker.ts",
      "nats/connect.ts",
    ]);
  });

  it("every request goes through the one transport, to {prefix}.api.v1.<op>", () => {
    // durable-sdk: `rtt` is a PING to the server, no subject: the blob store's `put` scales its
    // wait with it (docs/findings.md F33).
    expect(roster(lib, NC)).toEqual([
      "engine/client.ts: isClosed, rtt, status",
      "engine/transport.ts: request",
      "engine/worker.ts: isClosed, rtt",
    ]);
    const names = lib.find((f) => f.path === "engine/names.ts")!.text;
    expect(code(names)).toMatch(/`\$\{prefix\}\.api\.v1\.\$\{op\}`/);
    const transport = lib.find((f) => f.path === "engine/transport.ts")!.text;
    expect(code(transport)).toMatch(/nc\.request\(apiSubject\(prefix, op\)/);
  });

  it("nothing under src/ publishes, subscribes, or names a JetStream, KV or object-store subject", () => {
    const offenders: string[] = [];
    const routes: Array<[string, RegExp]> = [
      ["publish", /\.publish\s*\(/],
      ["subscribe", /\.subscribe\s*\(/],
      ["a JetStream API subject", /\$JS\./],
      ["a KV subject", /\$KV\./],
      ["an object-store subject", /\$O\./],
      [
        "a journal/work/timer subject",
        /["`'](?:[A-Za-z0-9_-]+|\$\{[^}]+\})\.(?:journal|work|timer)\./,
      ],
    ];
    for (const f of lib) {
      for (const [what, re] of routes)
        if (re.test(code(f.text))) offenders.push(`${f.path}: ${what}`);
    }
    expect(offenders).toEqual([]);
  });
});

describe("the consumer roster — the SDK creates no consumer at all", () => {
  it("no route to a consumer exists anywhere under src/", () => {
    const routes: Array<[string, RegExp]> = [
      ["consumers.add/get", /consumers\s*\.\s*(?:add|get)\s*\(/],
      ["ordered/push consumer", /getPushConsumer|getPullConsumer|getBoundPushConsumer/],
      ["consume/fetch/next", /\.(?:consume|fetch|next)\s*\(\s*\{/],
      [
        "KV watch / history / keys",
        /\.(?:watch|watchAll|history)\s*\(|await\s+[^;\n]*?\.keys\s*\(/,
      ],
      ["object-store get/list", /\.getBlob\s*\(|(?<![\w$])os\.(?:get|list)\s*\(/],
    ];
    const offenders: string[] = [];
    for (const f of lib) {
      for (const [what, re] of routes)
        if (re.test(code(f.text))) offenders.push(`${f.path}: ${what}`);
    }
    expect(offenders).toEqual([]);
  });

  it("in the tests, a consumer is created only where a test says why and who deletes it", () => {
    // The library's rule, kept for the suites' own JetStream client (devDependencies only, F13).
    const ALLOWED = new Map<string, { calls: RegExp; owner: RegExp }>([
      [
        "integration/payload-codec.test.ts",
        {
          calls: /consumers\s*\.\s*(?:add|get)\s*\(|\.fetch\s*\(\s*\{/,
          owner: /consumers\s*\.\s*delete\s*\(/,
        },
      ],
    ]);
    const CALLS: Array<[string, RegExp]> = [
      ["consumers.add", /consumers\s*\.\s*add\s*\(/],
      ["consumers.get", /consumers\s*\.\s*get\s*\(/],
      ["ordered/push consumer", /getPushConsumer|getPullConsumer|getBoundPushConsumer/],
      ["legacy pull subscribe", /\.pullSubscribe\s*\(/],
      ["consume/fetch", /\.(?:consume|fetch)\s*\(\s*\{/],
      ["KV watch / history", /\.(?:watch|watchAll|history)\s*\(/],
    ];
    const offenders: string[] = [];
    for (const f of tests) {
      const c = code(f.text);
      const allowed = ALLOWED.get(f.path);
      for (const [what, re] of CALLS) {
        const all = [...c.matchAll(new RegExp(re.source, "g"))];
        if (all.length === 0) continue;
        if (!allowed || !all.every((m) => allowed.calls.test(m[0])))
          offenders.push(`${f.path}: ${what}`);
      }
      if (allowed && !allowed.owner.test(c)) offenders.push(`${f.path}: owner missing`);
    }
    expect(offenders).toEqual([]);
  });
});
