// The engine binary, for global-setup.ts (which may not import vitest) and engine.ts: `DE_ENGINE`
// (a path), or built with `go build -o` from the engine checkout (`DE_ENGINE_REPO`, default
// ../durable-engine) at the commit `engine.pin` names. A checkout at another commit, or with local
// changes, is refused unless `DE_ENGINE_ANY=1`.
//
// The build is the engine's `make build-dev` (F42): `-tags devlicence`, so it trusts the test
// licence issuer, and `durable-licence` beside it. A standalone `serve` starts only with a licence:
// the run mints one test licence (`durable-licence mint -test`) and every standalone engine it
// spawns gets it as `DE_LICENCE`, unless the caller's `DE_LICENCE` names one.

import { execFileSync } from "node:child_process";
import { accessSync, constants, existsSync, mkdirSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const repo = fileURLToPath(new URL("../..", import.meta.url));

export class EngineNotAvailableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "EngineNotAvailableError";
  }
}

/** The commit `engine.pin` names. */
export function enginePin(): string {
  return readFileSync(join(repo, "engine.pin"), "utf8").trim();
}

function onPath(name: string): string | null {
  for (const dir of (process.env.PATH ?? "").split(":")) {
    if (!dir) continue;
    try {
      accessSync(join(dir, name), constants.X_OK);
      return join(dir, name);
    } catch {
      /* keep looking */
    }
  }
  return null;
}

/**
 * The engine binary to run: `DE_ENGINE`, or a build of the pinned checkout. Throws
 * EngineNotAvailableError when neither can be had (the suites skip), and a plain Error when the
 * checkout is at the wrong commit (the run fails: a silent skip would hide it).
 */
/** Which build of the engine: `dev` (-tags devlicence, standalone with a test licence) or `fabric` (-tags fabric, the only build with --tenancy; no licence). */
export type EngineBuild = "dev" | "fabric";

export function resolveEngineBinary(build: EngineBuild = "dev"): string {
  const given = build === "fabric" ? process.env.DE_ENGINE_FABRIC : process.env.DE_ENGINE;
  if (given !== undefined && given !== "") {
    try {
      accessSync(given, constants.X_OK);
    } catch {
      throw new Error(`DE_ENGINE=${given}: not an executable file`);
    }
    return given;
  }
  const pin = enginePin();
  const src = process.env.DE_ENGINE_REPO ?? join(repo, "..", "durable-engine");
  if (!existsSync(join(src, "go.mod"))) {
    throw new EngineNotAvailableError(
      `no ${build === "fabric" ? "DE_ENGINE_FABRIC" : "DE_ENGINE"} and no engine checkout at ${src} (DE_ENGINE_REPO)`,
    );
  }
  if (onPath("go") === null) {
    throw new EngineNotAvailableError(`no DE_ENGINE, and Go is not on PATH to build ${src}`);
  }
  const git = (...args: string[]): string =>
    execFileSync("git", ["-C", src, ...args], { encoding: "utf8" }).trim();
  const head = git("rev-parse", "HEAD");
  const any = process.env.DE_ENGINE_ANY === "1";
  if (!head.startsWith(pin) && !any) {
    throw new Error(
      `the engine checkout ${src} is at ${head.slice(0, 7)}, engine.pin names ${pin}: ` +
        `check out ${pin} there, point DE_ENGINE at a binary, or set DE_ENGINE_ANY=1`,
    );
  }
  const dirty = git("status", "--porcelain", "--untracked-files=no") !== "";
  if (dirty && !any) {
    throw new Error(`the engine checkout ${src} has local changes; it is not ${pin} (DE_ENGINE_ANY=1 to run it anyway)`); // prettier-ignore
  }
  const outDir = join(repo, ".engine-bin");
  mkdirSync(outDir, { recursive: true });
  const tag = `${head.slice(0, 12)}${dirty ? "-dirty" : ""}`;
  const out = join(outDir, `durable-engine-${build}-${tag}`);
  const tool = join(outDir, `durable-licence-${tag}`);
  if (!existsSync(out) || !existsSync(tool) || dirty) {
    // `make build-dev`'s two builds, into this repo's .engine-bin: nothing is written in the checkout.
    execFileSync("go", ["build", "-tags", build === "fabric" ? "fabric" : "devlicence", "-o", out, "./cmd/durable-engine"], { cwd: src, stdio: "inherit" }); // prettier-ignore
    execFileSync("go", ["build", "-o", tool, "./cmd/durable-licence"], { cwd: src, stdio: "inherit" }); // prettier-ignore
  }
  return out;
}

/**
 * `durable-licence`, for the engine binary `bin`: beside a build of ours (`durable-licence-<commit>`)
 * or beside a `DE_ENGINE` (`make build-dev` puts both in `bin/`), or `DE_LICENCE_TOOL`.
 */
export function licenceTool(bin: string): string {
  const given = process.env.DE_LICENCE_TOOL;
  if (given !== undefined && given !== "") return given;
  const ours = bin.replace(/durable-engine-dev-([^/]+)$/, "durable-licence-$1");
  for (const p of [ours, join(dirname(bin), "durable-licence")]) {
    if (p !== bin && existsSync(p)) return p;
  }
  throw new Error(
    `no durable-licence beside ${bin} to mint the run's test licence: set DE_LICENCE to a licence ` +
      `file, DE_LICENCE_TOOL to the tool, or build both with \`make build-dev\` in the engine`,
  );
}

/** Flags of `durable-licence mint` (`-workflows 1`, …): a licence whose limits a test meets. */
export type LicenceLimits = Partial<
  Record<"workflows" | "workers" | "runs-in-flight" | "runs-per-day" | "tenants", number>
>;

/**
 * A test licence (`durable-licence mint -test`), written to a fresh temporary file: the run's
 * (`name` durable-sdk-tests, the tool's generous defaults) or a test's own, with `limits`.
 */
export function mintTestLicence(
  bin: string,
  name = "durable-sdk-tests",
  limits: LicenceLimits = {},
): string {
  const out = join(mkdtempSync(join(tmpdir(), "de-licence-")), "licence.jwt");
  const flags = Object.entries(limits).flatMap(([k, v]) => [`-${k}`, String(v)]);
  execFileSync(licenceTool(bin), ["mint", "-test", "-name", name, ...flags, "-out", out], { stdio: "pipe" }); // prettier-ignore
  return out;
}

/**
 * The run's licence: the caller's `DE_LICENCE`, or a test licence minted once for the run, with no
 * limit on workflow names (`-workflows 0`). Since the engine's F77 standalone keeps the set of
 * names in `{P}_TENANTS` (key `workflows`, 7 days' retention) and the licence's `maxWorkflows`
 * counts it at every poll; the set persists across tests, and the existing-server run puts about
 * 150 names on one prefix, so the tool's default of 100 would refuse polls mid-run. The engine's
 * own cap of 1000 names stays. A test's own mint (`limits.test.ts`) keeps its limits.
 */
export function runLicence(bin: string): { file: string; minted: boolean } {
  const given = process.env.DE_LICENCE;
  if (given !== undefined && given !== "") return { file: given, minted: false };
  return { file: mintTestLicence(bin, "durable-sdk-tests", { workflows: 0 }), minted: true };
}
