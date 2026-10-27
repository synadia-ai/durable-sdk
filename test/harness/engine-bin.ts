// The engine binaries and the licence, for global-setup.ts (which may not import vitest),
// engine.ts, engine-process.ts and the `limits` suite. The harness reads four variables and
// nothing else:
//
// - `DE_ENGINE`: an engine build from Synadia (a path). Without it the integration suites skip.
// - `DE_LICENCE`: the licence every standalone engine the run spawns gets (F42).
// - `DE_LICENCE_TOOL`: the engine's licence tool (a path). The `limits` suite makes its low-limit
//   licence with it, and without `DE_LICENCE` the run's licence too.
// - `DE_ENGINE_FABRIC`: the engine's fabric build (a path), the only one with --tenancy, for the
//   `limits` suite's tenant limit.
//
// A variable that is set but names no executable file fails the run, loud: a silent skip would
// hide it. `engine.pin` names the engine version the suites were last run against.

import { execFileSync } from "node:child_process";
import { accessSync, constants, existsSync, mkdtempSync, readFileSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const repo = fileURLToPath(new URL("../..", import.meta.url));

export class EngineNotAvailableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "EngineNotAvailableError";
  }
}

/** The engine version `engine.pin` names. */
export function enginePin(): string {
  return readFileSync(join(repo, "engine.pin"), "utf8").trim();
}

type Executable = "DE_ENGINE" | "DE_ENGINE_FABRIC" | "DE_LICENCE_TOOL";

/** The executable `name` gives, or undefined when it is unset; one that is not an executable file throws. */
function executable(name: Executable): string | undefined {
  const given = process.env[name];
  if (given === undefined || given === "") return undefined;
  let ok = false;
  try {
    accessSync(given, constants.X_OK);
    ok = statSync(given).isFile();
  } catch {
    ok = false;
  }
  if (!ok) throw new Error(`${name}=${given}: not an executable file`);
  return given;
}

/** Which build of the engine: `dev`, the standalone one (`DE_ENGINE`), or `fabric`, the only build with --tenancy (`DE_ENGINE_FABRIC`). */
export type EngineBuild = "dev" | "fabric";

/**
 * The engine binary to run: `DE_ENGINE`, or `DE_ENGINE_FABRIC` for the fabric build. Throws
 * EngineNotAvailableError when the variable is unset (the suites skip), and a plain Error when it
 * names no executable file (the run fails).
 */
export function resolveEngineBinary(build: EngineBuild = "dev"): string {
  const name = build === "fabric" ? "DE_ENGINE_FABRIC" : "DE_ENGINE";
  const bin = executable(name);
  if (bin === undefined) throw new EngineNotAvailableError(`no ${name}`);
  return bin;
}

/** The engine's fabric build (`DE_ENGINE_FABRIC`), or undefined when it is not given. */
export function fabricEngine(): string | undefined {
  return executable("DE_ENGINE_FABRIC");
}

/** The engine's licence tool (`DE_LICENCE_TOOL`), or undefined when it is not given. */
export function licenceTool(): string | undefined {
  return executable("DE_LICENCE_TOOL");
}

/** A licence's limits (`workflows: 1`, …), for a test that meets them. */
export type LicenceLimits = Partial<
  Record<"workflows" | "workers" | "runs-in-flight" | "runs-per-day" | "tenants", number>
>;

/**
 * A licence made by `DE_LICENCE_TOOL`, written to a fresh temporary file: the run's (`name`
 * durable-sdk-tests) or a test's own, with `limits`. The tool signs it with the key its own
 * environment gives it.
 */
export function mintLicence(name = "durable-sdk-tests", limits: LicenceLimits = {}): string {
  const tool = licenceTool();
  if (tool === undefined) throw new EngineNotAvailableError("no DE_LICENCE_TOOL");
  const out = join(mkdtempSync(join(tmpdir(), "de-licence-")), "licence.jwt");
  const flags = Object.entries(limits).flatMap(([k, v]) => [`-${k}`, String(v)]);
  execFileSync(tool, ["mint", "-name", name, ...flags, "-out", out], { stdio: "pipe" });
  return out;
}

/**
 * The run's licence: the caller's `DE_LICENCE`, or, with `DE_LICENCE_TOOL`, one made once for the
 * run with no limit on workflow names. A standalone engine keeps the set of names in `{P}_TENANTS`
 * (key `workflows`, 7 days' retention) and the licence's `maxWorkflows` counts it at every poll;
 * the set persists across tests, and the existing-server run puts about 150 names on one prefix.
 * The engine's own cap of 1000 names stays. Throws EngineNotAvailableError with neither: a
 * standalone engine starts only with a licence.
 */
export function runLicence(): { file: string; minted: boolean } {
  const given = process.env.DE_LICENCE;
  if (given !== undefined && given !== "") {
    if (!existsSync(given)) throw new Error(`DE_LICENCE=${given}: no such file`);
    return { file: given, minted: false };
  }
  if (licenceTool() === undefined) {
    throw new EngineNotAvailableError("no DE_LICENCE: a standalone engine starts only with a licence"); // prettier-ignore
  }
  return { file: mintLicence("durable-sdk-tests", { workflows: 0 }), minted: true };
}
