// The engine binary, for global-setup.ts (which may not import vitest) and engine.ts: `DE_ENGINE`
// (a path), or built with `go build -o` from the engine checkout (`DE_ENGINE_REPO`, default
// ../durable-engine) at the commit `engine.pin` names. A checkout at another commit, or with local
// changes, is refused unless `DE_ENGINE_ANY=1`.

import { execFileSync } from "node:child_process";
import { accessSync, constants, existsSync, mkdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
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
export function resolveEngineBinary(): string {
  const given = process.env.DE_ENGINE;
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
      `no DE_ENGINE and no engine checkout at ${src} (DE_ENGINE_REPO)`,
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
  const out = join(outDir, `durable-engine-${head.slice(0, 12)}${dirty ? "-dirty" : ""}`);
  if (!existsSync(out) || dirty) {
    execFileSync("go", ["build", "-o", out, "./cmd/durable-engine"], {
      cwd: src,
      stdio: "inherit",
    });
  }
  return out;
}
