// The exported VERSION must match package.json. It has drifted twice (0.5.1 and 0.6.0 both shipped
// a hand-bumped package.json and a stale src/version.ts), and nothing noticed — the constant is
// exported, so a wrong value is what a user's build reports, and what a bug report would quote.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { VERSION, WIRE_VERSION, WIRE_VERSION_BLOBS } from "../../src/version.js";

describe("version metadata", () => {
  it("VERSION matches package.json", () => {
    const pkgPath = fileURLToPath(new URL("../../package.json", import.meta.url));
    const pkg = JSON.parse(readFileSync(pkgPath, "utf8")) as { version: string };
    expect(VERSION).toBe(pkg.version);
  });

  it("the wire versions are the two the codec accepts, and stay distinct", () => {
    // v:2 exists so a pre-offload reader fails loud on a ref-stripped entry instead of replaying it
    // as undefined; collapsing them would reintroduce exactly that silent corruption.
    expect(WIRE_VERSION).toBe(1);
    expect(WIRE_VERSION_BLOBS).toBe(2);
    expect(WIRE_VERSION_BLOBS).not.toBe(WIRE_VERSION);
  });
});
