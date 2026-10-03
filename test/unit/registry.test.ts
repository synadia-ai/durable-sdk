// Pure tests for the workflow registry — version-pinned dispatch's lookup table. No NATS.

import { describe, expect, it } from "vitest";
import { buildRegistry } from "../../src/core/registry.js";
import { defineWorkflow } from "../../src/core/workflow.js";

describe("workflow registry (version-pinned dispatch)", () => {
  const v1 = defineWorkflow("charge", () => Promise.resolve(1), { version: 1 });
  const v2 = defineWorkflow("charge", () => Promise.resolve(2), { version: 2 });
  const refund = defineWorkflow("refund", () => Promise.resolve(0)); // default version 1

  it("registers multiple versions of one name and resolves the EXACT one", () => {
    const r = buildRegistry([v1, v2, refund]);
    expect(r.resolve("charge", 1)).toBe(v1);
    expect(r.resolve("charge", 2)).toBe(v2);
    expect(r.resolve("refund", 1)).toBe(refund);
    expect(new Set(r.names())).toEqual(new Set(["charge", "refund"])); // one consumer per NAME
  });

  it("returns undefined for an unregistered (name, version) — the park-loud trigger", () => {
    const r = buildRegistry([v1]);
    expect(r.resolve("charge", 2)).toBeUndefined(); // version not deployed here
    expect(r.resolve("ghost", 1)).toBeUndefined(); // name not deployed here
  });

  it("throws loud on a duplicate (name, version)", () => {
    const dup = defineWorkflow("charge", () => Promise.resolve(99), { version: 1 });
    expect(() => buildRegistry([v1, dup])).toThrow(/duplicate workflow "charge" version 1/);
  });

  it("treats the same version under different names as distinct (no collision)", () => {
    const a1 = defineWorkflow("a", () => Promise.resolve(0), { version: 1 });
    const b1 = defineWorkflow("b", () => Promise.resolve(0), { version: 1 });
    expect(() => buildRegistry([a1, b1])).not.toThrow();
  });

  it("an empty worker registers no names", () => {
    expect(buildRegistry([]).names()).toEqual([]);
  });
});
