// @synadia-ai/durable — internal invariant assertions (Tiger-Style: fail loud on any violation).
// These guard library invariants (journal monotonicity, lease ownership, replay determinism).
// A thrown InvariantViolation indicates a bug in the library, not user error.

export class InvariantViolation extends Error {
  constructor(message: string) {
    super(`invariant violation: ${message}`);
    this.name = "InvariantViolation";
  }
}

export function invariant(condition: unknown, message: string): asserts condition {
  if (!condition) throw new InvariantViolation(message);
}
