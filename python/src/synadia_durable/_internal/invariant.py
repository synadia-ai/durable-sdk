"""synadia-durable — internal invariant assertions (fail loud on any violation).

These guard LIBRARY invariants (journal monotonicity, replay determinism, executor bookkeeping). An
:class:`InvariantViolation` means a bug in this library, not user error.

Deliberately **not** Python's ``assert``: the interpreter strips those under ``-O``, and an invariant
that silently disappears in a production run is worse than no invariant at all — several of these
guard whether a side effect is about to fire.

Mirrors ``src/internal/assert.ts``.
"""

from __future__ import annotations

from typing import TypeVar

__all__ = ["InvariantViolation", "invariant", "invariant_not_none"]

T = TypeVar("T")


class InvariantViolation(Exception):
    """A library invariant was violated — a bug here, not in the caller's workflow."""

    def __init__(self, message: str) -> None:
        super().__init__(f"invariant violation: {message}")


def invariant(condition: object, message: str) -> None:
    """Raise :class:`InvariantViolation` unless ``condition`` is truthy."""
    if not condition:
        raise InvariantViolation(message)


def invariant_not_none(value: T | None, message: str) -> T:
    """Return ``value``, raising :class:`InvariantViolation` if it is ``None``.

    The returning form exists so a type checker narrows the result at the call site without a
    separate ``assert``.
    """
    if value is None:
        raise InvariantViolation(message)
    return value
