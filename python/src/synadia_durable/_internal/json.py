"""synadia-durable — JSON byte helpers with the wire's value-fidelity rules enforced.

Everything this library puts on NATS goes through here: journal entries, work items and status
records alike. Three settings and one pre-pass make Python's ``json`` produce what the protocol
expects, and each of them is load-bearing rather than cosmetic:

* ``allow_nan=False`` — ``json.dumps`` otherwise emits bare ``NaN`` / ``Infinity``, which is not
  JSON at all. A TypeScript reader's ``JSON.parse`` rejects it outright, so an unguarded non-finite
  float would write a journal entry no other SDK can read. (The TypeScript twin has the mirror-image
  problem — ``JSON.stringify`` silently coerces them to ``null`` — and fails loud for the same
  reason: the first execution would return ``NaN`` and a replay would read ``null``.)
* ``separators=(",", ":")`` — compact, matching ``JSON.stringify``.
* ``ensure_ascii=False`` — ``JSON.stringify`` emits non-ASCII characters raw; Python escapes them to
  ``\\uXXXX`` by default. Both parse identically, but only the raw form is byte-identical to what the
  reference implementation writes, which is what the cross-SDK golden vectors compare.
* a **key pre-pass** — ``json.dumps`` silently coerces ``int``/``float``/``bool``/``None`` dict keys
  to strings, so ``{1: "a"}`` would journal as ``{"1": "a"}`` and replay as a *different value* than
  the step returned. That is the same divergence class as the non-finite floats, so it fails the same
  way: loudly, at the journal boundary, instead of subtly, later.

**Known coercion, deliberately not rejected:** a ``tuple`` encodes as a JSON array and therefore
replays as a ``list``. Rejecting tuples would be hostile in Python (returning ``(a, b)`` from a step
is ordinary), so this one is documented and pinned by a test rather than guarded. It only bites code
that then requires a tuple specifically — e.g. using the value as a dict key.

Mirrors ``src/internal/json.ts`` plus the fidelity rules of ``src/journal/codec.ts``.
"""

from __future__ import annotations

import json
import math
from typing import Any

__all__ = ["decode_json", "encode_json"]


def _validate(value: object, path: str) -> None:
    """Reject values whose JSON encoding would not round-trip to what was passed in.

    Costs one extra traversal of the value — the same order as the encode itself, and worth it: every
    caller here is about to write a permanent record that a replay will read back and compare.
    """
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"cannot journal a non-finite number ({value}) at {path}")
    elif isinstance(value, dict):
        for key, item in value.items():  # pyright: ignore[reportUnknownVariableType]
            if not isinstance(key, str):
                raise TypeError(
                    f"cannot journal a non-string dict key ({key!r}: {type(key).__name__}) at "
                    f"{path} — JSON would silently coerce it to a string and the replayed value "
                    f"would differ from the one recorded"
                )
            _validate(item, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):  # pyright: ignore[reportUnknownVariableType]
            _validate(item, f"{path}[{index}]")


def encode_json(value: object) -> bytes:
    """Encode to compact UTF-8 JSON, failing loud on anything that would not round-trip.

    Raises ``ValueError`` / ``TypeError``; callers at the journal boundary translate those into
    :class:`~synadia_durable.errors.SerializationError` so the run fails terminally rather than
    retrying bytes that can never encode.
    """
    _validate(value, "$")
    return json.dumps(value, allow_nan=False, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


def decode_json(data: bytes) -> Any:  # `Any`: the wire is untyped by construction
    """Decode UTF-8 JSON bytes. Raises ``ValueError`` on malformed input."""
    return json.loads(data.decode("utf-8"))
