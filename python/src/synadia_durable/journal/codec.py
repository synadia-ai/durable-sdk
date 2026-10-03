"""synadia-durable — journal codec (pluggable; JSON default). Pure, transport-free.

Mirrors ``src/journal/codec.ts``. The fidelity rules that make a value survive a replay unchanged
live one level down in :mod:`synadia_durable._internal.json`, because work items and status records
need exactly the same guarantees as journal entries.
"""

from __future__ import annotations

from typing import Any, Protocol, cast, get_args, runtime_checkable

from synadia_durable._internal.json import decode_json, encode_json
from synadia_durable.journal.entry import EntryKind, JournalEntry
from synadia_durable.version import (
    WIRE_VERSION,
    WIRE_VERSION_BLOBS,
    WIRE_VERSION_ENCODED,
)

__all__ = ["VALID_KINDS", "Codec", "JsonCodec", "json_codec"]

#: The canonical set of decodable entry kinds, derived from the ``EntryKind`` literal itself so a
#: kind can never be added to the wire type without the decoder accepting it (the TypeScript twin
#: maintains this list by hand and needs a test to catch the drift).
VALID_KINDS: frozenset[str] = frozenset(get_args(EntryKind))


@runtime_checkable
class Codec(Protocol):
    """The journal's encode/decode seam. ``v:1`` denotes JSON on the wire (PROTOCOL §5.1)."""

    def encode(self, entry: JournalEntry) -> bytes: ...

    def decode(self, data: bytes) -> JournalEntry: ...

    def encode_value(self, value: object) -> bytes:
        """Encode a single VALUE (one payload slot) for blob offload.

        MUST apply the same fidelity rules as :meth:`encode`, so an offloaded value round-trips
        exactly as it would have inline.
        """
        ...

    def decode_value(self, data: bytes) -> Any: ...


def _is_int(value: object) -> bool:
    """``bool`` is a subclass of ``int`` in Python, and ``True == 1``.

    Without this, a hand-crafted entry carrying ``"v": true`` or ``"seq": true`` would sail through
    the version and sequence guards. The wire says number; we mean number.
    """
    return isinstance(value, int) and not isinstance(value, bool)


class JsonCodec:
    """The default codec: compact UTF-8 JSON with the wire's value-fidelity rules."""

    def encode(self, entry: JournalEntry) -> bytes:
        return encode_json(entry)

    def decode(self, data: bytes) -> JournalEntry:
        obj: object = decode_json(data)
        if not isinstance(obj, dict):
            raise ValueError("journal decode: not an object")
        fields = cast(dict[str, object], obj)

        # v:1 is the base layout; v:2 is the same layout with a payload slot offloaded to a blob ref
        # (a distinct version so pre-offload readers fail loud, never replay a stripped slot as an
        # absent value).
        version = fields.get("v")
        # v:3 = the payload slot went through a PayloadCodec. Accepted STRUCTURALLY here;
        # whether a codec is actually configured is enforced one layer up, where the slot is
        # restored (blobs.py), so the failure names the real problem rather than "unsupported
        # version".
        if not _is_int(version) or version not in (
            WIRE_VERSION,
            WIRE_VERSION_BLOBS,
            WIRE_VERSION_ENCODED,
        ):
            raise ValueError(
                f"journal decode: unsupported wire version {version!r} "
                f"(expected {WIRE_VERSION}, {WIRE_VERSION_BLOBS} or {WIRE_VERSION_ENCODED})"
            )

        kind = fields.get("kind")
        if not isinstance(kind, str) or kind not in VALID_KINDS:
            raise ValueError(f"journal decode: invalid kind {kind!r}")

        if not _is_int(fields.get("seq")):
            raise ValueError("journal decode: missing seq")

        return cast(JournalEntry, obj)

    def encode_value(self, value: object) -> bytes:
        return encode_json(value)

    def decode_value(self, data: bytes) -> Any:  # `Any`: the wire is untyped by construction
        return decode_json(data)


#: The default codec instance (stateless; safe to share).
json_codec: Codec = JsonCodec()
