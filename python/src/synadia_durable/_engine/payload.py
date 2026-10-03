"""synadia-durable — the two payload forms of ``durable-engine`` ``docs/API.md`` "Payloads".
Mirrors ``src/engine/payload.ts``.

Without a payload codec every value travels inline (``input``, ``value``, ``data``, ``error``).
With one, the engine never sees a plaintext payload:

* an appended entry carries its slot exactly as the library's journal writes it: ``encode_payload``
  of ``journal/blobs.py`` applied after ``stamp`` (slot removed, ``<slot>Enc`` appended after ``v``
  and ``ts``, ``v:3``);
* a start's or a child's input and a signal's data travel as ``inputEnc`` / ``dataEnc``, the
  base64 of the codec's output over the value's JSON bytes, the same bytes ``encode_payload`` would
  put in the entry the engine builds from them;
* every journal the engine hands out is reversed slot by slot (``resolve_entry``) before replay.

Offload is the engine's: no blob store is reachable from here, and a ref would fail loud.
"""

from __future__ import annotations

import base64
import binascii
from typing import TYPE_CHECKING, Any, Literal

from synadia_durable.errors import PayloadCodecError, SerializationError
from synadia_durable.journal.blobs import encode_payload, resolve_entry
from synadia_durable.journal.codec import json_codec
from synadia_durable.journal.entry import LoadedEntry

if TYPE_CHECKING:
    from synadia_durable.journal.entry import JournalEntry
    from synadia_durable.journal.payload_codec import PayloadCodec

__all__ = ["WireJournal", "reverse_journal", "slot", "unslot", "wire_entry"]

#: A journal as the engine answers it: ``[{"streamSeq": n, "entry": {...}}]``.
WireJournal = list[dict[str, Any]]


async def wire_entry(
    run_id: str, entry: JournalEntry, payload_codec: PayloadCodec | None
) -> JournalEntry:
    """The entry an ``append`` carries: the stamped entry, its slot transformed under a codec."""
    out = await encode_payload(run_id, entry, json_codec, payload_codec)
    try:
        # The library's codec: rejects a non-finite number anywhere, as the library's journal does,
        # so the first execution and a replay can never read different values.
        json_codec.encode(out)
    except (TypeError, ValueError) as e:
        raise SerializationError(
            run_id,
            f"run {run_id}: cannot encode {entry['kind']} entry (seq {entry.get('seq')}): {e}",
        ) from e
    return out


async def reverse_journal(
    run_id: str, journal: WireJournal, payload_codec: PayloadCodec | None
) -> list[LoadedEntry]:
    """Reverse every ``<slot>Enc`` of a journal the engine handed out, in order."""
    out: list[LoadedEntry] = []
    for le in journal:
        # No blob store, by construction: the engine inflates every ref before it answers.
        entry = await resolve_entry(run_id, le["entry"], json_codec, None, payload_codec)
        out.append(LoadedEntry(entry=entry, stream_seq=int(le["streamSeq"])))
    return out


async def slot(
    name: Literal["input", "data"],
    value: Any,
    payload_codec: PayloadCodec | None,
    run_id: str = "",
) -> dict[str, Any]:
    """A value for a request slot: ``{name: value}`` plain, ``{name + "Enc": base64}`` under a
    codec. ``None`` is sent as JSON ``null``, which is what the library's journal holds for it."""
    try:
        plain = json_codec.encode_value(value)
    except (TypeError, ValueError) as e:
        raise SerializationError(run_id, f"run {run_id}: cannot encode the {name}: {e}") from e
    if payload_codec is None:
        return {name: value}
    try:
        out = await payload_codec.encode(plain)
    except PayloadCodecError:
        raise
    except Exception as e:
        raise PayloadCodecError(
            "the configured payload_codec failed to encode this payload", run_id or None
        ) from e
    return {f"{name}Enc": base64.b64encode(out).decode("ascii")}


async def unslot(run_id: str, encoded: str, payload_codec: PayloadCodec | None) -> Any:
    """Decode a ``valueEnc`` / ``errorEnc`` of a ``result`` reply."""
    if payload_codec is None:
        raise PayloadCodecError(
            "the result is encoded (v:3) and this client has no payload_codec — configure the "
            "deployment's codec",
            run_id,
        )
    try:
        data = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as e:
        raise PayloadCodecError("malformed encoded result (expected base64)", run_id) from e
    try:
        plain = await payload_codec.decode(data)
    except PayloadCodecError:
        raise
    except Exception as e:
        raise PayloadCodecError(
            "the payload_codec could not decode the result (wrong key, or altered bytes)", run_id
        ) from e
    return json_codec.decode_value(plain)
