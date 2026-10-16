"""synadia-durable — the two payload forms of the engine's ``API.md`` "Payloads".
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

Offload is the engine's for every value that fits a request. One that does not goes up in ``put``
parts (``blobs.py``) and its request carries the ref: :func:`fit_entry` and :func:`fit_slot`. A ref
the engine hands back, for a value too large for a reply, is fetched with ``get`` by
``resolve_entry`` with the engine as its blob store.
"""

from __future__ import annotations

import base64
import binascii
from typing import TYPE_CHECKING, Any, Literal, cast

from synadia_durable._engine.blobs import body_bytes
from synadia_durable.errors import PayloadCodecError, SerializationError
from synadia_durable.journal.blobs import (
    encode_payload,
    offload_entry,
    resolve_entry,
    store_payload_blob,
)
from synadia_durable.journal.codec import json_codec
from synadia_durable.journal.entry import LoadedEntry

if TYPE_CHECKING:
    from synadia_durable._engine.blobs import Sizes
    from synadia_durable.journal.blobs import BlobStore
    from synadia_durable.journal.entry import BlobRef, JournalEntry
    from synadia_durable.journal.payload_codec import PayloadCodec

__all__ = [
    "WireJournal",
    "fit_entry",
    "fit_slot",
    "reverse_journal",
    "slot",
    "unref",
    "unslot",
    "wire_entry",
]

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


async def fit_entry(
    run_id: str,
    entry: JournalEntry,
    envelope: dict[str, Any],
    sizes: Sizes | None,
    blobs: BlobStore,
) -> JournalEntry:
    """The entry an ``append`` carries, fitted to ``request_max``: when the request (``envelope``
    with the entry) would pass it, the entry's slot goes up in parts first and the entry carries its
    ref in the library's stored form (``offload_entry``: slot removed, ``<slot>Ref``, ``v:2``, or
    ``v:3`` over codec output). ``sizes`` ``None`` is ``offload=False``: nothing goes up, and an
    entry too large is left for the transport or the engine to refuse."""
    if sizes is None:
        return entry
    request_max = (await sizes()).request_max
    if body_bytes({**envelope, "entry": entry}) <= request_max:
        return entry
    out, _ = await offload_entry(run_id, entry, json_codec.encode(entry), json_codec, blobs, 0)
    return out


async def fit_slot(
    name: Literal["input", "data"],
    value: Any,
    payload_codec: PayloadCodec | None,
    run_id: str,
    rest: dict[str, Any],
    sizes: Sizes | None,
    blobs: BlobStore,
) -> dict[str, Any]:
    """A value for a request slot, fitted to ``request_max``: :func:`slot`'s form when the request
    (``rest`` with it) fits; else the value is ``put`` under ``run_id`` and the slot is
    ``{name + "Ref": ref}``, with ``payloadEnc: true`` when the blob holds codec output."""
    inline = await slot(name, value, payload_codec, run_id)
    if sizes is None:
        return inline
    if body_bytes({**rest, **inline}) <= (await sizes()).request_max:
        return inline
    ref, transformed = await store_payload_blob(
        run_id, f"the {name}", value, json_codec, blobs, payload_codec
    )
    out: dict[str, Any] = {f"{name}Ref": ref}
    if transformed:
        out["payloadEnc"] = True
    return out


async def reverse_journal(
    run_id: str,
    journal: WireJournal,
    payload_codec: PayloadCodec | None,
    blobs: BlobStore | None = None,
) -> list[LoadedEntry]:
    """Reverse every ``<slot>Enc`` of a journal the engine handed out, in order, and fetch every ref
    it handed in its stored form (a value too large for a reply) from ``blobs``."""
    out: list[LoadedEntry] = []
    for le in journal:
        entry = await resolve_entry(run_id, le["entry"], json_codec, blobs, payload_codec)
        out.append(LoadedEntry(entry=entry, stream_seq=int(le["streamSeq"])))
    return out


async def unref(
    run_id: str,
    slot_name: Literal["value", "error"],
    ref: BlobRef,
    payload_enc: bool,
    payload_codec: PayloadCodec | None,
    blobs: BlobStore,
) -> Any:
    """A ``result`` reply's ``valueRef`` / ``errorRef``: fetched, its digest checked, reversed
    through the codec when ``payloadEnc``, as ``resolve_entry`` resolves it on a terminal entry."""
    entry: dict[str, Any] = {
        "kind": "result" if slot_name == "value" else "fail",
        "seq": -1,
        "v": 3 if payload_enc else 2,
        f"{slot_name}Ref": ref,
    }
    r = await resolve_entry(run_id, cast("JournalEntry", entry), json_codec, blobs, payload_codec)
    return cast("dict[str, Any]", r).get(slot_name)


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
