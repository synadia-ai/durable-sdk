"""The payload-codec seam (encryption at rest), pure: no server.

The bar is the one the offload tranche set — prove the mechanism, then prove each way it could be
SILENTLY wrong is loud instead. The dangerous failure is not "encryption breaks"; it is "a reader
without the codec decodes a transformed payload as if it were the value".
"""

from __future__ import annotations

import base64
import json
from typing import Any, cast

import pytest

from synadia_durable._nats.protocol import WorkItem, shrink_work_item
from synadia_durable.errors import PayloadCodecError, SerializationError
from synadia_durable.journal.blobs import (
    BlobStore,
    blob_name,
    digest_of,
    encode_payload,
    offload_entry,
    resolve_entry,
    with_blob_ref,
)
from synadia_durable.journal.codec import json_codec
from synadia_durable.journal.entry import JournalEntry, stamp
from synadia_durable.journal.payload_codec import PayloadCodec, aes_gcm_payload_codec
from synadia_durable.journal.store import InMemoryJournal, JournalAppend
from synadia_durable.version import WIRE_VERSION, WIRE_VERSION_BLOBS, WIRE_VERSION_ENCODED

pytestmark = pytest.mark.asyncio

KEY = bytes(32)
OTHER_KEY = bytes([9]) * 32
TS = 1_700_000_000_000


class _Rot:
    """A visible, reversible stand-in for when the assertion is about plumbing, not crypto."""

    async def encode(self, plain: bytes) -> bytes:
        return bytes((b + 1) & 0xFF for b in plain)

    async def decode(self, encoded: bytes) -> bytes:
        return bytes((b - 1) & 0xFF for b in encoded)


class _Blobs(BlobStore):
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def put(self, name: str, data: bytes) -> None:
        self.objects[name] = data

    async def get(self, name: str) -> bytes:
        return self.objects[name]


def _codec() -> PayloadCodec:
    return aes_gcm_payload_codec(KEY)


def _start(value: object, seq: int = 0) -> JournalEntry:
    return stamp({"seq": seq, "kind": "start", "wf": "w", "wfVersion": 1, "input": value}, TS)


async def _append(j: InMemoryJournal, run: str, e: JournalEntry, last: int) -> int:
    r = await j.append(
        run, JournalAppend(entry=e, msg_id=f"{run}:{e['kind']}:{e['seq']}", expect_last_seq=last)
    )
    return r.stream_seq


def _raw(j: InMemoryJournal, run: str, i: int = 0) -> dict[str, Any]:
    return cast("dict[str, Any]", json.loads(j._logs[run][i][0].decode()))


# --------------------------------------------------------------------------- round trip


async def test_transformed_payload_round_trips() -> None:
    j = InMemoryJournal(payload_codec=_codec())
    secret = {"card": "4242424242424242", "cvc": "123", "nested": [1, {"deep": True}]}
    await _append(j, "r1", _start(secret), 0)
    loaded = await j.load("r1")
    assert loaded[0].entry["input"] == secret  # type: ignore[typeddict-item]


async def test_every_payload_carrying_kind_round_trips() -> None:
    """The slot map is covered, not just ``input``."""
    j = InMemoryJournal(payload_codec=_codec())
    kinds: list[JournalEntry] = [
        stamp({"seq": 0, "kind": "start", "wf": "w", "wfVersion": 1, "input": {"a": 1}}, TS),
        stamp({"seq": 1, "kind": "step-end", "name": "s", "value": {"b": 2}}, TS),
        stamp({"seq": 2, "kind": "signal", "name": "sig", "data": {"c": 3}, "signalId": "i"}, TS),
        stamp(
            {
                "seq": 3,
                "kind": "call",
                "name": "c",
                "childRunId": "k",
                "wf": "w2",
                "input": {"d": 4},
            },
            TS,
        ),
        stamp({"seq": 4, "kind": "resolve", "resolvesSeq": 3, "value": {"e": 5}}, TS),
        stamp({"seq": 6, "kind": "result", "value": {"f": 6}}, TS),
    ]
    last = 0
    for e in kinds:
        last = await _append(j, "r2", e, last)
    loaded = await j.load("r2")
    # `v` legitimately moves 1 -> 3 on the way through; everything else survives untouched.
    assert [{**cast("dict[str, Any]", le.entry), "v": WIRE_VERSION} for le in loaded] == [
        cast("dict[str, Any]", e) for e in kinds
    ]
    assert all(le.entry["v"] == WIRE_VERSION_ENCODED for le in loaded)


async def test_envelope_stays_readable() -> None:
    j = InMemoryJournal(payload_codec=_Rot())
    await _append(
        j, "r3", stamp({"seq": 1, "kind": "step-end", "name": "s", "value": "s3cr3t"}, TS), 0
    )
    raw = _raw(j, "r3")
    assert raw["kind"] == "step-end"
    assert raw["name"] == "s"  # step names stay legible: the dashboard still works
    assert raw["v"] == WIRE_VERSION_ENCODED
    assert "value" not in raw  # the plaintext slot is GONE
    assert isinstance(raw["valueEnc"], str)
    assert "s3cr3t" not in json.dumps(raw)


async def test_kinds_without_a_payload_slot_stay_v1() -> None:
    j = InMemoryJournal(payload_codec=_codec())
    await _append(j, "r4", stamp({"seq": 0, "kind": "now", "value": TS}, TS), 0)
    loaded = await j.load("r4")
    assert loaded[0].entry["v"] == WIRE_VERSION
    assert loaded[0].entry["value"] == TS  # type: ignore[typeddict-item]


# ------------------------------------------------------------- the silent-corruption guards


async def test_a_reader_without_the_codec_parks() -> None:
    """It must never hand the workflow base64 where its input should be."""
    store = InMemoryJournal(payload_codec=_codec())
    await _append(store, "r5", _start({"secret": 1}), 0)
    blind = InMemoryJournal()
    blind._logs = store._logs
    with pytest.raises(PayloadCodecError, match="no payload_codec configured"):
        await blind.load("r5")


async def test_control_without_the_transform_the_same_blind_reader_succeeds() -> None:
    """Proves the guard above does the work — not that load() simply always raises."""
    store = InMemoryJournal()
    await _append(store, "r6", _start({"secret": 1}), 0)
    blind = InMemoryJournal()
    blind._logs = store._logs
    loaded = await blind.load("r6")
    assert loaded[0].entry["input"] == {"secret": 1}  # type: ignore[typeddict-item]


async def test_the_wrong_key_parks() -> None:
    store = InMemoryJournal(payload_codec=aes_gcm_payload_codec(KEY))
    await _append(store, "r7", _start({"amount": 100}), 0)
    wrong = InMemoryJournal(payload_codec=aes_gcm_payload_codec(OTHER_KEY))
    wrong._logs = store._logs
    with pytest.raises(PayloadCodecError, match="wrong key, or the bytes were altered"):
        await wrong.load("r7")


async def test_tampered_ciphertext_parks() -> None:
    """AES-GCM is authenticated, so a flipped bit is detected rather than yielding garbage."""
    j = InMemoryJournal(payload_codec=_codec())
    await _append(j, "r8", _start({"amount": 100}), 0)
    raw = _raw(j, "r8")
    b = bytearray(base64.b64decode(raw["inputEnc"]))
    b[-1] ^= 0xFF
    raw["inputEnc"] = base64.b64encode(bytes(b)).decode()
    j._logs["r8"][0] = (json.dumps(raw).encode(), j._logs["r8"][0][1])
    with pytest.raises(PayloadCodecError):
        await j.load("r8")


async def test_malformed_enc_parks() -> None:
    j = InMemoryJournal(payload_codec=_codec())
    await _append(j, "r9", _start({"a": 1}), 0)
    raw = _raw(j, "r9")
    raw["inputEnc"] = "!!!not base64!!!"
    j._logs["r9"][0] = (json.dumps(raw).encode(), j._logs["r9"][0][1])
    with pytest.raises(PayloadCodecError):
        await j.load("r9")


async def test_a_codec_that_raises_on_encode_writes_nothing() -> None:
    class _Boom:
        async def encode(self, plain: bytes) -> bytes:  # noqa: ARG002
            raise RuntimeError("kms unavailable")

        async def decode(self, encoded: bytes) -> bytes:
            return encoded

    j = InMemoryJournal(payload_codec=_Boom())
    with pytest.raises(PayloadCodecError):
        await _append(j, "r10", _start({"a": 1}), 0)
    assert await j.load("r10") == []


async def test_an_unencodable_value_still_fails_as_serialization_error() -> None:
    """The codec must not swallow the class that distinguishes 'fail the run' from 'park it'."""
    j = InMemoryJournal(payload_codec=_codec())
    with pytest.raises(SerializationError):
        await _append(j, "r11", _start({"bad": float("nan")}), 0)


# --------------------------------------------------------------------------- × offload
#
# The Python in-memory Journal double does not mirror offload (the TypeScript one does), so these
# drive the real functions directly — which is exactly the sequence both stores execute:
# encode_payload -> offload_entry -> ... -> resolve_entry.


async def _store(entry: JournalEntry, blobs: _Blobs, pc: PayloadCodec | None) -> JournalEntry:
    e = await encode_payload("r", entry, json_codec, pc)
    stored, _ = await offload_entry("r", e, json_codec.encode(e), json_codec, blobs, 1024)
    return stored


async def test_oversized_transformed_payload_offloads_ciphertext() -> None:
    blobs = _Blobs()
    big = {"blob": "x" * 5000}
    stored = await _store(_start(big), blobs, _codec())

    assert len(blobs.objects) == 1
    assert b"xxxxx" not in next(iter(blobs.objects.values()))  # the BLOB holds ciphertext
    assert stored["v"] == WIRE_VERSION_ENCODED  # v:3 survives offload
    assert "inputRef" in cast("dict[str, Any]", stored)

    back = await resolve_entry("r", stored, json_codec, blobs, _codec())
    assert back["input"] == big  # type: ignore[typeddict-item]


async def test_control_without_a_codec_the_blob_holds_plaintext() -> None:
    blobs = _Blobs()
    big = {"blob": "x" * 5000}
    stored = await _store(_start(big), blobs, None)
    assert b"xxxxx" in next(iter(blobs.objects.values()))
    assert stored["v"] == WIRE_VERSION_BLOBS


async def test_offloaded_v3_parks_for_a_reader_with_blobs_but_no_codec() -> None:
    blobs = _Blobs()
    stored = await _store(_start({"blob": "x" * 5000}), blobs, _codec())
    with pytest.raises(PayloadCodecError):
        await resolve_entry("r", stored, json_codec, blobs, None)


# --------------------------------------------------------------------------- the AES helper


async def test_aes_rejects_a_short_key() -> None:
    with pytest.raises(PayloadCodecError, match="32 bytes"):
        aes_gcm_payload_codec(bytes(16))


async def test_aes_is_non_deterministic() -> None:
    c = _codec()
    a, b = await c.encode(b"same"), await c.encode(b"same")
    assert a != b  # a fresh IV per call
    assert await c.decode(a) == b"same"
    assert await c.decode(b) == b"same"


async def test_aes_rejects_ciphertext_too_short_for_an_iv() -> None:
    with pytest.raises(PayloadCodecError):
        await _codec().decode(b"\x00\x01")


async def test_aes_format_matches_the_typescript_twin() -> None:
    """iv(12) || ciphertext||tag — a mixed fleet sharing a key must read each other's payloads.

    Pinned structurally here; ``conformance/interop`` proves it against the real TypeScript codec.
    """
    c = _codec()
    out = await c.encode(b"hello")
    assert len(out) == 12 + len(b"hello") + 16  # iv + plaintext + GCM tag
    assert await c.decode(out) == b"hello"


# ------------------------------------------------------------------- work items and their blobs
#
# `shrink_work_item` is the one payload path that does not run through a journal append: an
# oversized signal / child-done is offloaded BEFORE any entry exists to transform. A blob is at
# rest, so it has to be transformed here or encryption would silently depend on payload size.

_BIG_SECRET = "pan-4242424242424242" + "x" * 4000


def _offload_args(blobs: _Blobs) -> tuple[Any, _Blobs, int]:
    return (json_codec, blobs, 1024)


def _signal_item() -> WorkItem:
    return {
        "runId": "r",
        "kind": "signal",
        "signal": {"name": "card", "data": _BIG_SECRET, "id": "s1"},
    }


async def test_an_oversized_signal_offloads_ciphertext_and_marks_the_item() -> None:
    blobs = _Blobs()
    out = await shrink_work_item(_signal_item(), *_offload_args(blobs), _codec())
    signal = out["signal"]
    assert "data" not in signal
    assert signal["dataRef"]["name"].startswith("r/sha256:")
    assert out.get("payloadEnc") is True
    assert len(blobs.objects) == 1
    assert b"4242424242424242" not in next(iter(blobs.objects.values()))


async def test_control_without_a_codec_the_same_item_stores_plaintext() -> None:
    blobs = _Blobs()
    out = await shrink_work_item(_signal_item(), *_offload_args(blobs))
    assert "dataRef" in out["signal"]
    assert "payloadEnc" not in out
    assert b"4242424242424242" in next(iter(blobs.objects.values()))


async def test_an_oversized_child_value_offloads_ciphertext() -> None:
    blobs = _Blobs()
    item: WorkItem = {
        "runId": "r",
        "kind": "child-done",
        "resolvesSeq": 3,
        "child": {"id": "r_0", "ok": True, "value": _BIG_SECRET},
    }
    out = await shrink_work_item(item, *_offload_args(blobs), _codec())
    assert out.get("payloadEnc") is True
    assert b"4242424242424242" not in next(iter(blobs.objects.values()))


async def test_an_oversized_child_error_offloads_ciphertext() -> None:
    """The failure path carries a payload too — a big error message leaks just as well."""
    blobs = _Blobs()
    item: WorkItem = {
        "runId": "r",
        "kind": "child-done",
        "resolvesSeq": 3,
        "child": {"id": "r_0", "ok": False, "error": {"name": "Error", "message": _BIG_SECRET}},
    }
    out = await shrink_work_item(item, *_offload_args(blobs), _codec())
    assert out.get("payloadEnc") is True
    assert b"4242424242424242" not in next(iter(blobs.objects.values()))


async def test_an_item_under_the_threshold_is_untouched() -> None:
    blobs = _Blobs()
    item: WorkItem = {
        "runId": "r",
        "kind": "signal",
        "signal": {"name": "card", "data": "small", "id": "s1"},
    }
    out = await shrink_work_item(item, *_offload_args(blobs), _codec())
    assert out == item  # the journal append encrypts this one instead
    assert blobs.objects == {}


async def test_a_throwing_codec_writes_nothing_to_the_store() -> None:
    """Blob-before-pointer makes a written blob a commitment; a failed transform must abort first."""

    class _Boom:
        async def encode(self, _plain: bytes) -> bytes:
            raise RuntimeError("kms unavailable")

        async def decode(self, encoded: bytes) -> bytes:
            return encoded

    blobs = _Blobs()
    with pytest.raises(PayloadCodecError):
        await shrink_work_item(_signal_item(), *_offload_args(blobs), _Boom())
    assert blobs.objects == {}


# ------------------------------------------------------- the v:2 / v:3 flag on a ref is load-bearing
#
# The work item tells the worker whether its blob is transformed; the worker stamps the entry
# accordingly. Both ways of getting that wrong must be LOUD — a reader that guessed would either
# hand a workflow raw ciphertext or decrypt plaintext into nonsense.


async def _ref_to(blobs: _Blobs, data: bytes) -> Any:
    digest = digest_of(data)
    name = blob_name("r", digest)
    await blobs.put(name, data)
    return {"name": name, "digest": digest, "size": len(data)}


def _step_end(value: object) -> JournalEntry:
    return stamp({"seq": 1, "kind": "step-end", "name": "s", "value": value}, TS)


async def test_a_v2_ref_pointing_at_ciphertext_parks() -> None:
    blobs = _Blobs()
    ref = await _ref_to(blobs, await _codec().encode(json_codec.encode_value({"card": "4242"})))
    entry = with_blob_ref(_step_end({"card": "4242"}), "value", ref, transformed=False)
    assert entry["v"] == WIRE_VERSION_BLOBS
    with pytest.raises(PayloadCodecError):
        await resolve_entry("r", entry, json_codec, blobs, _codec())


async def test_a_v3_ref_pointing_at_plaintext_parks() -> None:
    blobs = _Blobs()
    ref = await _ref_to(blobs, json_codec.encode_value({"card": "4242"}))
    entry = with_blob_ref(_step_end({"card": "4242"}), "value", ref, transformed=True)
    assert entry["v"] == WIRE_VERSION_ENCODED
    with pytest.raises(PayloadCodecError):
        await resolve_entry("r", entry, json_codec, blobs, _codec())


async def test_control_each_flag_paired_with_its_bytes_resolves() -> None:
    blobs = _Blobs()
    plain = await _ref_to(blobs, json_codec.encode_value({"card": "4242"}))
    cipher = await _ref_to(blobs, await _codec().encode(json_codec.encode_value({"card": "4242"})))
    for ref, flag in ((plain, False), (cipher, True)):
        entry = with_blob_ref(_step_end({"card": "4242"}), "value", ref, transformed=flag)
        resolved = await resolve_entry("r", entry, json_codec, blobs, _codec())
        assert cast("dict[str, Any]", resolved)["value"] == {"card": "4242"}
