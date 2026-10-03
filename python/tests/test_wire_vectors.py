"""Cross-SDK conformance: this codec must reproduce the reference implementation's bytes.

``conformance/vectors/journal-entries.json`` is generated from the TypeScript ``jsonCodec`` — those
bytes are the golden by definition. This is the standing alarm for encoder drift between the two
SDKs, and it is deliberately cheap: no server, no workflow engine, just bytes.

If a vector fails here, exactly one of two things is true — this codec changed, or the wire did. The
second is a breaking change for every other implementation and must be a deliberate act (regenerate
the vectors, bump the wire version, say so in PROTOCOL.md).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from synadia_durable.journal.codec import VALID_KINDS, json_codec
from synadia_durable.journal.entry import JournalEntry

VECTORS_PATH = (
    Path(__file__).resolve().parents[2] / "conformance" / "vectors" / "journal-entries.json"
)


def _load() -> list[dict[str, Any]]:
    with VECTORS_PATH.open(encoding="utf-8") as fh:
        doc: dict[str, Any] = json.load(fh)
    vectors: list[dict[str, Any]] = doc["vectors"]
    return vectors


VECTORS = _load()


def test_the_vector_file_is_present_and_populated() -> None:
    # A silently-missing fixture would turn every parametrised test below into zero tests.
    assert VECTORS_PATH.is_file(), f"missing conformance vectors at {VECTORS_PATH}"
    assert len(VECTORS) >= 20


@pytest.mark.parametrize("vector", VECTORS, ids=lambda v: str(v["name"]))
def test_encoding_reproduces_the_reference_bytes(vector: dict[str, Any]) -> None:
    entry: JournalEntry = vector["entry"]
    assert json_codec.encode(entry).decode("utf-8") == vector["encoded"]


@pytest.mark.parametrize("vector", VECTORS, ids=lambda v: str(v["name"]))
def test_decoding_the_reference_bytes_yields_the_entry(vector: dict[str, Any]) -> None:
    decoded = json_codec.decode(vector["encoded"].encode("utf-8"))
    assert decoded == vector["entry"]


@pytest.mark.parametrize("vector", VECTORS, ids=lambda v: str(v["name"]))
def test_every_vector_round_trips_through_this_codec(vector: dict[str, Any]) -> None:
    entry: JournalEntry = vector["entry"]
    assert json_codec.decode(json_codec.encode(entry)) == entry


def test_the_vectors_cover_every_wire_kind() -> None:
    # Coverage is the point: a kind with no vector is a kind whose encoding nothing pins.
    covered = {v["entry"]["kind"] for v in VECTORS}
    assert covered == VALID_KINDS


def test_the_vectors_include_the_offloaded_v2_form() -> None:
    # v:2 is the one shape a decoder can get subtly wrong (a stripped slot must NOT read as absent),
    # so its presence in the corpus is asserted rather than assumed.
    offloaded = [v for v in VECTORS if v["entry"].get("v") == 2]
    assert offloaded, "no v:2 vector — the offloaded form is unpinned"
    for v in offloaded:
        assert any(key.endswith("Ref") for key in v["entry"]), v["name"]


# The stored forms are where a decoder goes subtly wrong, and where these two SDKs drifted for real:
# Python once stamped a work-item blob ref v:1 where TypeScript wrote v:2. Nothing in this corpus
# could have caught it — only ONE stored-form vector existed, and it was hand-written with the ref
# placed before `v`/`ts`, a key order neither engine emits. The vectors are generated from
# `encode_payload` / `offload_entry` now, so these assertions are about coverage of that corpus.

_SLOTS = ("input", "value", "data", "error")


def _fields(vector: dict[str, Any]) -> list[str]:
    return list(vector["entry"].keys())


def test_every_payload_slot_is_pinned_in_its_offloaded_form() -> None:
    refs = {k for v in VECTORS for k in _fields(v) if k.endswith("Ref")}
    assert sorted(refs) == ["dataRef", "errorRef", "inputRef", "valueRef"]


def test_every_payload_slot_is_pinned_in_its_transformed_form() -> None:
    encs = {k for v in VECTORS for k in _fields(v) if k.endswith("Enc")}
    assert sorted(encs) == ["dataEnc", "errorEnc", "inputEnc", "valueEnc"]


def test_the_v3_with_a_ref_combination_is_pinned() -> None:
    """The shape a large ENCRYPTED payload takes: the blob holds codec output, so the reader must
    untransform after fetching. Getting `v` wrong here is the difference between a value and
    ciphertext, and it is what an oversized signal or child outcome produces (PROTOCOL §5.7, §6.1).
    """
    both = [
        v
        for v in VECTORS
        if v["entry"].get("v") == 3 and any(k.endswith("Ref") for k in _fields(v))
    ]
    assert len(both) >= 2, "the transformed-and-offloaded shape is unpinned"
    for v in both:
        assert not any(k.endswith("Enc") for k in _fields(v)), v["name"]


def test_a_stored_form_never_carries_the_plaintext_slot_it_stands_in_for() -> None:
    """The silent-corruption shape: a reader preferring the plaintext would bypass encryption."""
    for vector in VECTORS:
        keys = set(_fields(vector))
        for slot in _SLOTS:
            stand_ins = [k for k in (f"{slot}Ref", f"{slot}Enc") if k in keys]
            assert len(stand_ins) <= 1, vector["name"]  # never both a ref AND an enc
            if stand_ins:
                assert slot not in keys, vector["name"]


def test_v_matches_the_form_the_entry_is_actually_in() -> None:
    for vector in VECTORS:
        keys = _fields(vector)
        version = vector["entry"].get("v")
        if any(k.endswith("Enc") for k in keys):
            assert version == 3, vector["name"]
        elif any(k.endswith("Ref") for k in keys):
            assert version >= 2, vector["name"]  # v:2, or v:3 when the blob holds codec output
        else:
            assert version == 1, vector["name"]
