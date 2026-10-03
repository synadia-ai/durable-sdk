"""The journal codec — round-trip completeness, the decode guards, and value fidelity.

PROTOCOL §5.1–§5.3. The fidelity cases are the ones worth staring at: each is a value that Python's
``json`` would happily encode into something that decodes back as a *different* value, which on a
replay is indistinguishable from nondeterministic workflow code.
"""

from __future__ import annotations

from typing import Any

import pytest

from synadia_durable._internal.json import encode_json
from synadia_durable.journal.codec import VALID_KINDS, json_codec
from synadia_durable.journal.entry import (
    NON_COMMAND_SEQ,
    BlobRef,
    JournalEntry,
    StepBeginEntry,
    StepEndEntry,
    stamp,
)
from synadia_durable.version import WIRE_VERSION, WIRE_VERSION_BLOBS

TS = 1781667505735

# One specimen per wire kind. The test below asserts this covers VALID_KINDS exactly, so a kind
# cannot be added to the wire without a round-trip proof landing with it.
SPECIMENS: list[JournalEntry] = [
    {"kind": "start", "seq": 0, "wf": "checkout", "wfVersion": 1, "input": {"cartId": "c-100"}},
    {"kind": "step-begin", "seq": 0, "name": "reserve", "key": "checkout-ok:0"},
    {"kind": "step-end", "seq": 0, "name": "reserve", "value": "ord-c-100"},
    {"kind": "now", "seq": 1, "value": TS},
    {"kind": "random", "seq": 2, "value": 0.5},
    {"kind": "uuid", "seq": 3, "value": "0bc7-…"},
    {"kind": "sleep", "seq": 4, "name": "settle", "dueMs": TS + 60_000},
    {"kind": "await", "seq": 5, "name": "payment", "dueMs": TS + 1000},
    {
        "kind": "signal",
        "seq": NON_COMMAND_SEQ,
        "name": "payment",
        "data": {"approved": True},
        "signalId": "sig-1",
    },
    {
        "kind": "call",
        "seq": 6,
        "name": "ship",
        "childRunId": "checkout-ok_6",
        "wf": "ship",
        "input": {"to": "us-west"},
    },
    {"kind": "resolve", "seq": NON_COMMAND_SEQ, "resolvesSeq": 6, "value": {"tracking": "TRK"}},
    {"kind": "result", "seq": NON_COMMAND_SEQ, "value": {"status": "shipped"}},
    {
        "kind": "fail",
        "seq": NON_COMMAND_SEQ,
        "error": {"name": "WorkflowFailure", "message": "nope"},
    },
    {
        "kind": "continued",
        "seq": NON_COMMAND_SEQ,
        "nextRunId": "sub-acme_g1",
        "input": {"cycle": 2},
        "generation": 1,
        "base": "sub-acme",
        "version": 2,
    },
    {"kind": "cancel-requested", "seq": NON_COMMAND_SEQ, "reason": "operator"},
    {"kind": "cancelled", "seq": NON_COMMAND_SEQ, "reason": "operator"},
]


def test_every_wire_kind_has_a_specimen() -> None:
    assert {s["kind"] for s in SPECIMENS} == VALID_KINDS


@pytest.mark.parametrize("entry", SPECIMENS, ids=lambda e: str(e["kind"]))
def test_every_kind_round_trips(entry: JournalEntry) -> None:
    stamped = stamp(entry, TS)
    assert json_codec.decode(json_codec.encode(stamped)) == stamped


def test_stamp_sets_the_wire_version_and_timestamp_last() -> None:
    # Key ORDER is part of byte-compatibility with the reference implementation, whose `stamp`
    # spreads the body and then appends v/ts.
    begin: StepBeginEntry = {"kind": "step-begin", "seq": 0, "name": "n", "key": "r:0"}
    stamped = stamp(begin, TS)
    assert list(stamped) == ["kind", "seq", "name", "key", "v", "ts"]
    assert stamped["v"] == WIRE_VERSION
    assert stamped["ts"] == TS


def test_an_offloaded_entry_is_stamped_v2() -> None:
    ref: BlobRef = {"name": "r/sha256:ab", "digest": "sha256:ab", "size": 3}
    end: StepEndEntry = {"kind": "step-end", "seq": 0, "name": "big", "valueRef": ref}
    stamped = stamp(end, TS, WIRE_VERSION_BLOBS)
    assert stamped["v"] == WIRE_VERSION_BLOBS
    assert json_codec.decode(json_codec.encode(stamped)) == stamped


@pytest.mark.parametrize(
    "payload",
    [
        b'{"kind":"start","seq":0,"v":4}',  # a wire version from the future
        b'{"kind":"start","seq":0}',  # no version at all
        b'{"kind":"start","seq":0,"v":true}',  # bool is not a number, even though True == 1
    ],
)
def test_decode_rejects_an_unsupported_wire_version(payload: bytes) -> None:
    with pytest.raises(ValueError, match="wire version"):
        json_codec.decode(payload)


def test_decode_rejects_an_unknown_kind() -> None:
    with pytest.raises(ValueError, match="invalid kind"):
        json_codec.decode(b'{"kind":"teleport","seq":0,"v":1}')


@pytest.mark.parametrize(
    "payload",
    [
        b'{"kind":"start","v":1}',  # missing
        b'{"kind":"start","seq":"0","v":1}',  # a string that looks like one
        b'{"kind":"start","seq":true,"v":1}',  # bool again
    ],
)
def test_decode_rejects_a_missing_or_non_numeric_seq(payload: bytes) -> None:
    with pytest.raises(ValueError, match="seq"):
        json_codec.decode(payload)


def test_decode_rejects_a_non_object() -> None:
    with pytest.raises(ValueError, match="not an object"):
        json_codec.decode(b"[1,2,3]")


# --- value fidelity: what must fail loud at the journal boundary -------------------------------


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_floats_fail_loud(bad: float) -> None:
    # json.dumps would emit bare NaN / Infinity, which is not JSON and which no other SDK can read.
    with pytest.raises(ValueError, match="non-finite"):
        encode_json({"value": bad})


def test_a_non_finite_float_nested_anywhere_fails_loud() -> None:
    with pytest.raises(ValueError, match=r"non-finite.*\$\.a\[1\]\.b"):
        encode_json({"a": [1, {"b": float("nan")}]})


@pytest.mark.parametrize("key", [1, 2.5, True, None])
def test_non_string_dict_keys_fail_loud(key: object) -> None:
    # json.dumps would silently coerce these to strings, so the replayed value would differ from
    # the one the step returned — the same divergence class as the non-finite floats.
    with pytest.raises(TypeError, match="non-string dict key"):
        encode_json({key: "v"})


def test_unencodable_types_fail_loud() -> None:
    for bad in ({1, 2}, object(), b"bytes"):
        with pytest.raises(TypeError):
            encode_json({"v": bad})


def test_a_tuple_replays_as_a_list_and_this_is_documented_not_guarded() -> None:
    # Deliberately NOT rejected: returning (a, b) from a step is ordinary Python and refusing it
    # would be hostile. Pinned here so the coercion is a decision rather than a surprise.
    encoded = encode_json({"pair": (1, 2)})
    assert encoded == b'{"pair":[1,2]}'
    assert json_codec.decode_value(encoded) == {"pair": [1, 2]}


def test_non_ascii_is_emitted_raw_like_json_stringify() -> None:
    # ensure_ascii=False: Python would otherwise escape to \uXXXX, which parses the same but is not
    # byte-identical to what the reference implementation writes.
    assert encode_json({"s": "héllo → 🎉"}) == '{"s":"héllo → 🎉"}'.encode()


def test_encoding_is_compact() -> None:
    assert encode_json({"a": 1, "b": [1, 2]}) == b'{"a":1,"b":[1,2]}'


def test_int_and_float_are_distinguishable_on_the_way_back() -> None:
    # Python has two numeric types where JSON (and JavaScript) have one. Pinned because it is the
    # one fidelity difference a cross-language OBSERVER can see.
    decoded: Any = json_codec.decode_value(encode_json({"i": 1, "f": 1.0}))
    assert isinstance(decoded["i"], int)
    assert isinstance(decoded["f"], float)
    assert encode_json({"f": 1.0}) == b'{"f":1.0}'


def test_decode_accepts_v3_structurally() -> None:
    """v:3 = a payload slot went through a PayloadCodec. The codec layer decodes it; whether one is
    actually CONFIGURED is enforced where the slot is restored (blobs.py), so the failure there can
    name the real problem instead of "unsupported version"."""
    entry = json_codec.decode(b'{"kind":"start","seq":0,"v":3,"inputEnc":"AAAA"}')
    assert entry["kind"] == "start"
