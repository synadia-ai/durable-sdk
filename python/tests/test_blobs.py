"""Blob-ref resolution (PROTOCOL §5.6) — and above all, what happens when it CANNOT resolve.

The failure modes matter more than the happy path here. A reader that meets an offloaded entry it
cannot resolve must fail LOUD; the one thing it must never do is treat the stripped payload slot as
an absent value, because that replays as ``None`` and corrupts the run silently.
"""

from __future__ import annotations

import json
from typing import Any, cast

import pytest

from synadia_durable.errors import BlobIntegrityError, BlobMissingError, FencedError
from synadia_durable.journal.blobs import (
    BlobStore,
    blob_name,
    digest_of,
    entry_ref,
    resolve_entry,
)
from synadia_durable.journal.codec import json_codec
from synadia_durable.journal.entry import BlobRef, JournalEntry, stamp
from synadia_durable.journal.store import InMemoryJournal, JournalAppend
from synadia_durable.version import WIRE_VERSION, WIRE_VERSION_BLOBS

TS = 1781667505735
VALUE = {"frames": ["a", "b"], "cost": 12}
BYTES = json_codec.encode_value(VALUE)
DIGEST = digest_of(BYTES)


class FakeBlobs:
    """A blob store that holds whatever it is given, and can be told to lose or corrupt it."""

    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = objects if objects is not None else {}

    async def get(self, name: str) -> bytes:
        if name not in self.objects:
            raise BlobMissingError("?", -1, name, "not in the fake store", "absent")
        return self.objects[name]

    async def put(self, name: str, data: bytes) -> None:
        self.objects[name] = data


def _offloaded(run: str = "r1") -> JournalEntry:
    ref: BlobRef = {"name": blob_name(run, DIGEST), "digest": DIGEST, "size": len(BYTES)}
    return stamp(
        {"kind": "step-end", "seq": 3, "name": "render", "valueRef": ref}, TS, WIRE_VERSION_BLOBS
    )


def test_digests_are_lowercase_hex_with_the_wire_prefix() -> None:
    assert DIGEST.startswith("sha256:")
    assert len(DIGEST) == len("sha256:") + 64
    assert DIGEST[7:] == DIGEST[7:].lower()


def test_the_object_name_is_run_scoped_because_the_run_is_the_gc_unit() -> None:
    assert blob_name("checkout-ok", DIGEST) == f"checkout-ok/{DIGEST}"


def test_an_inline_entry_has_no_ref_and_resolves_to_itself() -> None:
    entry: JournalEntry = stamp({"kind": "step-end", "seq": 0, "name": "s", "value": 1}, TS)
    assert entry_ref(entry) is None


async def test_an_inline_entry_passes_through_untouched_without_a_store() -> None:
    entry: JournalEntry = stamp({"kind": "step-end", "seq": 0, "name": "s", "value": 1}, TS)
    assert await resolve_entry("r1", entry, json_codec) == entry


async def test_a_ref_is_inlined_and_the_ref_field_removed() -> None:
    blobs = FakeBlobs({blob_name("r1", DIGEST): BYTES})
    resolved = await resolve_entry("r1", _offloaded(), json_codec, blobs)
    assert resolved["value"] == VALUE  # type: ignore[typeddict-item]
    assert "valueRef" not in resolved


async def test_no_blob_store_fails_loud_rather_than_replaying_an_absent_value() -> None:
    with pytest.raises(BlobMissingError) as caught:
        await resolve_entry("r1", _offloaded(), json_codec, None)
    # `no-store` is the configuration error that waiting never heals — distinct from an object that
    # an operator could still restore.
    assert caught.value.reason == "no-store"


async def test_a_missing_object_is_reported_against_the_run_and_entry_that_wanted_it() -> None:
    with pytest.raises(BlobMissingError) as caught:
        await resolve_entry("r1", _offloaded(), json_codec, FakeBlobs())
    assert caught.value.reason == "absent"
    # The store knows only a name; the run and seq are what make the runbook line actionable.
    assert caught.value.run_id == "r1"
    assert caught.value.seq == 3


async def test_corrupted_bytes_fail_verification_and_are_never_used() -> None:
    blobs = FakeBlobs({blob_name("r1", DIGEST): b'{"frames":["tampered"]}'})
    with pytest.raises(BlobIntegrityError, match="digest mismatch"):
        await resolve_entry("r1", _offloaded(), json_codec, blobs)


async def test_a_malformed_ref_is_an_integrity_error_not_a_crash() -> None:
    entry = stamp(
        {"kind": "step-end", "seq": 3, "name": "r", "valueRef": {"size": 3}},  # type: ignore[typeddict-item]
        TS,
        WIRE_VERSION_BLOBS,
    )
    with pytest.raises(BlobIntegrityError, match="malformed"):
        await resolve_entry("r1", entry, json_codec, FakeBlobs())


async def test_resolution_survives_a_round_trip_through_the_codec() -> None:
    # The blob holds the slot VALUE alone, encoded by the same value encoding as inline entries, so
    # an offloaded value must replay exactly as it would have inline.
    blobs = FakeBlobs({blob_name("r1", DIGEST): BYTES})
    decoded = json_codec.decode(json_codec.encode(_offloaded()))
    resolved = await resolve_entry("r1", decoded, json_codec, blobs)
    assert resolved["value"] == VALUE  # type: ignore[typeddict-item]


# ------------------------------------------------- offload THROUGH the in-memory journal double
#
# Until the double mirrored the offload WRITE path, these could only be expressed by calling
# `offload_entry` directly — which tests the function but not the seam the engine actually uses, and
# left the two SDKs' doubles behaving differently for no stated reason. The TypeScript double has
# always offloaded on append; this is the twin.


class _CountingBlobs(BlobStore):
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.puts = 0

    async def put(self, name: str, data: bytes) -> None:
        self.puts += 1
        self.objects[name] = data

    async def get(self, name: str) -> bytes:
        # The seam's contract: definitively absent is a BlobMissingError, never a None or a KeyError.
        if name not in self.objects:
            raise BlobMissingError("?", -1, name, "not in the fake store", "absent")
        return self.objects[name]


def _entry(value: object, seq: int = 1) -> JournalEntry:
    return stamp({"seq": seq, "kind": "step-end", "name": "s", "value": value}, TS)


async def _append(journal: InMemoryJournal, run: str, entry: JournalEntry, last: int) -> int:
    result = await journal.append(
        run, JournalAppend(entry=entry, msg_id=f"{run}:{entry['seq']}", expect_last_seq=last)
    )
    return result.stream_seq


async def test_an_over_threshold_append_offloads_and_load_resolves_it_back() -> None:
    blobs = _CountingBlobs()
    journal = InMemoryJournal(blobs=blobs, offload_threshold_bytes=256)
    big = {"frames": ["x" * 200] * 8}
    await _append(journal, "off-1", _entry(big), 0)

    assert blobs.puts == 1
    stored = json.loads(next(iter(blobs.objects.values())))
    assert stored == big  # the blob holds the VALUE, not the entry

    (loaded,) = await journal.load("off-1")
    assert cast("dict[str, Any]", loaded.entry)["value"] == big
    assert loaded.entry["v"] == WIRE_VERSION_BLOBS


async def test_control_under_the_threshold_nothing_is_offloaded() -> None:
    blobs = _CountingBlobs()
    journal = InMemoryJournal(blobs=blobs, offload_threshold_bytes=4096)
    await _append(journal, "off-2", _entry({"small": True}), 0)
    assert blobs.puts == 0
    (loaded,) = await journal.load("off-2")
    assert loaded.entry["v"] == WIRE_VERSION
    assert cast("dict[str, Any]", loaded.entry)["value"] == {"small": True}


async def test_control_without_a_threshold_the_double_never_writes_a_blob() -> None:
    """The pre-parity behaviour, kept explicit: `blobs` alone is a READ capability."""
    blobs = _CountingBlobs()
    journal = InMemoryJournal(blobs=blobs)
    await _append(journal, "off-3", _entry({"frames": ["x" * 200] * 8}), 0)
    assert blobs.puts == 0


async def test_blob_before_pointer_the_blob_survives_a_fenced_append() -> None:
    """The ordering the real flow depends on, now observable in the pure suite.

    The blob is written BEFORE the CAS, so a racer that loses still leaves its object behind —
    content-addressed, so it is either identical to the winner's or garbage nobody references. A
    double that offloaded only on a successful append would hide any bug that depends on this.
    """
    blobs = _CountingBlobs()
    journal = InMemoryJournal(blobs=blobs, offload_threshold_bytes=256)
    await _append(journal, "off-4", _entry({"frames": ["a" * 200] * 8}), 0)
    assert blobs.puts == 1

    # A second writer with a STALE expect_last_seq: same slot, different value.
    with pytest.raises(FencedError):
        await journal.append(
            "off-4",
            JournalAppend(
                entry=_entry({"frames": ["b" * 200] * 8}, seq=2),
                msg_id="off-4:2",
                expect_last_seq=0,  # stale — the log already advanced
            ),
        )
    assert blobs.puts == 2, "the loser's blob was written before its CAS was evaluated"
    assert len(blobs.objects) == 2  # different content ⇒ different content-addressed name


async def test_an_identical_value_offloaded_twice_converges_on_one_object() -> None:
    """Content addressing, through the seam: same value + same run ⇒ same name ⇒ one object."""
    blobs = _CountingBlobs()
    journal = InMemoryJournal(blobs=blobs, offload_threshold_bytes=256)
    same = {"frames": ["z" * 200] * 8}
    last = await _append(journal, "off-5", _entry(same, seq=1), 0)
    await _append(journal, "off-5", _entry(same, seq=2), last)
    assert blobs.puts == 2  # written twice…
    assert len(blobs.objects) == 1  # …under one name


async def test_a_missing_blob_is_loud_at_load_never_a_silent_none() -> None:
    blobs = _CountingBlobs()
    journal = InMemoryJournal(blobs=blobs, offload_threshold_bytes=256)
    await _append(journal, "off-6", _entry({"frames": ["q" * 200] * 8}), 0)
    blobs.objects.clear()  # an early GC, or a lost bucket
    with pytest.raises(BlobMissingError):
        await journal.load("off-6")
