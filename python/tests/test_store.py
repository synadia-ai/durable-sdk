"""The Journal seam's contract, exercised against the in-memory double.

Every assertion here is about the ONE primitive (PROTOCOL §2): optimistic append. The double exists
so the replay engine's crash and concurrency stories can be tested without a server, which only works
if it is faithful — so the sharp cases (CAS dominating dedup, encode-before-CAS) are pinned here and
re-proved against a real server in ``tests/integration/test_journal.py``.
"""

from __future__ import annotations

import pytest

from synadia_durable.errors import FencedError, PayloadTooLargeError, SerializationError
from synadia_durable.journal.entry import JournalEntry, MsgId, stamp
from synadia_durable.journal.store import InMemoryJournal, JournalAppend

TS = 1781667505735


def _start() -> JournalEntry:
    return stamp({"kind": "start", "seq": 0, "wf": "wf", "wfVersion": 1, "input": {"a": 1}}, TS)


def _begin(seq: int, run: str = "r1") -> JournalEntry:
    return stamp({"kind": "step-begin", "seq": seq, "name": "s", "key": f"{run}:{seq}"}, TS)


async def test_an_empty_run_loads_as_nothing() -> None:
    assert await InMemoryJournal().load("nope") == []


async def test_append_then_load_round_trips_in_order() -> None:
    j = InMemoryJournal()
    await j.append("r1", JournalAppend(_start(), MsgId.start("r1"), 0))
    first = (await j.load("r1"))[0]
    await j.append("r1", JournalAppend(_begin(0), MsgId.step_begin("r1", 0), first.stream_seq))

    loaded = await j.load("r1")
    assert [le.entry["kind"] for le in loaded] == ["start", "step-begin"]
    assert loaded[0].stream_seq < loaded[1].stream_seq


async def test_a_stale_expect_last_seq_fences() -> None:
    j = InMemoryJournal()
    await j.append("r1", JournalAppend(_start(), MsgId.start("r1"), 0))
    # A zombie that still believes the subject is empty.
    with pytest.raises(FencedError):
        await j.append("r1", JournalAppend(_begin(0), MsgId.step_begin("r1", 0), 0))


async def test_a_duplicate_msg_id_dedups_only_while_the_cas_still_matches() -> None:
    j = InMemoryJournal()
    first = await j.append("r1", JournalAppend(_start(), MsgId.start("r1"), 0))
    assert first.duplicate is False
    # Same msgID, with the CAS advanced to the current head: the entry already exists, so this is a
    # true no-op rather than a second write.
    again = await j.append("r1", JournalAppend(_start(), MsgId.start("r1"), first.stream_seq))
    assert again.duplicate is True
    assert again.stream_seq == first.stream_seq
    assert len(await j.load("r1")) == 1


async def test_a_retry_from_a_stale_view_fences_rather_than_deduplicating() -> None:
    """Idempotency in the journal path comes from reload-on-fence, NOT from the dedup flag.

    Every journal append carries the CAS, so the ambiguous-ack retry — the client never saw the ack,
    so it republishes from its old view — fences rather than returning ``duplicate``. The caller
    reloads and discovers the entry is already recorded. ``duplicate`` is the underlying NATS
    primitive showing through; it is not the mechanism anything relies on.
    """
    j = InMemoryJournal()
    await j.append("r1", JournalAppend(_start(), MsgId.start("r1"), 0))
    with pytest.raises(FencedError):
        await j.append("r1", JournalAppend(_start(), MsgId.start("r1"), 0))


async def test_cas_dominates_dedup() -> None:
    """The sharp one, and the reason a zombie cannot slip a write past the fence.

    A stale ``expect_last_seq`` fences EVEN when the msgID is a duplicate — the CAS is evaluated
    first. Verified against a real server on the TypeScript side; the double must not be softer.
    """
    j = InMemoryJournal()
    await j.append("r1", JournalAppend(_start(), MsgId.start("r1"), 0))
    first = (await j.load("r1"))[0]
    await j.append("r1", JournalAppend(_begin(0), MsgId.step_begin("r1", 0), first.stream_seq))

    # Same msgID as an entry that already exists, but the subject has moved on: fence, not duplicate.
    with pytest.raises(FencedError):
        await j.append("r1", JournalAppend(_begin(0), MsgId.step_begin("r1", 0), first.stream_seq))


async def test_runs_are_independent_logs() -> None:
    j = InMemoryJournal()
    await j.append("r1", JournalAppend(_start(), MsgId.start("r1"), 0))
    await j.append("r2", JournalAppend(_start(), MsgId.start("r2"), 0))
    assert len(await j.load("r1")) == 1
    assert len(await j.load("r2")) == 1


async def test_stream_sequences_are_globally_monotonic_across_runs() -> None:
    # NATS-faithful: the stream seq is the STREAM's, not the subject's, so two runs interleave.
    j = InMemoryJournal()
    a = await j.append("r1", JournalAppend(_start(), MsgId.start("r1"), 0))
    b = await j.append("r2", JournalAppend(_start(), MsgId.start("r2"), 0))
    assert b.stream_seq > a.stream_seq


async def test_an_unencodable_value_is_a_serialization_error_not_a_cas_outcome() -> None:
    # Deterministic: retrying the same bytes can never succeed, so the run must FAIL rather than
    # NAK-loop. This is exactly the bug the TypeScript double was blind to until it encoded eagerly.
    j = InMemoryJournal()
    entry = stamp(
        {"kind": "start", "seq": 0, "wf": "wf", "wfVersion": 1, "input": {"bad": float("nan")}},
        TS,
    )
    with pytest.raises(SerializationError):
        await j.append("r1", JournalAppend(entry, MsgId.start("r1"), 0))
    assert await j.load("r1") == []


async def test_an_oversized_entry_is_payload_too_large_not_a_retry() -> None:
    j = InMemoryJournal(max_payload=200)
    entry = stamp(
        {"kind": "start", "seq": 0, "wf": "wf", "wfVersion": 1, "input": {"blob": "x" * 500}}, TS
    )
    with pytest.raises(PayloadTooLargeError):
        await j.append("r1", JournalAppend(entry, MsgId.start("r1"), 0))


async def test_payload_too_large_is_a_serialization_error() -> None:
    # The subclassing is load-bearing: callers branch on "deterministic → fail the run", and an
    # oversized payload belongs on that side of the line, not with transient infrastructure.
    assert issubclass(PayloadTooLargeError, SerializationError)


async def test_a_load_is_a_snapshot_and_does_not_tear() -> None:
    j = InMemoryJournal()
    await j.append("r1", JournalAppend(_start(), MsgId.start("r1"), 0))
    entries = await j.load("r1")
    first = entries[0]
    await j.append("r1", JournalAppend(_begin(0), MsgId.step_begin("r1", 0), first.stream_seq))
    # The list handed out earlier must not have grown underneath its holder.
    assert len(entries) == 1
