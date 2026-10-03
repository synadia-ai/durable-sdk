"""synadia-durable — recording an INBOUND EVENT into a run's journal, before replay.

A timer wake, a signal, a child's outcome, a cancel request: each is journaled (idempotently, by
content) so the matching ``ctx.*`` continues on the replay that follows. Pure — it depends only on
the Journal seam, which is what lets a simulation drive THIS code, not a model of it, under fences
it chooses. PROTOCOL §6.4. Mirrors ``src/core/inbound.ts``.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, cast

from synadia_durable.errors import FencedError
from synadia_durable.journal.blobs import with_blob_ref
from synadia_durable.journal.entry import NON_COMMAND_SEQ, MsgId, stamp
from synadia_durable.journal.store import JournalAppend

if TYPE_CHECKING:
    from collections.abc import Callable

    from synadia_durable.core.work_item import WorkItem
    from synadia_durable.journal.entry import BlobRef, JournalEntry, LoadedEntry
    from synadia_durable.journal.store import Journal

__all__ = ["EVENT_APPEND_ATTEMPTS", "append_event", "has_resolve", "has_signal", "record_inbound"]

#: How many times one inbound event's append is retried past a fence before the item is NAK'd. A
#: fence means a concurrent writer won, not that the event landed — see `append_event`.
EVENT_APPEND_ATTEMPTS = 5


def has_signal(loaded: list[LoadedEntry], signal_id: str) -> bool:
    return any(
        le.entry["kind"] == "signal"
        and cast("dict[str, Any]", le.entry).get("signalId") == signal_id
        for le in loaded
    )


def has_resolve(loaded: list[LoadedEntry], seq: int) -> bool:
    return any(
        le.entry["kind"] == "resolve" and cast("dict[str, Any]", le.entry).get("resolvesSeq") == seq
        for le in loaded
    )


def _now_ms() -> int:
    return int(time.time() * 1000)


async def record_inbound(
    journal: Journal,
    item: WorkItem,
    loaded: list[LoadedEntry],
    clock: Callable[[], int] = _now_ms,
) -> list[LoadedEntry]:
    """Journal the event a work item carries, and return the journal to replay from.

    Idempotent by CONTENT — a ``resolve`` only for a seq that has none, a ``signal`` only for an
    unseen signalId, one ``cancel-requested`` per run — so a redelivered wake never appends twice.
    Returns ``loaded`` itself when there was nothing to record (a ``start`` / ``resume`` item, or an
    event already present), a fresh load otherwise.

    Raises when the event could not be journaled; the caller NAKs and the item comes back. It never
    reports an event as recorded that is not in the journal.
    """
    run_id = item["runId"]
    kind = item["kind"]

    if kind == "timer":
        seq = item.get("resolvesSeq")
        if seq is not None and not has_resolve(loaded, seq):
            wake = seq
            return await append_event(
                journal,
                run_id,
                loaded,
                MsgId.resolve(run_id, wake),
                stamp({"kind": "resolve", "seq": NON_COMMAND_SEQ, "resolvesSeq": wake}, clock()),
                lambda fresh: has_resolve(fresh, wake),
            )

    elif kind == "signal":
        signal = item.get("signal")
        if signal is not None and not has_signal(loaded, signal["id"]):
            body: dict[str, Any] = {
                "kind": "signal",
                "seq": NON_COMMAND_SEQ,
                "name": signal["name"],
                "signalId": signal["id"],
            }
            # A shrunk work item carries a blob ref instead of inline data: journal the ref
            # VERBATIM — the value is already durably in the blob store and load() resolves it.
            # `with_blob_ref` is what stamps v:2 (or v:3 when the blob holds payload-codec
            # output, which the WRITER tells us via `payloadEnc` — this worker's own config
            # cannot say what somebody else put in the bucket).
            body["data"] = signal.get("data")
            entry = stamp(cast("JournalEntry", body), clock())
            if "dataRef" in signal:
                entry = with_blob_ref(
                    entry,
                    "data",
                    signal["dataRef"],
                    transformed=item.get("payloadEnc") is True,
                )
            signal_id = signal["id"]
            return await append_event(
                journal,
                run_id,
                loaded,
                MsgId.signal(run_id, signal_id),
                entry,
                lambda fresh: has_signal(fresh, signal_id),
            )

    elif kind == "child-done":
        seq = item.get("resolvesSeq")
        child = item.get("child")
        if seq is not None and child is not None and not has_resolve(loaded, seq):
            body = {"kind": "resolve", "seq": NON_COMMAND_SEQ, "resolvesSeq": seq}
            transformed = item.get("payloadEnc") is True
            slot = "value" if child.get("ok") else "error"
            ref = cast("BlobRef | None", child.get(f"{slot}Ref"))
            if ref is None:
                body[slot] = child.get(slot)
            entry = stamp(cast("JournalEntry", body), clock())
            if ref is not None:
                entry = with_blob_ref(entry, slot, ref, transformed=transformed)
            done_seq = seq
            return await append_event(
                journal,
                run_id,
                loaded,
                MsgId.resolve(run_id, done_seq),
                entry,
                lambda fresh: has_resolve(fresh, done_seq),
            )

    elif kind == "cancel" and not any(le.entry["kind"] == "cancel-requested" for le in loaded):
        # Cancellation enters the journal like a signal: record it before replay so the engine
        # hard-stops at the next command boundary.
        body = {"kind": "cancel-requested", "seq": NON_COMMAND_SEQ}
        reason = item.get("reason")
        if reason is not None:
            body["reason"] = reason
        return await append_event(
            journal,
            run_id,
            loaded,
            MsgId.cancel(run_id),
            stamp(cast("JournalEntry", body), clock()),
            lambda fresh: any(le.entry["kind"] == "cancel-requested" for le in fresh),
        )

    return loaded


async def append_event(
    journal: Journal,
    run_id: str,
    loaded: list[LoadedEntry],
    msg_id: str,
    entry: JournalEntry,
    recorded: Callable[[list[LoadedEntry]], bool],
) -> list[LoadedEntry]:
    """Journal an inbound event, RETRYING past a fence, and return the fresh journal.

    A fence here means the append did **not** land — someone else advanced the log between this
    worker's load and its CAS. The msgID makes the event at-most-once; nothing but a retry makes
    it at-least-once. Treating a fence as "already handled" loses the event, and the work item
    carrying it is acked moments later: for a ``child-done`` that is a parent parked forever on
    a child that finished. (Found by the multi-worker chaos test, in both SDKs — the two events
    that race here are two DIFFERENT children of one parent, recorded by two workers, and event
    recording is deliberately ungated by the lease so a cancel can always land.)

    ``recorded`` is the caller's own "is it already there?" predicate: a fence caused by another
    worker recording THIS event is success, not a retry. If the event is still absent after the
    attempts, this raises — the caller NAKs, and the item comes back rather than being lost.
    """
    current = loaded
    for _ in range(EVENT_APPEND_ATTEMPTS):
        last = current[-1].stream_seq if current else 0
        try:
            await journal.append(run_id, JournalAppend(entry, msg_id, last))
            return await journal.load(run_id)
        except FencedError:
            current = await journal.load(run_id)
            if recorded(current):
                return current  # another worker recorded this very event: nothing to redo
    raise RuntimeError(
        f"run {run_id}: could not journal an inbound event after "
        f"{EVENT_APPEND_ATTEMPTS} attempts (contended); redelivering"
    )
