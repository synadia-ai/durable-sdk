"""synadia-durable — the Journal seam between the pure core and the NATS shell, plus an in-memory
implementation for unit tests. Mirrors ``src/journal/store.ts``.

Both implementations honour the same contract:

* :meth:`Journal.append` is an optimistic CAS keyed by ``expect_last_seq`` (the run subject's last
  stream sequence); a mismatch raises :class:`~synadia_durable.errors.FencedError` (NATS 10071).
* **CAS dominates dedup** — verified against a real server on the TypeScript side: a stale
  ``expect_last_seq`` fences *even when the msgID is a duplicate*, so a concurrent zombie cannot slip
  a write past the fence. A duplicate msgID resolves as a duplicate only when the CAS still matches,
  which makes it a true no-op.

This is the single primitive the whole design rests on (PROTOCOL §2).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from synadia_durable.errors import (
    FencedError,
    PayloadTooLargeError,
    SerializationError,
)
from synadia_durable.journal.blobs import encode_payload, offload_entry, resolve_entry
from synadia_durable.journal.codec import json_codec
from synadia_durable.journal.entry import LoadedEntry
from synadia_durable.journal.payload_codec import PayloadCodec

if TYPE_CHECKING:
    from typing import Protocol

    from synadia_durable.journal.blobs import BlobStore
    from synadia_durable.journal.codec import Codec
    from synadia_durable.journal.entry import JournalEntry
else:
    from typing import Protocol

__all__ = ["AppendResult", "InMemoryJournal", "Journal", "JournalAppend"]


@dataclass(frozen=True, slots=True)
class JournalAppend:
    """One optimistic append: the entry, its dedup id, and the CAS it is conditioned on."""

    entry: JournalEntry
    msg_id: str
    #: Expected stream-seq of the last message on this run's journal subject (0 if none).
    expect_last_seq: int


@dataclass(frozen=True, slots=True)
class AppendResult:
    #: Stream sequence assigned to the entry (or the original one on a duplicate).
    stream_seq: int
    #: True if the msgID was deduplicated — the entry already existed and nothing was written.
    duplicate: bool


class Journal(Protocol):
    """The append/load seam. Injected into the core; implemented over JetStream by the shell."""

    async def load(self, run_id: str) -> list[LoadedEntry]:
        """The full ordered journal for a run (empty if none), with blob refs resolved to values."""
        ...

    async def append(self, run_id: str, append: JournalAppend) -> AppendResult:
        """Append one entry with optimistic concurrency. Raises ``FencedError`` if the CAS fails."""
        ...


class InMemoryJournal:
    """An in-memory :class:`Journal` with NATS-faithful semantics.

    Per-run subject = per-run ordered log; a global stream-seq counter; msgID dedup; CAS via
    ``expect_last_seq``. This is what lets the replay engine and the two-phase crash logic be tested
    without a server. Dedup here is unbounded where NATS' is time-bounded — correctness never relies
    on it, ``seq`` + CAS is the guard.

    Entries are stored **encoded** (round-tripped through the codec), exactly like the NATS shell, so
    the pure suite is not blind to serialization — the gap that once let a non-serializable value's
    infinite-retry bug hide on the TypeScript side.

    With ``blobs`` and ``offload_threshold_bytes`` it also mirrors large-payload offload faithfully:
    an encoded entry over the threshold writes its payload slot to the blob store FIRST
    (blob-before-pointer, so a CAS loser's blob still lands exactly as in the real flow), stores the
    ref'd ``v:2`` form, and :meth:`load` resolves refs — digest-verified — back to inline values.
    Without the threshold the store is read-only for blobs, which is what it was before: refs could
    be resolved but never written, so offload could only be tested by driving the pure functions.
    """

    def __init__(
        self,
        codec: Codec = json_codec,
        *,
        max_payload: int | None = None,
        blobs: BlobStore | None = None,
        offload_threshold_bytes: int | None = None,
        payload_codec: PayloadCodec | None = None,
    ) -> None:
        self._codec = codec
        self._max_payload = max_payload
        self._blobs = blobs
        self._offload_threshold_bytes = offload_threshold_bytes
        self._payload_codec = payload_codec
        self._logs: dict[str, list[tuple[bytes, int]]] = {}
        self._seen: dict[str, int] = {}
        self._stream_seq = 0

    async def load(self, run_id: str) -> list[LoadedEntry]:
        # Snapshot before the per-entry awaits below: an append landing mid-load must not tear the
        # view (the NATS shell reads a counted batch — the same consistency).
        log = list(self._logs.get(run_id, ()))
        out: list[LoadedEntry] = []
        for data, stream_seq in log:
            entry = await resolve_entry(
                run_id,
                self._codec.decode(data),
                self._codec,
                self._blobs,
                self._payload_codec,
            )
            out.append(LoadedEntry(entry=entry, stream_seq=stream_seq))
        return out

    async def append(self, run_id: str, append: JournalAppend) -> AppendResult:
        # The payload transform runs BEFORE the structural encode: it rewrites the entry, so the
        # bytes stored must be of the rewritten form. Its own error classes propagate unchanged.
        entry = await encode_payload(run_id, append.entry, self._codec, self._payload_codec)
        # Encode FIRST (faithful to the shell): an unencodable entry is a deterministic failure that
        # raises SerializationError, never a CAS or dedup outcome.
        try:
            data = self._codec.encode(entry)
        except (TypeError, ValueError) as e:
            raise SerializationError(
                run_id,
                f"run {run_id}: cannot encode {entry['kind']} entry (seq {entry['seq']}): {e}",
            ) from e

        # Offload BEFORE the max_payload check (faithful: offload exists to keep an oversized
        # payload from ever reaching the publish). The blob write lands even if the CAS below
        # fences — the real blob-before-pointer ordering. Error classes pass through untouched: an
        # encode failure arrives as SerializationError (deterministic), a blob-store put failure as
        # its raw error (transient infrastructure, which the worker retries).
        if self._blobs is not None and self._offload_threshold_bytes is not None:
            _stored, data = await offload_entry(
                run_id,
                entry,
                data,
                self._codec,
                self._blobs,
                self._offload_threshold_bytes,
            )

        # Over max_payload is likewise deterministic for this value — republishing the same bytes can
        # never succeed — so it fails the run instead of being retried as infrastructure.
        if self._max_payload is not None and len(data) > self._max_payload:
            raise PayloadTooLargeError(
                run_id,
                f"run {run_id}: {entry['kind']} entry (seq {entry['seq']}) encodes to "
                f"{len(data)} bytes — over the server max_payload of {self._max_payload}",
            )

        log = self._logs.setdefault(run_id, [])
        last_seq = log[-1][1] if log else 0
        # CAS dominates dedup (real-NATS faithful): evaluate the optimistic CAS FIRST, so a stale
        # expect_last_seq fences even a duplicate msgID and a concurrent zombie cannot slip through.
        if append.expect_last_seq != last_seq:
            raise FencedError(
                run_id,
                f"run {run_id}: expected last seq {append.expect_last_seq}, have {last_seq}",
            )

        seen = self._seen.get(append.msg_id)
        if seen is not None:
            return AppendResult(stream_seq=seen, duplicate=True)

        self._stream_seq += 1
        log.append((data, self._stream_seq))
        self._seen[append.msg_id] = self._stream_seq
        return AppendResult(stream_seq=self._stream_seq, duplicate=False)
