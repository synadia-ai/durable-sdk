"""synadia-durable — the Journal implemented over JetStream. Mirrors ``src/nats/journal-store.ts``.

``append()`` is the optimistic publish with ``Nats-Expected-Last-Subject-Sequence`` (CAS) plus
``Nats-Msg-Id`` (dedup); a 10071 rejection becomes a :class:`FencedError`. ``load()`` reads the run's
per-run subject in order with a batched DIRECT GET — one request, no consumer — verified complete
against the leader's per-subject count. This is the shell over the one primitive the whole design
rests on.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

import nats.errors
from nats.js import api
from nats.js.errors import APIError

from synadia_durable._internal.metrics import Metrics, emit_safe
from shell.direct import DEFAULT_BATCH, read_subject
from shell.resources import js_error_code
from synadia_durable.errors import (
    FencedError,
    PayloadCodecError,
    PayloadTooLargeError,
    SerializationError,
)
from synadia_durable.journal.blobs import (
    effective_threshold,
    encode_payload,
    offload_entry,
    resolve_entry,
)
from synadia_durable.journal.codec import json_codec
from synadia_durable.journal.entry import LoadedEntry
from synadia_durable.journal.payload_codec import PayloadCodec
from synadia_durable.journal.store import AppendResult, JournalAppend

if TYPE_CHECKING:
    from nats.aio.client import Client as NatsClient

    from shell.names import Names
    from synadia_durable.journal.blobs import BlobStore
    from synadia_durable.journal.codec import Codec
    from synadia_durable.journal.entry import JournalEntry

__all__ = ["NatsJournal"]

#: NATS error code for a rejected expected-last-subject-sequence (``StreamWrongLastSequence``).
WRONG_LAST_SEQUENCE = 10071


class NatsJournal:
    """A :class:`~synadia_durable.journal.store.Journal` over a JetStream stream."""

    def __init__(
        self,
        nc: NatsClient,
        names: Names,
        codec: Codec = json_codec,
        *,
        blobs: BlobStore | None = None,
        offload_threshold_bytes: int | None = None,
        metrics: Metrics | None = None,
        payload_codec: PayloadCodec | None = None,
        read_batch: int = DEFAULT_BATCH,
    ) -> None:
        self._nc = nc
        self._js = nc.jetstream()
        self._jsm = nc.jsm()
        self._n = names
        self._read_batch = read_batch
        self._codec = codec
        self._blobs = blobs
        self._metrics = metrics
        self._payload_codec = payload_codec
        #: The connection's max_payload, enforced BEFORE publish so an oversized entry fails as a
        #: deterministic error rather than as an opaque client-side rejection.
        self._max_payload: int | None = getattr(nc, "max_payload", None) or None
        #: Encoded size above which an entry's payload slot moves to the blob store. ``None`` when
        #: offload is off, or when nothing said how big is too big — better to fail loud at
        #: max_payload than to offload against a guessed limit.
        self._threshold: int | None = (
            effective_threshold(offload_threshold_bytes, self._max_payload)
            if blobs is not None
            else None
        )

    async def resolve_loaded(self, run_id: str, entry: JournalEntry) -> JournalEntry:
        """Resolve one entry's blob ref to its inline value (a no-op for an inline entry).

        Public so observers that scan with :meth:`load_raw` can inflate only what they need.
        """
        return await resolve_entry(run_id, entry, self._codec, self._blobs, self._payload_codec)

    async def load(self, run_id: str) -> list[LoadedEntry]:
        """Load with blob refs resolved — the Journal seam's contract (the core never sees a ref)."""
        raw = await self.load_raw(run_id)
        # Resolve AFTER the read: a multi-MB blob fetch inside the batch loop would eat the read
        # timeout and masquerade as a short read.
        return [
            LoadedEntry(entry=await self.resolve_loaded(run_id, le.entry), stream_seq=le.stream_seq)
            for le in raw
        ]

    async def load_raw(self, run_id: str) -> list[LoadedEntry]:
        """Load WITHOUT resolving blob refs — for observers that only scan kinds and metadata.

        A waiting client polling for a terminal must not download every intermediate offloaded step
        value just to learn "not done yet".
        """
        started = time.monotonic()
        subject = self._n.journal_subject(run_id)

        def emit(entries: int, outcome: str) -> None:
            elapsed_ms = round((time.monotonic() - started) * 1000)
            emit_safe(
                self._metrics,
                "journal.load",
                {"run_id": run_id, "entries": entries, "ms": elapsed_ms, "outcome": outcome},
            )

        # Two requests in flight together, one round trip in all: the LEADER's per-subject message
        # count (a JS API request — leader-served) and the batched direct read of the subject
        # (which a replica may answer). The count is the completeness check the protocol requires
        # (§5.5): a direct read served by a lagging follower is a consistent PREFIX, and replaying
        # over a prefix mis-places the frontier — it fences on an already-recorded command,
        # indistinguishable from a real concurrent writer. The old path did this in four sequential
        # API calls (stream info, consumer create, fetch, consumer delete — ~570 ms at a 50 ms RTT,
        # and the create and delete serialize on the stream server-side); this is one, and it
        # creates no consumer at all, so a fleet's loads no longer churn the journal stream's
        # consumer list (LEARNINGS #58).
        async def count() -> int:
            info = await self._jsm.stream_info(self._n.journal_stream, subjects_filter=subject)
            return int((info.state.subjects or {}).get(subject, 0))

        expected, out = await asyncio.gather(count(), self._read_subject(subject))
        if len(out) < expected:
            # The two requests raced an append: the count saw it, the read did not. That is the
            # common shape of a "short" read on a single server (an append landing between two
            # concurrent requests), so read once more before calling it one — the append has
            # landed by now. A genuinely lagging follower comes up short twice and fails loud.
            out = await self._read_subject(subject)
        # A short read must FAIL LOUD, never return a truncated journal (see above; invariant #2 —
        # progress is never gated on an unverified read). Raising here is a transient infra error:
        # the worker NAKs and the redelivery re-reads. (More than the count is fine: the count was
        # taken first and an append landed in between — the read is a longer consistent prefix.)
        if len(out) < expected:
            emit(len(out), "short-read")
            raise RuntimeError(
                f"run {run_id}: journal load incomplete — got {len(out)} of {expected} entries "
                f"(transient short read)"
            )
        emit(len(out), "ok")
        return out

    async def _read_subject(self, subject: str) -> list[LoadedEntry]:
        """The run's subject as a decoded journal: one batched direct get, no consumer (direct.py)."""
        read = await read_subject(self._nc, self._n.journal_stream, subject, batch=self._read_batch)
        return [LoadedEntry(entry=self._codec.decode(m.data), stream_seq=m.seq) for m in read]

    async def append(self, run_id: str, append: JournalAppend) -> AppendResult:
        entry = append.entry
        subject = self._n.journal_subject(run_id)

        # Encode FIRST and isolate its failure: a non-serializable value is a deterministic
        # programming error (retrying cannot help), distinct from a transient publish failure.
        # Surfacing it as SerializationError is what lets the core fail the run terminally instead
        # of NAK-looping forever.
        started = time.monotonic()

        def emit(outcome: str, size: int) -> None:
            emit_safe(
                self._metrics,
                "journal.append",
                {
                    "run_id": run_id,
                    "kind": str(entry["kind"]),
                    "seq": int(entry["seq"]),
                    "bytes": size,
                    "ms": round((time.monotonic() - started) * 1000),
                    "outcome": outcome,
                },
            )

        # The payload transform runs BEFORE the structural encode (it rewrites the entry). Its
        # error classes are distinct and must survive: PayloadCodecError parks, Serialization fails.
        try:
            entry = await encode_payload(run_id, entry, self._codec, self._payload_codec)
        except PayloadCodecError:
            emit("payload-codec", 0)
            raise
        try:
            payload = self._codec.encode(entry)
        except (TypeError, ValueError) as e:
            emit("serialization", 0)
            raise SerializationError(
                run_id,
                f"run {run_id}: cannot encode {entry['kind']} entry (seq {entry['seq']}): {e}",
            ) from e

        # Large-payload offload, BEFORE the max_payload gate: an over-threshold entry writes its
        # payload slot to the blob store (blob-before-pointer) and publishes the small v:2 ref form
        # instead. The error classes pass through untouched — a SerializationError from encoding the
        # value is deterministic and fails the run, a blob-store put failure is transient
        # infrastructure and the caller NAKs.
        if (
            self._blobs is not None
            and self._threshold is not None
            and len(payload) > self._threshold
        ):
            entry, payload = await offload_entry(
                run_id, entry, payload, self._codec, self._blobs, self._threshold
            )

        # Beyond max_payload is DETERMINISTIC for this value — republishing the same bytes can never
        # succeed — so it surfaces like an encode failure and fails the run, rather than NAK-looping
        # and head-of-line-blocking the workflow's consumer. With offload on, only oversized
        # METADATA (a pathological step name, say) can still land here: there is no payload slot
        # left to move.
        if self._max_payload is not None and len(payload) > self._max_payload:
            emit("payload-too-large", len(payload))
            raise PayloadTooLargeError(
                run_id,
                f"run {run_id}: {entry['kind']} entry (seq {entry['seq']}) encodes to "
                f"{len(payload)} bytes — over the server max_payload of {self._max_payload}; "
                f"keep journaled values small, or enable large-payload offload",
            )

        headers: dict[str, str] = {
            api.Header.MSG_ID: append.msg_id,
            # The CAS. Sent even when 0 — "this subject has no messages yet" is an assertion, not
            # an absence, and dropping the header would turn the first append into an unconditional
            # publish that could not fence.
            api.Header.EXPECTED_LAST_SUBJECT_SEQUENCE: str(append.expect_last_seq),
        }
        try:
            ack = await self._js.publish(subject, payload, headers=headers)
        except APIError as e:
            if js_error_code(e) == WRONG_LAST_SEQUENCE:
                emit("fenced", len(payload))
                raise FencedError(
                    run_id,
                    f"run {run_id}: journal append fenced at "
                    f"expect_last_seq={append.expect_last_seq}",
                ) from e
            emit("error", len(payload))
            raise
        except nats.errors.MaxPayloadError as e:
            # Fallback for when the connection's limit was unknown at construction: nats-py rejected
            # it client-side. Same determinism argument as the pre-publish check above.
            emit("payload-too-large", len(payload))
            raise PayloadTooLargeError(
                run_id,
                f"run {run_id}: {entry['kind']} entry (seq {entry['seq']}) encodes to "
                f"{len(payload)} bytes — over the server max_payload",
            ) from e
        except Exception:
            # Everything that is not a server VERDICT: a timeout (the ack never came — the append
            # may or may not have landed), a dropped connection, no responders. The outcome the
            # caller acts on is unchanged (it propagates: infra, retried), but it must be COUNTED —
            # these are the failures an operator most needs to see, and the ones a fleet's
            # dashboard was blind to.
            emit("error", len(payload))
            raise

        duplicate = bool(getattr(ack, "duplicate", False))
        seq: Any = getattr(ack, "seq", 0)
        emit("duplicate" if duplicate else "ok", len(payload))
        return AppendResult(stream_seq=int(seq or 0), duplicate=duplicate)
