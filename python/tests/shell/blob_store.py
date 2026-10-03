"""synadia-durable — the blob object store over JetStream (``DE_BLOBS``). PROTOCOL §5.6.

Writes go through the object-store client. READS do not: its ``get`` streams the chunks through an
ordered push subscription that it unsubscribes but whose consumer is never deleted (LEARNINGS #59 —
one consumer left behind per blob read, and consumers are capped per stream on a managed service).
The object store's on-stream layout is a NATS ADR (meta at ``$O.<bucket>.M.<name>``, chunks at
``$O.<bucket>.C.<nuid>``), so the chunks are read here with one batched direct get (direct.py), no
consumer, and verified against the size the meta records.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from nats.js import api
from nats.js.errors import NotFoundError, ObjectNotFoundError

from synadia_durable._internal.metrics import Metrics, emit_safe
from shell.direct import read_subject
from synadia_durable.errors import BlobMissingError

if TYPE_CHECKING:
    from nats.aio.client import Client as NatsClient

__all__ = ["NatsBlobStore"]


#: The object-store clients' default chunk size (both SDKs). A chunk is one message, so it can never
#: exceed the server's max_payload; the put above clamps it.
_DEFAULT_CHUNK_BYTES = 128 * 1024


class NatsBlobStore:
    """A :class:`~synadia_durable.journal.blobs.BlobStore` over a JetStream object store.

    The bucket is opened lazily: a deployment that never offloads should not require it to exist, and
    a reader that meets a ref without a bucket must fail the same loud way as one with no store at
    all (a run parked, never a terminal).
    """

    def __init__(self, nc: NatsClient, bucket: str, metrics: Metrics | None = None) -> None:
        self._nc = nc
        self._js = nc.jetstream()
        self._bucket = bucket
        self._store: object | None = None
        self._metrics = metrics

    async def _open(self) -> object:
        if self._store is None:
            self._store = await self._js.object_store(self._bucket)
        return self._store

    async def get(self, name: str) -> bytes:
        started = time.monotonic()
        try:
            store = await self._open()
            # The meta record (a last-by-subject stream get, no consumer).
            info = await store.get_info(name)  # type: ignore[attr-defined]
        except (ObjectNotFoundError, NotFoundError) as e:
            # Definitively absent (or the bucket is gone). OPERATIONAL and possibly recoverable —
            # restore the object, or cancel the run — so the caller parks rather than failing the
            # run terminally. A transient store failure is a different exception and propagates as
            # ordinary infrastructure, to be retried.
            self._emit("blob.get", name, 0, started, "missing")
            raise BlobMissingError(
                "?", -1, name, f"object not found in {self._bucket}: {e}", "absent"
            ) from e
        if info.deleted:
            self._emit("blob.get", name, 0, started, "missing")
            raise BlobMissingError("?", -1, name, f"object {name} was deleted", "absent")
        # The chunks: one batched direct get on the object's chunk subject, in stream order.
        try:
            read = await read_subject(
                self._nc, f"OBJ_{self._bucket}", f"$O.{self._bucket}.C.{info.nuid}"
            )
        except Exception:
            self._emit("blob.get", name, 0, started, "error")
            raise
        data = b"".join(m.data for m in read)
        # Completeness: the meta records the byte size, and a direct read may be a prefix (a
        # follower may answer, a chunk may still be in flight). Short = transient, retried by the
        # caller — never handed to the digest check as "corrupt".
        if len(data) != (info.size or 0) or len(read) != (info.chunks or 0):
            self._emit("blob.get", name, len(data), started, "error")
            raise RuntimeError(
                f"blob {name}: read {len(read)} chunks / {len(data)} bytes, meta says "
                f"{info.chunks} / {info.size} (transient short read)"
            )
        self._emit("blob.get", name, len(data), started, "ok")
        return data

    async def put(self, name: str, data: bytes) -> None:
        """Write a blob. Overwriting is safe: the name is the content digest, so the bytes match.

        A failure propagates raw — writing a blob is ordinary infrastructure, and the caller
        classifies it as transient and retries. It must NOT become a run failure: the entry that
        would have referenced this object was never published (blob-before-pointer), so a retry
        starts from the same place.
        """
        started = time.monotonic()
        try:
            store = await self._open()
            # nats-py chunks at a fixed 128 KiB unless told otherwise, and never asks the server —
            # on a max_payload below that (64 KiB is a real setting; the test harness's `tight`
            # profile has it) every put died client-side with MaxPayloadError, and inside a worker
            # that is a NAK loop on an entry that can never be written. Chunk at
            # min(default, max_payload), which is what the TypeScript object-store client does.
            chunk = min(_DEFAULT_CHUNK_BYTES, int(self._nc.max_payload))
            meta = api.ObjectMeta(name=name, options=api.ObjectMetaOptions(max_chunk_size=chunk))
            await store.put(name, data, meta=meta)  # type: ignore[attr-defined]
        except Exception:
            self._emit("blob.put", name, len(data), started, "error")
            raise
        self._emit("blob.put", name, len(data), started, "ok")

    def _emit(self, event: str, name: str, size: int, started: float, outcome: str) -> None:
        emit_safe(
            self._metrics,
            event,
            {
                "name": name,
                "bytes": size,
                "ms": round((time.monotonic() - started) * 1000),
                "outcome": outcome,
            },
        )
