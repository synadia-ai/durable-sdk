"""synadia-durable — the SDK half of the chunked upload and download (the engine's
``API.md`` "Sizes", ``put``, ``get``; the engine's findings F25, this repo's F1). Mirrors
``src/engine/blobs.ts``.

A value whose request would pass ``request_max`` goes up in ``put`` parts first and travels as the
ref the engine answers; a ref the engine hands back because a value would not fit a reply comes down
in ``get`` parts, its digest checked. Both directions run through the library's own offload code
(``offload_entry``, ``store_payload_blob``, ``resolve_entry`` of ``journal/blobs.py``) with the
engine as its blob store, so a ref, its name (``{runId}/sha256:<hex>``), its ``v:2`` / ``v:3``
marking and its digest check are the library's bytes. The pure core never sees a ref: the worker's
journal is resolved before replay.
"""

from __future__ import annotations

import base64
import json
import math
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from synadia_durable._engine.errors import EngineUnavailableError, InvalidRequestError
from synadia_durable._engine.transport import REQUEST_MARGIN_MS
from synadia_durable._internal.metrics import Metrics, emit_safe
from synadia_durable.errors import BlobMissingError, PayloadTooLargeError, SerializationError
from synadia_durable.journal.blobs import digest_of

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from synadia_durable._engine.names import Op
    from synadia_durable._engine.transport import Transport
    from synadia_durable.journal.entry import BlobRef

__all__ = [
    "PUT_MS_PER_MIB",
    "PUT_RTTS_PER_CHUNK",
    "STORE_CHUNK_BYTES",
    "EngineBlobs",
    "Sizes",
    "body_bytes",
    "part_bytes_of",
    "put_timeout_ms",
    "run_of_blob",
]

#: The headroom the engine keeps under max_payload for a request's envelope (API.md "Sizes").
_REQUEST_HEADROOM = 16 * 1024


def part_bytes_of(request_max: int) -> int:
    """``(request_max − 4096) / 4 × 3``: one part's raw bytes, base64 of which fits a request."""
    return (request_max - 4096) // 4 * 3


@dataclass(frozen=True, slots=True)
class Limits:
    """What ``info``'s ``limits`` says a request may weigh, and the raw bytes of one ``put`` part."""

    request_max: int
    part_bytes: int


class Sizes:
    """The connection's sizes, from the engine's ``info`` once per transport (and from the server's
    max_payload less the engine's headroom while no engine has answered)."""

    def __init__(self, request: Transport, max_payload: Callable[[], int]) -> None:
        self._request = request
        self._max_payload = max_payload
        self._known: Limits | None = None

    async def __call__(self) -> Limits:
        if self._known is not None:
            return self._known
        try:
            r = await self._request.request("info", {})
            rm = (r.get("limits") or {}).get("request_max")
            if isinstance(rm, int) and rm > 4096:
                self._known = Limits(rm, part_bytes_of(rm))
                return self._known
        except Exception:  # noqa: BLE001 — the fallback below; the request itself will say
            pass
        rm = max(8 * 1024, (self._max_payload() or 1024 * 1024) - _REQUEST_HEADROOM)
        return Limits(rm, part_bytes_of(rm))


#: The engine's object-store chunk: one acked publish each, so one dependent round trip each.
STORE_CHUNK_BYTES = 128 * 1024
#: Round trips allowed per chunk the engine stores, reads back or deletes for a ``put``.
PUT_RTTS_PER_CHUNK = 4
#: The slowest store a ``put`` waits for: a MiB of what the engine moves per this many ms.
PUT_MS_PER_MIB = 1_000


def put_timeout_ms(stored: int, rtt_ms: float) -> int:
    """How long a ``put`` part may take to answer (this repo's F33). A part is staged in the blob
    store before it is answered; the last one is answered once the engine has read every part back,
    checked the digest, written the blob and deleted the parts (API.md ``put``). So the wait grows
    with the bytes the engine moves for it (``stored``: the part's own, plus the value twice on the
    last), at a round trip per store chunk and a floor on the store's throughput, over the margin
    every request gets. A slow store is waited for, not refused. Mirrors ``putTimeoutMs``."""
    chunks = -(-stored // STORE_CHUNK_BYTES)
    return math.ceil(
        REQUEST_MARGIN_MS
        + chunks * PUT_RTTS_PER_CHUNK * max(0.0, rtt_ms)
        + stored / (1024 * 1024) * PUT_MS_PER_MIB
    )


def _deterministic_put(run_id: str, size: int, part: int, e: Exception) -> Exception:
    """A ``put`` that the engine did not answer within its size-scaled wait, or refused as
    malformed, is deterministic for this value (this repo's F33): a PayloadTooLargeError or
    SerializationError, which the pure core fails the run on (and a client call raises), rather
    than a transient the item's redelivery would meet again with the same value, for ever. A lease
    that expired, a closed connection, no engine at all and the engine's own ``unavailable`` keep
    their meaning."""
    if isinstance(e, EngineUnavailableError) and e.timed_out:
        return PayloadTooLargeError(
            run_id,
            f"put (run {run_id}): the engine did not store a {size}-byte value in time (part "
            f"{part}); the blob store is too slow for a value this size: {e}",
        )
    if isinstance(e, InvalidRequestError):
        return SerializationError(run_id, f"put (run {run_id}): {e}")
    return e


class BlobRequest(Protocol):
    """The transport's ``request``, or the worker's leased one: a ``put`` gives its own timeout."""

    def __call__(
        self, op: Op, body: dict[str, Any], /, *, timeout_ms: int | None = None
    ) -> Awaitable[dict[str, Any]]: ...


def run_of_blob(name: str) -> str:
    """The run a blob belongs to: its name is ``{runId}/sha256:<hex>``."""
    return name.split("/", 1)[0] if "/" in name else ""


def body_bytes(body: dict[str, Any]) -> int:
    """The encoded size of a request body, as the transport will send it."""
    return len(
        json.dumps(body, allow_nan=False, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    )


class EngineBlobs:
    """The engine as the library's ``BlobStore``: ``put`` uploads in parts and checks that the ref the
    engine answers names the same blob; ``get`` downloads in parts, and raises the library's
    :class:`BlobMissingError` when the engine says the blob is gone (``get``'s ``blob_gone``, which
    the transport maps to it), so the worker parks the run; bytes the engine found not to be the
    digest's come up as :class:`BlobIntegrityError`, which parks it the same way.

    ``lease`` is the item's ``(run_id, lease)`` from an execution: sent with a part of that run's
    blob only (another run's, a child's input, carries none).
    """

    def __init__(
        self,
        request: BlobRequest,
        sizes: Sizes,
        *,
        lease: tuple[str, str] | None = None,
        metrics: Metrics | None = None,
        rtt: Callable[[], Awaitable[float]] | None = None,
    ) -> None:
        self._request = request
        self._sizes = sizes
        self._lease = lease
        self._metrics = metrics
        #: The connection's round trip in seconds (``nc.rtt``), measured once per upload.
        self._rtt = rtt

    async def _rtt_ms(self) -> float:
        if self._rtt is None:
            return 0.0
        try:
            return (await self._rtt()) * 1000
        except Exception:  # noqa: BLE001 — the request itself will say what is wrong
            return 0.0

    def _lease_for(self, run_id: str) -> dict[str, str]:
        if self._lease is not None and self._lease[0] == run_id:
            return {"lease": self._lease[1]}
        return {}

    def _emit(self, name: str, blob: str, size: int, started: float, outcome: str) -> None:
        emit_safe(
            self._metrics,
            name,
            {
                "name": blob,
                "bytes": size,
                "ms": round((time.monotonic() - started) * 1000),
                "outcome": outcome,
            },
        )

    async def put(self, name: str, data: bytes) -> None:
        """Upload ``data`` as the blob ``name`` (``{runId}/sha256:<hex>``, the library's address)."""
        ref = await self.upload(run_of_blob(name), data)
        if ref["name"] != name:
            raise EngineUnavailableError(
                "put", "internal", f"put: the engine stored {ref['name']}, not {name}"
            )

    async def upload(self, run_id: str, data: bytes) -> BlobRef:
        """Upload ``data`` into ``run_id``'s namespace in parts; the ref the engine answers."""
        started = time.monotonic()
        digest = digest_of(data)
        name = f"{run_id}/{digest}"
        try:
            part_bytes = (await self._sizes()).part_bytes
            rtt_ms = await self._rtt_ms()
            upload = str(uuid.uuid4())
            last = max(0, -(-len(data) // part_bytes) - 1)
            ref: BlobRef | None = None
            for part in range(last + 1):
                raw = data[part * part_bytes : (part + 1) * part_bytes]
                body: dict[str, Any] = {
                    "runId": run_id,
                    **self._lease_for(run_id),
                    "name": upload,
                    "part": part,
                    "bytes": base64.b64encode(raw).decode("ascii"),
                }
                stored = len(raw)
                if part == last:
                    body |= {"last": True, "digest": digest}
                    stored += 2 * len(data)
                try:
                    r = await self._request("put", body, timeout_ms=put_timeout_ms(stored, rtt_ms))
                except Exception as e:
                    raise _deterministic_put(run_id, len(data), part, e) from e
                ref = r.get("ref")
            if ref is None:
                raise EngineUnavailableError(
                    "put", "internal", "put: the last part answered no ref"
                )
        except Exception:
            self._emit("blob.put", name, len(data), started, "error")
            raise
        self._emit("blob.put", name, len(data), started, "ok")
        return ref

    async def get(self, name: str) -> bytes:
        """Download the blob ``name`` in parts."""
        started = time.monotonic()
        run_id = run_of_blob(name)
        parts: list[bytes] = []
        size = 0
        try:
            part = 0
            while True:
                r = await self._request(
                    "get", {"runId": run_id, **self._lease_for(run_id), "name": name, "part": part}
                )
                b = base64.b64decode(r.get("bytes") or "")
                parts.append(b)
                size += len(b)
                if r.get("last") or not b or size >= int(r.get("size", 0)):
                    break
                part += 1
        except BlobMissingError:
            # `get` answers `blob_gone` for a blob that is gone (API.md, the engine's F30): a
            # definitive absence, which the transport made the library's BlobMissingError.
            self._emit("blob.get", name, 0, started, "missing")
            raise
        except Exception:
            self._emit("blob.get", name, size, started, "error")
            raise
        self._emit("blob.get", name, size, started, "ok")
        return b"".join(parts)
