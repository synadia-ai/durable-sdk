"""synadia-durable — the SDK half of the chunked upload and download (``durable-engine``
``docs/API.md`` "Sizes", ``put``, ``get``; the engine's findings F25, this repo's F1). Mirrors
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
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from synadia_durable._engine.errors import EngineUnavailableError
from synadia_durable._internal.metrics import Metrics, emit_safe
from synadia_durable.errors import BlobMissingError
from synadia_durable.journal.blobs import digest_of

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from synadia_durable._engine.names import Op
    from synadia_durable._engine.transport import Transport
    from synadia_durable.journal.entry import BlobRef

__all__ = ["EngineBlobs", "Sizes", "body_bytes", "part_bytes_of", "run_of_blob"]

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
    :class:`BlobMissingError` when the engine says the blob is gone (``get``'s ``internal``), so
    ``resolve_entry`` names the run and the entry and the worker parks the run.

    ``lease`` is the item's ``(run_id, lease)`` from an execution: sent with a part of that run's
    blob only (another run's, a child's input, carries none).
    """

    def __init__(
        self,
        request: Callable[[Op, dict[str, Any]], Awaitable[dict[str, Any]]],
        sizes: Sizes,
        *,
        lease: tuple[str, str] | None = None,
        metrics: Metrics | None = None,
    ) -> None:
        self._request = request
        self._sizes = sizes
        self._lease = lease
        self._metrics = metrics

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
            upload = str(uuid.uuid4())
            last = max(0, -(-len(data) // part_bytes) - 1)
            ref: BlobRef | None = None
            for part in range(last + 1):
                body: dict[str, Any] = {
                    "runId": run_id,
                    **self._lease_for(run_id),
                    "name": upload,
                    "part": part,
                    "bytes": base64.b64encode(
                        data[part * part_bytes : (part + 1) * part_bytes]
                    ).decode("ascii"),
                }
                if part == last:
                    body |= {"last": True, "digest": digest}
                r = await self._request("put", body)
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
        except EngineUnavailableError as e:
            # `get` answers `internal` for a blob that is gone (API.md): a definitive absence.
            if e.code == "internal":
                self._emit("blob.get", name, 0, started, "missing")
                raise BlobMissingError(
                    run_id, -1, name, "the engine says the blob is gone from the blob store"
                ) from e
            self._emit("blob.get", name, size, started, "error")
            raise
        except Exception:
            self._emit("blob.get", name, size, started, "error")
            raise
        self._emit("blob.get", name, size, started, "ok")
        return b"".join(parts)
