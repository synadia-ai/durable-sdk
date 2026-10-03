"""synadia-durable — the run status index (``DE_RUNS`` KV). PROTOCOL §10.

A cache/hint **only**: the journal is the source of truth (invariant #1), so status writes are plain
puts and never a progress gate. Mirrors ``src/nats/runs.ts``.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, NotRequired, TypedDict, cast

from nats.js.errors import KeyNotFoundError

from synadia_durable._internal.json import decode_json, encode_json
from synadia_durable.journal.blobs import store_payload_blob
from synadia_durable.journal.codec import json_codec
from synadia_durable.journal.entry import BlobRef, ErrorShape

if TYPE_CHECKING:
    from nats.js.kv import KeyValue

    from synadia_durable.journal.blobs import BlobStore
    from synadia_durable.journal.codec import Codec
    from synadia_durable.types import RunState

__all__ = ["TERMINAL_STATUSES", "RunRecord", "RunsStore"]

#: Never downgraded by a best-effort non-terminal hint — the journal's terminal wins.
TERMINAL_STATUSES: frozenset[str] = frozenset({"completed", "failed", "continued", "cancelled"})


class RunRecord(TypedDict):
    """The per-run status record. A hint for observers; never read to gate progress."""

    status: RunState
    wf: str
    version: int
    updatedMs: int
    result: NotRequired[Any]
    #: Offloaded form: ``result`` was too large for a KV value and lives in the blob store. The hint
    #: stays a hint — ``handle.result()`` resolves values through the journal, never through this.
    resultRef: NotRequired[BlobRef]
    error: NotRequired[ErrorShape]
    errorRef: NotRequired[BlobRef]
    #: For status ``continued``: the next generation's runId.
    next: NotRequired[str]
    #: For status ``blocked``: a human-readable reason.
    detail: NotRequired[str]
    #: For status ``suspended``: the earliest pending wake, when parked on a timer.
    dueMs: NotRequired[int]


def _now_ms() -> int:
    return int(time.time() * 1000)


class RunsStore:
    """Reads and writes the status index."""

    def __init__(
        self,
        kv: KeyValue,
        *,
        blobs: BlobStore | None = None,
        threshold_bytes: int | None = None,
        codec: Codec = json_codec,
        redact_payloads: bool = False,
    ) -> None:
        self._kv = kv
        self._blobs = blobs
        self._threshold = threshold_bytes
        self._codec = codec
        #: Set when a payload_codec is configured: the terminal hint then carries NO payload.
        #: Encrypting the journal while this KV kept the plaintext result would defeat the point —
        #: the finished value is the most sensitive thing a run produces and a status-KV read right
        #: is the weakest one we hand out (the dashboard's, the read-only observer's). Redacting
        #: costs nothing: this record is a HINT and ``result()`` resolves through the journal.
        self._redact = redact_payloads

    async def get(self, run_id: str) -> RunRecord | None:
        try:
            entry = await self._kv.get(run_id)
        except KeyNotFoundError:
            return None
        if entry.value is None:
            return None
        return cast("RunRecord", decode_json(entry.value))

    async def set(self, run_id: str, record: RunRecord) -> None:
        await self._kv.put(run_id, encode_json(await self._shrink(run_id, record)))

    async def _shrink(self, run_id: str, record: RunRecord) -> RunRecord:
        """Offload an over-threshold record's result/error, so a big outcome cannot wedge the hint.

        The terminal journal append already offloaded the same value with the same codec, so this
        converges on the SAME content-addressed object and the put dedups. Without it, finalizing a
        run whose result is near ``max_payload`` fails the KV put — and the terminal re-drive that
        follows fails it again, forever.
        """
        if self._redact:
            out = dict(record)
            out.pop("result", None)
            out.pop("error", None)
            return cast("RunRecord", out)
        if self._blobs is None or self._threshold is None:
            return record
        if len(encode_json(record)) <= self._threshold:
            return record
        out = dict(record)
        # ``None`` deliberately: with a codec configured this method returned above with the payload
        # REDACTED, so a blob written here can only hold a value that was never going to be
        # transformed anyway. See the redact_payloads branch.
        if out.get("result") is not None:
            ref, _ = await store_payload_blob(
                run_id,
                "the result of the status record",
                out["result"],
                self._codec,
                self._blobs,
                None,
            )
            out["resultRef"] = ref
            del out["result"]
        elif out.get("error") is not None:
            ref, _ = await store_payload_blob(
                run_id,
                "the error of the status record",
                out["error"],
                self._codec,
                self._blobs,
                None,
            )
            out["errorRef"] = ref
            del out["error"]
        return cast("RunRecord", out)

    async def mark_running(self, run_id: str, wf: str, version: int) -> None:
        await self.set(
            run_id, {"status": "running", "wf": wf, "version": version, "updatedMs": _now_ms()}
        )

    async def _mark_non_terminal(
        self,
        run_id: str,
        wf: str,
        version: int,
        status: RunState,
        *,
        detail: str | None = None,
        due_ms: int | None = None,
    ) -> None:
        """Set a non-terminal status. Best-effort: it NEVER downgrades a terminal one.

        The journal is the truth, so a worker that already finalized this run wins. Without a gating
        lease a stale read can still race a concurrent finalize — but a terminal status is never
        clobbered, and observers that need certainty read the journal.
        """
        current = await self.get(run_id)
        if current is not None and current["status"] in TERMINAL_STATUSES:
            return
        record: RunRecord = {
            "status": status,
            "wf": wf,
            "version": version,
            "updatedMs": _now_ms(),
        }
        if detail is not None:
            record["detail"] = detail
        if due_ms is not None:
            record["dueMs"] = due_ms
        await self.set(run_id, record)

    async def mark_suspended(
        self, run_id: str, wf: str, version: int, due_ms: int | None = None
    ) -> None:
        """Mark a run parked on a ``sleep``/``wait``/``call`` (never downgrades a terminal).

        ``due_ms`` is the earliest pending wake when parked on a timer — a hint for ``status()``; the
        sweep that repairs a lost wake reads ``DE_PARKED``, never this.
        """
        await self._mark_non_terminal(run_id, wf, version, "suspended", due_ms=due_ms)

    async def mark_blocked(self, run_id: str, wf: str, version: int, detail: str) -> None:
        """Mark a run BLOCKED: its recorded version is not registered on any worker, so it is parked
        (not failed) until that version is deployed."""
        await self._mark_non_terminal(run_id, wf, version, "blocked", detail=detail)

    async def finalize_completed(self, run_id: str, wf: str, version: int, result: object) -> None:
        await self.set(
            run_id,
            {
                "status": "completed",
                "wf": wf,
                "version": version,
                "updatedMs": _now_ms(),
                "result": result,
            },
        )

    async def finalize_failed(self, run_id: str, wf: str, version: int, error: ErrorShape) -> None:
        await self.set(
            run_id,
            {
                "status": "failed",
                "wf": wf,
                "version": version,
                "updatedMs": _now_ms(),
                "error": error,
            },
        )

    async def finalize_cancelled(self, run_id: str, wf: str, version: int) -> None:
        await self.set(
            run_id,
            {"status": "cancelled", "wf": wf, "version": version, "updatedMs": _now_ms()},
        )

    async def mark_continued(self, run_id: str, wf: str, version: int, next_run_id: str) -> None:
        await self.set(
            run_id,
            {
                "status": "continued",
                "wf": wf,
                "version": version,
                "updatedMs": _now_ms(),
                "next": next_run_id,
            },
        )
