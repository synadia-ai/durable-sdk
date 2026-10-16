"""synadia-durable — the one way this package speaks to the engine: a JSON request on
``{prefix}.api.v1.<op>`` and its parsed reply (the engine's ``API.md`` "Transport",
"Encoding", "Errors"). A refusal ``{"error": {"code", "message"}}`` becomes a typed error here and
nowhere else. Mirrors ``src/engine/transport.ts``.

No broker error code exists in this package. A fence is a reply (``fenced``), whatever the journal's
replication answered underneath (the engine's findings F3 and F16 are about the library's
10071-only mapping on a replicated stream; the thin SDK never inherits it).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import TYPE_CHECKING, Any

import nats.errors

from synadia_durable._engine.errors import (
    EngineFencedError,
    EngineUnavailableError,
    InvalidRequestError,
    LeaseExpiredError,
    LimitError,
    RunNotFoundError,
)
from synadia_durable._engine.names import Op, api_subject
from synadia_durable._internal.logger import Logger, SilentLogger
from synadia_durable.errors import (
    BlobIntegrityError,
    BlobMissingError,
    ConnectionClosedError,
    PayloadTooLargeError,
    SerializationError,
)

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from nats.aio.client import Client as NatsClient
    from nats.aio.msg import Msg

__all__ = ["REQUEST_MARGIN_MS", "Transport"]

#: The margin over ``wait_ms`` for an op that waits (``result``, ``poll``), and the timeout of
#: every other op.
REQUEST_MARGIN_MS = 10_000

#: How often a request in flight looks at whether its connection has closed for good.
_CLOSED_TICK_S = 0.1


class Transport:
    """``await transport.request(op, body)``: one request, its reply, or a typed error."""

    def __init__(self, nc: NatsClient, prefix: str, logger: Logger | None = None) -> None:
        self._nc = nc
        self._prefix = prefix
        self._log: Logger = logger or SilentLogger()

    async def request(
        self,
        op: Op,
        body: dict[str, Any],
        *,
        wait_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        """Send ``body`` to ``{prefix}.api.v1.<op>``. For ``result`` and ``poll``, ``wait_ms`` is
        the request's own ``waitMs``; the timeout is that plus the margin. ``timeout_ms`` is the
        timeout itself, for a request whose answer waits on work of its size (``put``, blobs.py)."""
        if timeout_ms is None:
            timeout_ms = (wait_ms or 0) + REQUEST_MARGIN_MS
        timeout_s = timeout_ms / 1000
        payload = json.dumps(body, allow_nan=False, separators=(",", ":"), ensure_ascii=False)
        data = payload.encode("utf-8")
        # A request past the server's max_payload can never be sent, so it is deterministic for this
        # value: fail it here, as the engine fails one past its request_max (`too_large`). A value
        # that would make it so has gone up in `put` parts already (blobs.py); what is left over is
        # metadata no upload can shrink, or a value under `offload=False`.
        max_payload = self._nc.max_payload if self._nc.is_connected else 0
        if max_payload and len(data) > max_payload:
            run_id = _run_of(body)
            where = f" (run {run_id})" if run_id else ""
            raise PayloadTooLargeError(
                run_id,
                f"{op}{where}: the request is {len(data)} bytes, over the server max_payload of "
                f"{max_payload}",
            )
        try:
            msg = await self._or_closed(
                self._nc.request(api_subject(self._prefix, op), data, timeout=timeout_s)
            )
        except Exception as e:  # every transport failure is mapped, by type, in _from_nats
            raise _from_nats(op, body, e) from e
        try:
            reply = json.loads(msg.data.decode("utf-8"))
        except ValueError as e:
            raise EngineUnavailableError(
                op, "internal", f"{op}: the engine's reply is not JSON"
            ) from e
        if not isinstance(reply, dict):
            raise EngineUnavailableError(
                op, "internal", f"{op}: the engine's reply is not an object"
            )
        # A refusal is an `error` member with a string `code`: a failed run's `result` and `status`
        # carry its error value `{name, message}` in `error` too, and that is a success (the
        # engine's F31, API.md "Encoding").
        refusal = reply.get("error")
        if isinstance(refusal, dict) and isinstance(refusal.get("code"), str):
            raise _from_refusal(op, body, refusal, self._log)
        return reply

    async def _or_closed(self, request: Coroutine[Any, Any, Msg]) -> Msg:
        """The ``nc.request``, failed with nats-py's ``ConnectionClosedError`` as soon as the connection
        closes for good. nats-py leaves a request in flight at its close waiting out its timeout
        (nats.js rejects it at once), so a worker's 30 s poll would hide a dead connection."""
        ask = asyncio.ensure_future(request)
        closed = asyncio.ensure_future(_closed(self._nc))
        try:
            await asyncio.wait({ask, closed}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in (closed, ask):
                if not t.done():
                    t.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await t
        if ask.done() and not ask.cancelled():
            return ask.result()
        raise nats.errors.ConnectionClosedError


async def _closed(nc: NatsClient) -> None:
    while not nc.is_closed:
        await asyncio.sleep(_CLOSED_TICK_S)


def _run_of(body: dict[str, Any]) -> str:
    run_id = body.get("runId")
    if isinstance(run_id, str):
        return run_id
    child = body.get("childRunId")
    return child if isinstance(child, str) else ""


def _int_or_none(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _from_refusal(op: Op, body: dict[str, Any], r: dict[str, Any], log: Logger) -> Exception:
    run_id = _run_of(body)
    code = r["code"]
    where = f" (run {run_id})" if run_id else ""
    message = f"engine refused {op}{where}: {r.get('message') or code}"
    if code == "fenced":
        return EngineFencedError(
            run_id, message, _int_or_none(r.get("expectLastSeq")), _int_or_none(r.get("serverCode"))
        )
    if code == "expired":
        return LeaseExpiredError(op, message)
    if code == "invalid":
        # Inside an execution a refused append or child is a value or an identifier the journal
        # cannot hold: deterministic, so the pure core fails the run (its SerializationError path)
        # rather than retry it forever. Anywhere else it is a bad request.
        if op in ("append", "child"):
            return SerializationError(run_id, message)
        return InvalidRequestError(op, message)
    if code == "too_large":
        return PayloadTooLargeError(run_id, message)
    if code == "not_found":
        return RunNotFoundError(op, run_id)
    if code == "blob_gone":
        return _blob_gone(run_id, r, message)
    if code == "internal":
        log.error(
            "durable: the engine answered internal — an operator must look",
            op=op,
            run_id=run_id,
            detail=message,
        )
        return EngineUnavailableError(op, "internal", message)
    if code in ("tenant_limit", "licence_limit"):
        # A refusal, never a transient (the engine's F46): it clears when runs end or an operator
        # acts, so nothing here retries it (F40).
        limit = r.get("limit")
        return LimitError(op, code, limit if isinstance(limit, str) else "", message)
    if code != "unavailable":
        # A code this package does not know (an engine newer than it): transient, as before, but
        # said, since it may be a refusal that will not clear.
        log.warn(
            "durable: the engine answered a code this SDK does not know; treating it as unavailable",
            op=op,
            run_id=run_id,
            code=code,
            detail=message,
        )
    return EngineUnavailableError(op, "unavailable", message)


def _blob_gone(run_id: str, r: dict[str, Any], message: str) -> Exception:
    """``blob_gone`` (the engine's F30): a blob is gone from the blob store, or holds bytes that are
    not its digest's. The library's own errors for the two, so the worker parks the run as the
    library did (loud, ``done retry``, never a terminal) and a client's ``result`` reports it. The
    engine's message says which: a digest that did not match is the integrity one, anything else is
    missing. ``seq`` is -1: the engine names the blob, not the entry (its NonCommandSeq)."""
    blob = r.get("blob")
    name = blob if isinstance(blob, str) and blob else "?"
    owner = name.split("/", 1)[0] if "/" in name else run_id
    if "digest" in str(r.get("message") or ""):
        return BlobIntegrityError(owner, -1, name, message)
    return BlobMissingError(owner, -1, name, message)


def _from_nats(op: Op, body: dict[str, Any], e: Exception) -> Exception:
    if isinstance(e, nats.errors.MaxPayloadError):
        return PayloadTooLargeError(_run_of(body), f"{op}: the request is over max_payload")
    if isinstance(e, (nats.errors.ConnectionClosedError, nats.errors.ConnectionDrainingError)):
        return ConnectionClosedError(f"{op}: the NATS connection is closed")
    if isinstance(e, nats.errors.NoRespondersError):
        return EngineUnavailableError(
            op,
            "unavailable",
            f"{op}: no engine answered (no responders) — is durable-engine serve running on this "
            "prefix?",
        )
    if isinstance(e, nats.errors.TimeoutError):
        return EngineUnavailableError(
            op, "unavailable", f"{op}: the engine did not answer in time", timed_out=True
        )
    return EngineUnavailableError(op, "unavailable", f"{op}: {e!r}")
