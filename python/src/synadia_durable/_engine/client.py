"""synadia-durable — the client: start runs, observe them, signal and cancel them, through the
engine's five client ops (``durable-engine`` ``docs/API.md``: start, result, status, signal,
cancel). Mirrors ``src/engine/client.ts``.

The library's ``Client`` and ``RunHandle``, unchanged in shape; what was the shell's (the journal
reads, the status hint subscription, provisioning, offload) is the engine's now.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from synadia_durable._engine.errors import EngineUnavailableError
from synadia_durable._engine.names import DEFAULT_PREFIX
from synadia_durable._engine.payload import slot, unslot
from synadia_durable._engine.transport import Transport
from synadia_durable._internal.logger import Logger, SilentLogger
from synadia_durable._internal.token import assert_token
from synadia_durable.errors import (
    CancelledError,
    ConnectionClosedError,
    DurableTimeoutError,
    WorkflowFailure,
)
from synadia_durable.types import RunStatus, StartOptions, Workflow

if TYPE_CHECKING:
    from nats.aio.client import Client as NatsClient

    from synadia_durable._internal.metrics import Metrics
    from synadia_durable.journal.blobs import OffloadOptions
    from synadia_durable.journal.payload_codec import PayloadCodec

__all__ = ["Client", "RunHandle", "create_client"]

OutT = TypeVar("OutT")

#: How long one ``result`` request asks the engine to wait (the engine caps it at its
#: ``max_poll_wait_ms``).
RESULT_WAIT_MS = 30_000

#: Back-off between ``result`` requests after a transient refusal (no engine, a timeout).
RESULT_RETRY_S = 0.5

#: How often a waiting ``result()`` looks at the connection's reconnect count.
_RECONNECT_TICK_S = 0.1


class RunHandle(Generic[OutT]):
    """A handle to a started run. Satisfies :class:`~synadia_durable.types.Handle`."""

    def __init__(self, client: Client, run_id: str) -> None:
        self._client = client
        self._run_id = run_id

    @property
    def run_id(self) -> str:
        return self._run_id

    async def status(self) -> RunStatus:
        r = await self._client._request.request("status", {"runId": self._run_id})
        state = r.get("status", "pending")
        if state == "pending" and "wf" not in r:
            return RunStatus(state="pending", workflow="", version=0, updated_ms=0)
        return RunStatus(
            state=state,
            workflow=r.get("wf", ""),
            version=r.get("version", 0),
            updated_ms=r.get("updatedMs", 0),
            next=r.get("next"),
            detail=r.get("detail"),
            due_ms=r.get("dueMs"),
        )

    async def result(self, timeout_ms: float = 30_000) -> OutT:
        """Wait for the run's terminal and return its value.

        The engine follows ``continued`` itself and answers the generation that returned: the client
        re-asks with ``waitMs`` until a terminal or the caller's deadline, nothing more.
        """
        c = self._client
        deadline = time.monotonic() + timeout_ms / 1000
        while True:
            if c._nc.is_closed:
                raise self._closed_error()
            remaining_ms = (deadline - time.monotonic()) * 1000
            wait_ms = int(max(0, min(RESULT_WAIT_MS, remaining_ms)))
            r: dict[str, Any] | None = None
            try:
                r = await c._result_request(self._run_id, wait_ms, deadline)
            except ConnectionClosedError:
                raise self._closed_error() from None
            except EngineUnavailableError as e:
                c._log.debug("durable: result() request failed; retrying", run_id=self._run_id, error=str(e))  # fmt: skip
                left = deadline - time.monotonic()
                if left > 0:
                    await asyncio.sleep(min(RESULT_RETRY_S, left))
            if r is not None and not r.get("pending") and "kind" in r:
                return cast("OutT", await self._terminal(r))
            if time.monotonic() >= deadline:
                if c._nc.is_closed:
                    raise self._closed_error()
                raise DurableTimeoutError(f"run {self._run_id}: result timed out")

    async def _terminal(self, r: dict[str, Any]) -> Any:
        codec = self._client._payload_codec
        gen = r.get("runId") or self._run_id
        kind = r["kind"]
        if kind == "cancelled":
            raise CancelledError(gen)
        if kind == "result":
            if "valueEnc" in r:
                return await unslot(gen, r["valueEnc"], codec)
            return r.get("value")
        err = (
            await unslot(gen, r["errorEnc"], codec)
            if "errorEnc" in r
            else r.get("error") or {"name": "Error", "message": "run failed"}
        )
        raise WorkflowFailure(gen, f"{err['name']}: {err['message']}")

    def _closed_error(self) -> ConnectionClosedError:
        return ConnectionClosedError(
            f"run {self._run_id}: the NATS connection closed while waiting for the result — the "
            f"run itself is unaffected; call client.get({self._run_id!r}).result() on a live "
            "connection"
        )


class Client:
    """Starts, observes, signals and cancels runs. Build one with :func:`create_client`."""

    def __init__(
        self,
        nc: NatsClient,
        *,
        prefix: str = DEFAULT_PREFIX,
        payload_codec: PayloadCodec | None = None,
        logger: Logger | None = None,
    ) -> None:
        self._nc = nc
        self._prefix = prefix
        self._payload_codec = payload_codec
        self._log: Logger = logger or SilentLogger()
        self._request = Transport(nc, prefix, self._log)

    async def _result_request(
        self, run_id: str, wait_ms: int, deadline: float
    ) -> dict[str, Any] | None:
        """One ``result`` request, settled with its reply, or with ``None`` at the caller's
        deadline or at the connection's next reconnect (the caller asks again or times out).

        A request in flight across a disconnect loses its reply: the engine answers an inbox nobody
        is subscribed to for that moment. So the wait is cut short at the next reconnect and the
        engine asked again, instead of waiting out the request's timeout.
        """
        ask = asyncio.ensure_future(
            self._request.request("result", {"runId": run_id, "waitMs": wait_ms}, wait_ms=wait_ms)
        )
        cut = asyncio.ensure_future(_next_reconnect(self._nc))
        try:
            timeout = max(0.0, deadline - time.monotonic()) + 0.05
            await asyncio.wait({ask, cut}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
            if ask.done():
                return ask.result()
            return None
        finally:
            for t in (ask, cut):
                if not t.done():
                    t.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await t

    async def start(
        self,
        workflow: Workflow[Any, OutT],
        input: Any,
        opts: StartOptions | None = None,
    ) -> RunHandle[OutT]:
        """Start a run idempotently. The runId is the idempotency key (PROTOCOL §11.1); without
        one the engine mints it."""
        run_id = opts.run_id if opts is not None and opts.run_id else None
        # Identifiers are checked here, so a bad one fails before a round trip.
        if run_id is not None:
            assert_token("runId", run_id)
        assert_token("workflow name", workflow.name)
        body: dict[str, Any] = {} if run_id is None else {"runId": run_id}
        body |= {"wf": workflow.name, "version": workflow.version}
        body |= await slot("input", input, self._payload_codec, run_id or "")
        r = await self._request.request("start", body)
        return RunHandle(self, str(r["runId"]))

    def get(self, run_id: str) -> RunHandle[Any]:
        """A handle to an existing run, without starting anything."""
        return RunHandle(self, run_id)

    async def signal(self, run_id: str, name: str, data: Any) -> None:
        """Deliver an external signal to a parked run (PROTOCOL §8). An unknown run raises
        :class:`~synadia_durable._engine.errors.RunNotFoundError`, a :class:`LookupError`."""
        assert_token("runId", run_id)
        body: dict[str, Any] = {"runId": run_id, "name": name}
        body |= await slot("data", data, self._payload_codec, run_id)
        await self._request.request("signal", body)

    async def cancel(self, run_id: str, reason: str | None = None) -> None:
        """Request cancellation of a run (idempotent; a no-op on an already-terminal run).

        Cancellation is an uncatchable hard-stop at the run's next COMMAND BOUNDARY: recorded
        commands still replay, but the next fresh ``ctx.*`` op — or the resume of a step that began
        and never finished — halts before executing. Pure compute BETWEEN commands is not preempted:
        a run that returns without crossing a stop point completes instead (PROTOCOL §11.7).
        """
        assert_token("runId", run_id)
        body: dict[str, Any] = {"runId": run_id}
        if reason is not None:
            body["reason"] = reason
        await self._request.request("cancel", body)


async def _next_reconnect(nc: NatsClient) -> None:
    """Settle at the connection's next reconnect, or when it closes for good.

    nats-py has no status iterator to wait on and its reconnect callback is fixed at connect time,
    so this watches the client's own reconnect count (``nc.stats["reconnects"]``).
    """
    base = nc.stats["reconnects"]
    while nc.stats["reconnects"] == base and not nc.is_closed:
        await asyncio.sleep(_RECONNECT_TICK_S)


def create_client(
    nc: NatsClient,
    *,
    prefix: str = DEFAULT_PREFIX,
    stream_max_bytes: int | None = None,
    offload: bool | OffloadOptions = True,
    payload_codec: PayloadCodec | None = None,
    metrics: Metrics | None = None,
    logger: Logger | None = None,
) -> Client:
    """Build a client on a caller-owned NATS connection (bring your own connection).

    Requests go to ``{prefix}.api.v1.<op>``, answered by ``durable-engine serve`` on that prefix.
    ``payload_codec`` is encryption at rest, as the library's: every worker and client on a prefix
    must hold the SAME codec. Under one, inputs and signal data travel as ``inputEnc`` /
    ``dataEnc``, results come back as ``valueEnc`` / ``errorEnc`` and are decoded here, and
    ``status()`` carries no payload (the engine cannot read one).

    ``stream_max_bytes`` and ``offload`` are the library's and the engine's now
    (``durable-engine serve --stream-max-bytes``; ``info``'s ``offload_threshold``): accepted and
    ignored. ``metrics`` is accepted for the library's API; the client emits no events of its own
    (the engine counts).
    """
    _ = (stream_max_bytes, offload, metrics)
    return Client(nc, prefix=prefix, payload_codec=payload_codec, logger=logger)
