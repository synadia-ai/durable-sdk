"""synadia-durable — error hierarchy.

All library errors extend :class:`DurableError`. The taxonomy is the protocol's (PROTOCOL §12), not
this SDK's, and the dividing line it encodes is the important part:

* a **logic** error (a workflow throw, replay non-determinism, an unencodable value) **fails the run**
  terminally — retrying it can never succeed;
* an **infrastructure** error (a transient publish/scheduler failure that is not a fence) is
  **retried** — the work item is NAK'd and replay resumes safely;
* a **blob** error is **operational** — the run is parked (loud, redelivered), never failed, because
  restoring the object or cancelling the run are both live options.

Mirrors ``src/errors.ts``.
"""

from __future__ import annotations

from typing import Literal

__all__ = [
    "BlobIntegrityError",
    "BlobMissingError",
    "CancelledError",
    "ConnectionClosedError",
    "DurableError",
    "DurableTimeoutError",
    "FencedError",
    "NonDeterminismError",
    "NotImplementedFeatureError",
    "PayloadCodecError",
    "PayloadTooLargeError",
    "SerializationError",
    "StepNestingError",
    "WorkflowFailure",
]


class DurableError(Exception):
    """Base class for every error this library raises."""


class FencedError(DurableError):
    """A journal append or KV CAS was rejected because the subject advanced underneath us.

    NATS ``JetStreamApiError`` code 10071 ("wrong last sequence"). It means another worker owns this
    run — the loser is a zombie and must stop. Surfaced out of band to the worker; never raised
    through user workflow code, so a user ``try``/``except`` cannot swallow it.
    """

    def __init__(self, run_id: str, message: str | None = None) -> None:
        super().__init__(
            message
            or f"run {run_id}: fenced — lost single-writer CAS (another worker owns this run)"
        )
        self.run_id = run_id


class NonDeterminismError(DurableError):
    """Replay diverged from the recorded journal (a different command name or kind at a position, or
    a replay that consumed fewer commands than were recorded).

    Indicates nondeterministic workflow code. We fail loud rather than corrupt the run.
    """

    def __init__(self, run_id: str, seq: int, detail: str) -> None:
        super().__init__(f"run {run_id}: non-determinism at seq {seq}: {detail}")
        self.run_id = run_id
        self.seq = seq


class WorkflowFailure(DurableError):
    """A workflow function raised; the run is terminally failed and the error is journaled."""

    def __init__(self, run_id: str, message: str) -> None:
        super().__init__(message)
        self.run_id = run_id


class StepNestingError(DurableError):
    """A ``ctx`` command was invoked INSIDE a ``ctx.run`` step function.

    Nesting interleaves command sequence numbers, so the run would complete once and then fail every
    replay — we fail loud at the first call instead. (``ctx.key``, ``ctx.get`` and ``ctx.check`` are
    not commands and ARE allowed inside a step.)
    """

    def __init__(self, run_id: str, message: str) -> None:
        super().__init__(message)
        self.run_id = run_id


class SerializationError(DurableError):
    """A journal value could not be encoded by the codec.

    A step result, sub-workflow input or continue-as-new input that is not serializable (a non-finite
    float, a ``set``, a ``datetime``, a non-string dict key, …). This is a DETERMINISTIC programming
    error: re-encoding the same value always fails, so the run is failed terminally rather than
    retried as transient infrastructure (which would loop forever).
    """

    def __init__(self, run_id: str, message: str) -> None:
        super().__init__(message)
        self.run_id = run_id


class PayloadTooLargeError(SerializationError):
    """An encoded journal entry exceeds the server's ``max_payload``.

    Deterministic for a given value — republishing the same bytes can never succeed — so it extends
    :class:`SerializationError` and terminally fails the run instead of being classified as transient
    infrastructure and NAK-looping forever (which would also head-of-line-block the workflow's work
    consumer). With offload enabled this is reachable only for oversized entry METADATA.
    """


class BlobMissingError(DurableError):
    """A journaled blob ref points at an object that is gone, or this reader has no blob store.

    OPERATIONAL, not a workflow failure, and potentially recoverable (restore the bucket, re-enable
    offload) — so the worker PARKS the run (NAK-with-backoff, loud) instead of writing a terminal.
    """

    def __init__(
        self,
        run_id: str,
        seq: int,
        blob_name: str,
        detail: str,
        reason: Literal["absent", "no-store"] = "absent",
    ) -> None:
        super().__init__(
            f'run {run_id}: blob "{blob_name}" (entry seq {seq}) cannot be resolved — {detail}'
        )
        self.run_id = run_id
        self.seq = seq
        self.blob_name = blob_name
        #: ``absent``: the store is reachable but the object is definitively gone (restorable —
        #: parkers keep waiting). ``no-store``: this READER has no blob store at all (offload
        #: disabled or unprovisioned) — a configuration error that waiting never heals.
        self.reason = reason


class BlobIntegrityError(DurableError):
    """A fetched blob failed digest verification, or a ref is malformed.

    Content-addressed names make this impossible under normal operation — it indicates out-of-band
    modification of the blob store. Fail loud; the run is parked (same recovery posture as
    :class:`BlobMissingError`).
    """

    def __init__(self, run_id: str, seq: int, blob_name: str, detail: str) -> None:
        super().__init__(
            f'run {run_id}: blob "{blob_name}" (entry seq {seq}) failed verification — {detail}'
        )
        self.run_id = run_id
        self.seq = seq
        self.blob_name = blob_name


class NotImplementedFeatureError(DurableError):
    """Reserved: a not-yet-wired feature was reached.

    Named with a ``Feature`` suffix because ``NotImplementedError`` is a Python builtin; shadowing it
    would make ``except NotImplementedError`` mean different things depending on import order.
    """

    def __init__(self, feature: str) -> None:
        super().__init__(f"{feature} is not implemented yet")


class DurableTimeoutError(DurableError):
    """A bounded wait elapsed before the run reached a terminal state.

    For ``ctx.wait(..., timeout_ms=…)`` this is a catchable control-flow signal inside the workflow;
    for ``handle.result(timeout_ms)`` it is a client-side timeout.
    """


class ConnectionClosedError(DurableError):
    """The NATS connection this client was given has CLOSED for good, so a wait on it can never be
    answered (closed by the application, or the client library gave up reconnecting —
    ``max_reconnect_attempts``).

    Raised by ``handle.result()`` at once rather than after its timeout: the RUN is unaffected — the
    workers own it — and ``client.get(run_id).result()`` on a live connection picks the wait up
    again. A connection that is merely RECONNECTING does not raise this; the wait rides it out.
    (Not ``nats.errors.ConnectionClosedError``: that one is the transport's, and a caller should not
    have to import the transport to tell a dead wait from a slow run.)
    """


class CancelledError(DurableError):
    """A run was cancelled via ``client.cancel``.

    Surfaced by :meth:`Handle.result` of a cancelled run, and as a cancelled child's failure to its
    parent's ``ctx.call``. NOT raised through the main workflow flow — a cancel is an out-of-band,
    uncatchable hard-stop at the next command boundary.

    Deliberately does NOT extend :class:`asyncio.CancelledError`: that one is a ``BaseException`` that
    asyncio itself throws into coroutines, and conflating the two would let a workflow's task
    cancellation be mistaken for a durable cancel (and vice versa).
    """

    def __init__(self, run_id: str, message: str | None = None) -> None:
        super().__init__(message or f"run {run_id}: cancelled")
        self.run_id = run_id


class PayloadCodecError(DurableError):
    """A payload slot could not be put through (or taken back out of) the configured PayloadCodec.

    Causes: the journal carries transformed payloads (``v:3``) and this reader has no codec, the
    codec itself raised (wrong key, corrupt ciphertext), or the encoded form is malformed.

    Treated exactly like :class:`BlobMissingError`: the worker PARKS the run with NAK-backoff rather
    than writing a terminal, because every cause is operational and heals the moment the right codec
    is configured. Failing the run instead would destroy recoverable work over a config mistake.
    """

    def __init__(self, detail: str, run_id: str | None = None, seq: int | None = None) -> None:
        where = (
            ""
            if run_id is None
            else f"run {run_id}" + ("" if seq is None else f" (entry seq {seq})") + ": "
        )
        super().__init__(f"{where}{detail}")
        self.run_id = run_id
        self.seq = seq
