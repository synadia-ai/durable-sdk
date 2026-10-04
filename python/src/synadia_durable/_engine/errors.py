"""synadia-durable — the errors the engine's API adds to the library's. Mirrors
``src/engine/errors.ts``.

A refusal is ``{"error": {"code", "message"}}`` from a closed set (``durable-engine``
``docs/API.md`` "Errors"); the transport turns each code into one of these, or into a library error
where the library already has the class: ``fenced`` → :class:`FencedError`, ``too_large`` →
:class:`PayloadTooLargeError`, ``blob_gone`` → :class:`BlobMissingError` (or
:class:`BlobIntegrityError` when the bytes are not the digest's).
"""

from __future__ import annotations

from typing import Literal

from synadia_durable.errors import DurableError, FencedError

__all__ = [
    "EngineError",
    "EngineErrorCode",
    "EngineFencedError",
    "EngineUnavailableError",
    "InvalidRequestError",
    "LeaseExpiredError",
    "RunNotFoundError",
]

#: The closed set of refusal codes of API v1.
EngineErrorCode = Literal[
    "invalid", "fenced", "expired", "not_found", "too_large", "unavailable", "blob_gone", "internal"
]


class EngineError(DurableError):
    """A refusal (or a failure to reach the engine) on one op."""

    def __init__(self, op: str, code: EngineErrorCode, message: str) -> None:
        super().__init__(message)
        self.op = op
        #: The API's code, or ``unavailable`` for no responders and a request timeout.
        self.code: EngineErrorCode = code


class LeaseExpiredError(EngineError):
    """The item's lease is not the engine's any more (``expired``): the item is done, past
    ``max_work_ms``, minted by an instance that is gone, or (for ``extend``) lapsed. The worker
    stops executing that item and sends nothing more under its lease; the item redelivers to a
    poll."""

    def __init__(self, op: str, message: str) -> None:
        super().__init__(op, "expired", message)


class InvalidRequestError(EngineError):
    """A malformed request (``invalid``) outside a journal append: a bug or a bad argument. Never
    retried."""

    def __init__(self, op: str, message: str) -> None:
        super().__init__(op, "invalid", message)


class EngineUnavailableError(EngineError):
    """Transient: no engine answered (NATS no responders), the request timed out, or the engine
    said ``unavailable`` or ``internal``. Inside an execution the pure core halts with ``retry`` on
    it and the worker reports ``done retry``; a client call may simply be repeated."""

    def __init__(
        self,
        op: str,
        code: Literal["unavailable", "internal"],
        message: str,
        *,
        timed_out: bool = False,
    ) -> None:
        super().__init__(op, code, message)
        #: No answer within the request's timeout (rather than no responders or a refusal).
        self.timed_out = timed_out


class RunNotFoundError(EngineError, LookupError):
    """``signal`` or ``cancel`` of a run that was never started (``not_found``).

    Also a :class:`LookupError`, which is what the library's ``client.signal`` and
    ``client.cancel`` raised for an unknown run, so a caller's ``except LookupError`` still holds.
    """

    def __init__(self, op: str, run_id: str) -> None:
        super().__init__(op, "not_found", f'client.{op}: run "{run_id}" not found')
        self.run_id = run_id


class EngineFencedError(FencedError):
    """The library's :class:`FencedError`, as an ``append`` refusal carries it.

    ``expect_last_seq`` is what the append said, ``server_code`` what the broker answered
    underneath (10071; 10164 or 10158 on a replicated journal; ``None`` when the engine fenced a
    duplicate ack itself). Both are for logs only: a fence is a reply, and this package maps no
    broker code of its own.
    """

    def __init__(
        self,
        run_id: str,
        message: str,
        expect_last_seq: int | None = None,
        server_code: int | None = None,
    ) -> None:
        super().__init__(run_id, message)
        self.expect_last_seq = expect_last_seq
        self.server_code = server_code
