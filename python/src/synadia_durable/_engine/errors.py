"""synadia-durable — the errors the engine's API adds to the library's. Mirrors
``src/engine/errors.ts``.

A refusal is ``{"error": {"code", "message"}}`` from a closed set (the engine's
``API.md`` "Errors"); the transport turns each code into one of these, or into a library error
where the library already has the class: ``fenced`` → :class:`FencedError`, ``too_large`` →
:class:`PayloadTooLargeError`, ``blob_gone`` → :class:`BlobMissingError` (or
:class:`BlobIntegrityError` when the bytes are not the digest's). ``tenant_limit`` and
``licence_limit`` are refusals of their own (:class:`LimitError`), never retried. One is this
SDK's alone, no engine's code: :class:`UnsupportedEngineError`, an engine older than the floor
(:data:`ENGINE_FLOOR`), read from ``info.capabilities``.
"""

from __future__ import annotations

from typing import Any, Literal

from synadia_durable.errors import DurableError, FencedError

__all__ = [
    "ENGINE_ERROR_CODES",
    "ENGINE_FLOOR_CAPABILITIES",
    "ENGINE_FLOOR_SINCE",
    "EngineError",
    "EngineErrorCode",
    "EngineFencedError",
    "EngineUnavailableError",
    "InvalidRequestError",
    "LeaseExpiredError",
    "LimitError",
    "RunNotFoundError",
    "UnsupportedEngineError",
    "check_engine_floor",
]

#: The closed set of refusal codes of API v1.
EngineErrorCode = Literal[
    "invalid",
    "fenced",
    "expired",
    "not_found",
    "too_large",
    "unavailable",
    "blob_gone",
    "internal",
    "tenant_limit",
    "licence_limit",
]

#: The codes table of the engine's ``API.md`` "Errors", mirrored: each code and the class
#: a refusal with it becomes (``invalid`` on an ``append`` or a ``child`` is a
#: :class:`SerializationError` instead). ``tests/test_engine_errors.py`` checks it against the table
#: at ``engine.pin``, so a code the engine adds fails a test here rather than reaching a caller as
#: ``unavailable``.
ENGINE_ERROR_CODES: dict[str, str] = {
    "invalid": "InvalidRequestError",
    "fenced": "FencedError",
    "expired": "LeaseExpiredError",
    "not_found": "RunNotFoundError",
    "too_large": "PayloadTooLargeError",
    "unavailable": "EngineUnavailableError",
    "blob_gone": "BlobMissingError",
    "internal": "EngineUnavailableError",
    "tenant_limit": "LimitError",
    "licence_limit": "LimitError",
}


class EngineError(DurableError):
    """A refusal (or a failure to reach the engine) on one op."""

    def __init__(
        self, op: str, code: EngineErrorCode | Literal["unsupported"], message: str
    ) -> None:
        super().__init__(message)
        self.op = op
        #: The API's code, or ``unavailable`` for no responders and a request timeout;
        #: ``unsupported`` (this SDK's, never the engine's) for an engine older than the floor.
        self.code: EngineErrorCode | Literal["unsupported"] = code


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


class LimitError(EngineError):
    """A refusal at a limit: ``tenant_limit`` (the fabric mode: the caller's tenant is at one of its
    limits) or ``licence_limit`` (standalone: the engine's licence bounds what was asked).
    ``limit`` names which one, as the engine says it: ``workflows``, ``workers``, ``runs``,
    ``runsPerDay``, ``tenants``, ``expired`` (the licence's), ``bytes``, ``runs``, ``workflows``,
    ``polls``, ``rate`` (the tenant's).

    Not a fault of the request and not transient: it clears when runs end, an operator raises the
    limit or installs a renewed licence (``rate`` within a second). So no call of this package
    retries it: a client's call raises it, a worker's refused poll is logged at error naming the
    limit and backs off, ``worker.start()`` raises it when its first poll is refused, and inside an
    execution the item goes back (``done retry``), logged at error.
    """

    def __init__(
        self, op: str, code: Literal["tenant_limit", "licence_limit"], limit: str, message: str
    ) -> None:
        super().__init__(op, code, message)
        #: Which limit, as the engine names it (``""`` if a refusal named none).
        self.limit = limit


#: What this SDK needs of the engine, read from ``info.capabilities`` (the engine's
#: ``API.md`` ``info``, its F57), never from ``info.engine``: the repo has no tag, so every
#: build says ``0.2.0-dev``. ``withdrawn-remembered`` (its F53): a worker withdrawn by
#: ``cancelPoll`` is remembered 5 s on every instance, so ``stop()`` sends one ``cancelPoll``.
ENGINE_FLOOR_CAPABILITIES: tuple[str, ...] = ("withdrawn-remembered",)
#: The engine that first announces them.
ENGINE_FLOOR_SINCE = "commit 16cb3da"


class UnsupportedEngineError(EngineError):
    """The engine that answered ``info`` is older than this SDK's floor
    (:data:`ENGINE_FLOOR_CAPABILITIES`): its ``capabilities`` are absent or lack one this SDK
    needs. ``worker.start()`` raises it; a worker whose start found no engine checks at its first
    answered poll, and stops, logged at error. ``engine`` and ``commit`` are as ``info`` gave them
    (``""`` when absent). Not retried: the engine must be upgraded."""

    def __init__(self, prefix: str, info: dict[str, Any]) -> None:
        engine = info.get("engine")
        commit = info.get("commit")
        raw = info.get("capabilities")
        caps = [c for c in raw if isinstance(c, str)] if isinstance(raw, list) else None
        missing = [c for c in ENGINE_FLOOR_CAPABILITIES if c not in (caps or [])]
        #: ``info.engine``, the build's version (``0.2.0-dev`` for every build so far).
        self.engine = engine if isinstance(engine, str) else ""
        #: ``info.commit``, the build's commit.
        self.commit = commit if isinstance(commit, str) else ""
        #: ``info.capabilities``, or ``None`` when the reply had none.
        self.capabilities = caps
        #: The capabilities this SDK needs that the engine lacks.
        self.missing = missing
        lacks = "announces no capabilities" if caps is None else f"lacks {', '.join(missing)}"
        super().__init__(
            "info",
            "unsupported",
            f'the engine on prefix "{prefix}" (engine {self.engine or "?"}, commit '
            f"{self.commit or '?'}) {lacks}: this SDK needs the engine at or after "
            f"{ENGINE_FLOOR_SINCE}, which announces {', '.join(ENGINE_FLOOR_CAPABILITIES)} in "
            "info.capabilities",
        )


def check_engine_floor(prefix: str, info: dict[str, Any]) -> UnsupportedEngineError | None:
    """The engine's ``info`` reply meets the floor (``None``), or the error that says why not."""
    caps = info.get("capabilities")
    if isinstance(caps, list) and all(c in caps for c in ENGINE_FLOOR_CAPABILITIES):
        return None
    return UnsupportedEngineError(prefix, info)


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
