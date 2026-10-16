"""synadia-durable — public types. Pure; no runtime imports beyond the standard library.

Mirrors ``src/types.ts``. The ``Ctx`` surface lands with the replay engine (P1); what is here is what
the client needs: a workflow token, a handle, and the run status.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Generator, Sequence
from dataclasses import dataclass
from typing import Any, Generic, Literal, NoReturn, Protocol, TypeVar

__all__ = [
    "Ctx",
    "DurableFuture",
    "Handle",
    "RetryPolicy",
    "RunState",
    "RunStatus",
    "StartOptions",
    "Workflow",
    "WorkflowFn",
]

InT = TypeVar("InT")
OutT = TypeVar("OutT")
OutT_co = TypeVar("OutT_co", covariant=True)

#: A workflow is an ordinary async function over a deterministic context (:class:`Ctx`, below).
#: Typed as ``Any`` in the context position so that a workflow may annotate its parameter as ``Ctx``
#: without this alias depending on a protocol declared further down the module.
WorkflowFn = Callable[[Any, InT], "Awaitable[OutT]"]

RunState = Literal[
    "pending",
    "running",
    "suspended",
    "blocked",
    "completed",
    "failed",
    "continued",
    "cancelled",
]


@dataclass(frozen=True, slots=True)
class Workflow(Generic[InT, OutT]):
    """A registered workflow definition — used for registration AND for typed start/call.

    Produced by the ``@workflow`` decorator, which returns this token rather than the function, so
    that ``ctx.call(charge, input)`` stays typed and a stray direct call fails loudly rather than
    running a workflow body outside the engine.
    """

    name: str
    version: int
    fn: WorkflowFn[InT, OutT]

    def __call__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            f'workflow "{self.name}" v{self.version} cannot be called directly — it must run inside '
            f"a worker, so that every ctx.* operation is journaled. Use client.start(...) to run it, "
            f"or ctx.call(...) from another workflow."
        )


@dataclass(frozen=True, slots=True)
class RunStatus:
    """A point-in-time view of a run, read from the ``DE_RUNS`` KV.

    A **hint**, never the truth (PROTOCOL §10): callers needing certainty await
    :meth:`Handle.result`, which resolves against the journal.
    """

    state: RunState
    workflow: str
    version: int
    updated_ms: int
    #: For state ``continued``: the next generation's runId (the continue-as-new chain).
    next: str | None = None
    #: For state ``blocked``: a human-readable reason (e.g. "waiting for charge v2").
    detail: str | None = None
    #: For state ``suspended``: when the earliest pending wake is due, if parked on a timer at all.
    due_ms: int | None = None


@dataclass(frozen=True, slots=True)
class StartOptions:
    """Options for :meth:`Client.start`."""

    #: The runId is the idempotency key for start. Default: a generated uuid.
    run_id: str | None = None


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """In-process retry of a step function, before its failure is journaled.

    **Not a durability mechanism.** These attempts are not journaled, so a crash resets the count —
    the durable retry is the work item's own redelivery. This is for smoothing flaky I/O inside one
    execution, and it exists mostly so that a transient 503 does not become a terminal run failure.

    Fixed delay by default; set ``factor`` for exponential backoff.
    """

    attempts: int
    delay_ms: float = 0.0
    #: When set, the delay multiplies by this each attempt (2.0 doubles).
    factor: float | None = None
    #: Upper bound on an exponential delay.
    max_ms: float | None = None

    def delay_for(self, attempt: int) -> float:
        """Delay in ms before retry number ``attempt`` (1-based)."""
        if self.factor is None:
            return self.delay_ms
        delay = self.delay_ms * (self.factor ** max(0, attempt - 1))
        return min(delay, self.max_ms) if self.max_ms is not None else delay


class DurableFuture(Protocol[OutT_co]):
    """A durable op that has been STARTED but not yet awaited — the fan-out primitive.

    Returned by :meth:`Ctx.start_call`, :meth:`Ctx.start_sleep` and :meth:`Ctx.start_wait`. It is an
    ordinary awaitable, so ``await f`` works and so do the standard combinators::

        results = await asyncio.gather(*(ctx.start_call(ship, p) for p in parcels))
        settled = await asyncio.gather(*futures, return_exceptions=True)   # Promise.allSettled

    For racing, use :meth:`Ctx.race` — ``asyncio.wait(..., FIRST_COMPLETED)`` and friends are unsafe
    over durable futures, because a branch that is still pending suspends the whole run before a
    ready branch can win.

    Always CONSUME a future — await it, gather it, or race it. Discarding one is unsupported: it does
    not block the run, its child or timer is armed only best-effort, and its result is ignored.
    (``ctx.call`` / ``sleep`` / ``wait`` are "start, then await", so the blocking forms are always
    safe.)
    """

    def __await__(self) -> Generator[Any, Any, OutT_co]: ...


class Ctx(Protocol):
    """The deterministic execution context.

    Every nondeterministic or side-effecting operation goes through a ``ctx`` call and is journaled.
    Two members are renamed from the reference implementation because their names are Python
    keywords: ``ctx.await`` is :meth:`wait`, and ``ctx.assert`` is :meth:`check`.
    """

    run_id: str
    workflow: str
    version: int
    #: The delivery count of the current frontier (1 on the first delivery). It counts every
    #: delivery, including an item handed back between engine instances, so it is not a count of
    #: failures. Not journaled: a crash resets it. Inherently nondeterministic, so a replay sees a
    #: different value than the execution it replays: fine for logging inside a step, a
    #: replay-divergence footgun if workflow control flow branches on it.
    attempt: int

    async def run(
        self,
        name: str,
        fn: Callable[..., Any],
        *args: Any,
        retry: RetryPolicy | None = None,
        **kwargs: Any,
    ) -> Any:
        """Journal a durable step (two-phase: intent first, then result). The core primitive."""
        ...

    async def now(self) -> int: ...

    async def random(self) -> float: ...

    async def uuid(self) -> str: ...

    async def sleep(self, name: str, ms: int) -> None:
        """Durable timer — suspend until ``ms`` have elapsed."""
        ...

    async def wait(self, name: str, timeout_ms: int | None = None) -> Any:
        """Block until a signal of this name arrives (the wire kind is ``await``)."""
        ...

    async def call(
        self, workflow: Workflow[Any, Any], input: Any, *, run_id: str | None = None
    ) -> Any:
        """Run a sub-workflow and resume with its result."""
        ...

    def start_call(
        self, workflow: Workflow[Any, Any], input: Any, *, run_id: str | None = None
    ) -> DurableFuture[Any]:
        """Start a sub-workflow WITHOUT blocking, returning a durable future.

        Gather several to fan out children concurrently. ``ctx.call`` is ``await ctx.start_call``.
        Not ``async``: the command's position is assigned here, synchronously, which is what keeps a
        fan-out's journal deterministic (see ``docs/DESIGN-python-sdk.md`` §3.2).
        """
        ...

    def start_sleep(self, name: str, ms: int) -> DurableFuture[None]:
        """Start a durable timer without blocking; await the future, or race it for a timeout."""
        ...

    def start_wait(self, name: str, timeout_ms: int | None = None) -> DurableFuture[Any]:
        """Start waiting for a signal without blocking; await the future, or race it against others."""
        ...

    async def race(self, futures: Sequence[DurableFuture[Any]]) -> Any:
        """Resolve with the first (journal-earliest) branch to settle, raising if that one failed.

        The safe alternative to racing durable futures with ``asyncio``: losing branches keep
        running, and their results are journaled and ignored.
        """
        ...

    async def continue_as_new(self, input: Any, *, version: int | None = None) -> NoReturn:
        """End this generation and atomically start a fresh one with a bounded journal."""
        ...

    def key(self) -> str:
        """This step's idempotency key, ``{runId}:{stepSeq}`` — for keying user side effects."""
        ...

    def get(self, key: str) -> Any:
        """Resolve an injected dependency."""
        ...

    def check(self, condition: bool, message: str | None = None) -> None:
        """Invariant guard — fail the run loudly if false (``assert`` is a Python keyword)."""
        ...

    def on_cancel(self, fn: Callable[[], Any]) -> None:
        """Register a compensation handler, run LIFO if this run is cancelled."""
        ...


class Handle(Protocol[OutT_co]):
    """A handle to a started run."""

    @property
    def run_id(self) -> str: ...

    async def result(self, timeout_ms: float = 30_000) -> OutT_co:
        """Resolve when the run reaches a terminal state.

        Raises :class:`~synadia_durable.errors.WorkflowFailure` if it failed,
        :class:`~synadia_durable.errors.CancelledError` if it was cancelled, and
        :class:`~synadia_durable.errors.DurableTimeoutError` if the wait elapses first. Follows a
        continue-as-new chain transparently to whichever generation finally returns.
        """
        ...

    async def status(self) -> RunStatus:
        """Read the run's status hint. See :class:`RunStatus`."""
        ...
