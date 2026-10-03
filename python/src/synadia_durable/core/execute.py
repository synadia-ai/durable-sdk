"""synadia-durable — the replay executor.

Drives a workflow function to its next halt point (completion, failure, suspension, fencing,
cancellation or continue-as-new) against a pre-loaded journal. Pure: depends only on the seams, so
the whole crash / replay / concurrency story is unit-testable without a server.

Mirrors ``src/core/execute.ts``.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast

from synadia_durable._internal.invariant import invariant_not_none
from synadia_durable.core.children import Children, UnavailableChildren
from synadia_durable.core.ctx import ContinueSpec, CtxImpl, Halt
from synadia_durable.core.scheduler import Scheduler, UnavailableScheduler
from synadia_durable.journal.entry import TERMINAL_KINDS

if TYPE_CHECKING:
    from collections.abc import Callable

    from synadia_durable.journal.entry import ErrorShape, JournalEntry, LoadedEntry
    from synadia_durable.journal.store import Journal
    from synadia_durable.types import Workflow

__all__ = ["ExecOutcome", "execute_run", "to_error_shape"]

#: How long teardown waits for a cancelled workflow task to unwind before giving up on it. Only a
#: workflow that catches the cancellation and then blocks can reach this.
TEARDOWN_TIMEOUT_S = 5.0


@dataclass(frozen=True, slots=True)
class ExecOutcome:
    """What one drive of a run produced. The worker maps this onto an ack, a NAK or a term."""

    kind: Literal["completed", "failed", "suspended", "fenced", "retry", "continue", "cancelled"]
    value: Any = None
    error: ErrorShape | None = None
    #: For ``retry``: the infrastructure error that caused it (for logging, never for control flow).
    cause: BaseException | None = None
    #: For ``continue``: the next generation to start.
    next: ContinueSpec | None = None


async def execute_run(
    run_id: str,
    *,
    workflow: Workflow[Any, Any],
    journal: Journal,
    loaded: list[LoadedEntry],
    dependencies: dict[str, Any] | None = None,
    clock: Callable[[], int] | None = None,
    attempt: int = 1,
    scheduler: Scheduler | None = None,
    children: Children | None = None,
) -> ExecOutcome:
    """Run ``workflow`` against its journal until it completes or halts."""
    start = next((le for le in loaded if le.entry["kind"] == "start"), None)
    start_entry = cast(
        "dict[str, Any]",
        invariant_not_none(start, f"run {run_id}: cannot execute — missing start entry").entry,
    )

    # A journal that already records this run's outcome is NEVER executed: report what it says. The
    # worker checks for a terminal when it first loads, but the journal it hands over here is a
    # LATER load — after recording an inbound event, after taking the lease — and another worker
    # may have finished the run in between. Replaying it anyway is not harmless: the terminal append
    # would pass its CAS (the terminal is at or before our tail), leaving only the shared terminal
    # msgID between the run and a SECOND terminal — for as long as the duplicate window lasts — and
    # a "duplicate" ack reads as "mine landed", so this execution would report its OWN outcome over
    # the journal's. With a `cancel-requested` recorded after a `continued`, that is a run reported
    # (and cascaded, and delivered to its parent) as cancelled while its next generation runs on,
    # with its on_cancel compensation fired for good measure.
    recorded = next((le for le in loaded if le.entry["kind"] in TERMINAL_KINDS), None)
    if recorded is not None:
        return _outcome_of_terminal(recorded.entry, workflow.version)

    ctx = CtxImpl(
        run_id=run_id,
        workflow=workflow,
        journal=journal,
        scheduler=scheduler if scheduler is not None else UnavailableScheduler(),
        children=children if children is not None else UnavailableChildren(),
        loaded=loaded,
        dependencies=dependencies,
        clock=clock,
        attempt=attempt,
        generation=start_entry.get("generation", 0),
        base=start_entry.get("base", run_id),
    )

    run_task: asyncio.Task[Any] = asyncio.ensure_future(workflow.fn(ctx, start_entry.get("input")))
    halt_future = ctx.halt_future()

    try:
        await asyncio.wait({run_task, halt_future}, return_when=asyncio.FIRST_COMPLETED)
        # Decide the race BEFORE draining: the drain yields to the event loop, and a branch that
        # completes during it must not be able to overturn a halt that has already fired.
        returned = run_task.done()
        # Drain what is already in motion BEFORE reporting any outcome — the worker acks the moment
        # this function returns, and an acked item is never redelivered. See ctx.flush_appends.
        await ctx.flush_appends()
        outcome = await _resolve(ctx, run_task, halt_future, returned=returned)
    finally:
        # Only NOW may the workflow task be disposed of, and the ORDER here is the whole guarantee.
        #
        # `close()` first: from this point every ctx command raises, so nothing user code does while
        # unwinding can journal into a run whose outcome is already committed.
        #
        # Then cancel. Python cannot offer the reference implementation's "no user code runs at all"
        # — an abandoned coroutine is closed by the interpreter at GC, which throws GeneratorExit and
        # runs `finally` blocks anyway, just nondeterministically and with a noisy warning. So we
        # unwind deterministically instead, AFTER the outcome exists. A workflow may observe the
        # cancellation and may run cleanup; it cannot swallow the halt, journal anything, or change
        # what this function returns.
        ctx.close()
        if not run_task.done():
            run_task.cancel()
            # Bounded: a workflow that catches the cancellation and blocks must not hold the worker.
            done, _pending = await asyncio.wait({run_task}, timeout=TEARDOWN_TIMEOUT_S)
            if run_task in done:
                with contextlib.suppress(BaseException):
                    run_task.result()  # retrieve, so asyncio does not log it as never-retrieved
    return outcome


async def _resolve(
    ctx: CtxImpl,
    run_task: asyncio.Task[Any],
    halt_future: asyncio.Future[Halt],
    *,
    returned: bool,
) -> ExecOutcome:
    # The workflow returning or raising beats a halt: if it got all the way to `return`, it did so
    # without needing anything new, and completion wins (PROTOCOL §11.7).
    if returned:
        try:
            value = run_task.result()
        except asyncio.CancelledError:  # pragma: no cover — the task is only cancelled after this
            raise
        except Exception as e:  # noqa: BLE001 — a workflow throw is a terminal failure, by design
            return await _fail_terminal(ctx, to_error_shape(e))
        # Fail loud if the workflow consumed FEWER commands than the journal recorded: a shorter
        # path is nondeterministic even though nothing mismatched along the way.
        try:
            ctx.assert_fully_consumed()
        except Exception as e:  # noqa: BLE001 — the guard's own failure is the run's failure
            return await _fail_terminal(ctx, to_error_shape(e))
        status = await ctx.append_terminal_result(value)
        if status == "retry":
            return ExecOutcome("retry", cause=ctx.last_error())
        if status == "fail":  # the return value itself is unencodable
            return await _fail_terminal(ctx, ctx.fatal_error())
        if status == "fenced":
            return ExecOutcome("fenced")
        if status == "adopted":
            return await _adopt_canonical(ctx)
        return ExecOutcome("completed", value=value)

    halt = halt_future.result()

    if halt.tag == "suspend":
        return ExecOutcome("suspended")
    if halt.tag == "fenced":
        return ExecOutcome("fenced")
    if halt.tag == "retry":
        return ExecOutcome("retry", cause=cast("BaseException | None", halt.error))
    if halt.tag == "fail":
        # An in-band serialization failure: the value is deterministically unencodable, so the run
        # fails terminally rather than retrying forever.
        return await _fail_terminal(ctx, cast("ErrorShape", halt.error))
    if halt.tag == "cancelled":
        # Opt-in compensation, before the terminal. A cleanup step that lost its CAS or hit an
        # infrastructure error ends this execution like any other step would: no terminal, the item
        # redelivers, and the replay picks the cleanup up from the journal.
        aborted = await ctx.run_cleanup()
        if aborted is not None:
            await ctx.flush_appends()
            if aborted.tag == "retry":
                return ExecOutcome("retry", cause=cast("BaseException | None", aborted.error))
            return ExecOutcome("fenced")
        return await _cancel_terminal(ctx)

    # halt.tag == "continue"
    try:
        ctx.assert_fully_consumed()
    except Exception as e:  # noqa: BLE001
        return await _fail_terminal(ctx, to_error_shape(e))
    status = await ctx.append_terminal_continue()
    if status == "retry":
        return ExecOutcome("retry", cause=ctx.last_error())
    if status == "fail":  # the continue-as-new input is unencodable
        return await _fail_terminal(ctx, ctx.fatal_error())
    if status == "fenced":
        return ExecOutcome("fenced")
    if status == "adopted":  # the winner's rollover, never our own
        return await _adopt_canonical(ctx)
    return ExecOutcome("continue", next=ctx.next_generation_spec())


async def _fail_terminal(ctx: CtxImpl, error: ErrorShape) -> ExecOutcome:
    """Write the terminal `fail` and map the append result — the shared failure path."""
    status = await ctx.append_terminal_fail(error)
    if status == "retry":
        return ExecOutcome("retry", cause=ctx.last_error())
    if status == "adopted":
        return await _adopt_canonical(ctx)
    if status == "fenced":
        return ExecOutcome("fenced")
    return ExecOutcome("failed", error=error)


async def _cancel_terminal(ctx: CtxImpl) -> ExecOutcome:
    status = await ctx.append_terminal_cancelled()
    if status == "retry":
        return ExecOutcome("retry", cause=ctx.last_error())
    if status == "adopted":
        return await _adopt_canonical(ctx)
    if status == "fenced":
        return ExecOutcome("fenced")
    return ExecOutcome("cancelled")


async def _adopt_canonical(ctx: CtxImpl) -> ExecOutcome:
    """A terminal CAS-loss where a canonical terminal already exists: adopt the JOURNALED outcome.

    Not this incarnation's own — the journal is the truth. Mirrors the step-end reconcile, and keeps
    the worker's status write consistent with what ``result()`` will read.
    """
    terminal: JournalEntry | None = await ctx.canonical_terminal()
    if terminal is None:
        return ExecOutcome("fenced")  # the terminal vanished — treat it as a plain fence
    return _outcome_of_terminal(terminal, ctx.version)


def _outcome_of_terminal(terminal: JournalEntry, current_version: int) -> ExecOutcome:
    """The outcome a journaled terminal entry stands for — the only outcome a finished run has."""
    entry = cast("dict[str, Any]", terminal)
    kind = entry["kind"]
    if kind == "result":
        return ExecOutcome("completed", value=entry.get("value"))
    if kind == "fail":
        return ExecOutcome("failed", error=entry.get("error"))
    if kind == "cancelled":
        return ExecOutcome("cancelled")
    if kind == "continued":
        return ExecOutcome(
            "continue",
            next=ContinueSpec(
                next_run_id=entry["nextRunId"],
                input=entry.get("input"),
                generation=entry["generation"],
                base=entry["base"],
                # The recorded next-gen version; a pre-versioning entry coalesces to this run's own.
                version=entry.get("version", current_version),
            ),
        )
    return ExecOutcome("fenced")  # not a terminal kind (unreachable)


def to_error_shape(error: BaseException) -> ErrorShape:
    """How an exception crosses the wire: a name and a message, nothing more."""
    return {"name": type(error).__name__, "message": str(error)}
