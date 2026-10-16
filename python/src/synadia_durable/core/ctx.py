"""synadia-durable — the deterministic context and two-phase replay engine.

Pure: depends only on the ``Journal`` / ``Scheduler`` / ``Children`` seams, never on ``nats``. Mirrors
``src/core/ctx.ts``; PROTOCOL §11.2 is the algorithm.

**Suspension, fencing, infra-retry and continue-as-new are surfaced OUT OF BAND** — they resolve an
internal halt future and return a coroutine that never completes, so they are never raised through
user workflow code and a user ``try``/``except`` cannot swallow them. That property is the reason this
module does not use exceptions for control flow, and it is the single hardest thing to port: the
original implementation guide's §7.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import inspect
import random as _random
import time
import uuid as _uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, NoReturn, TypeVar, cast

from synadia_durable._internal.invariant import (
    InvariantViolation,
    invariant,
    invariant_not_none,
)
from synadia_durable.errors import (
    DurableError,
    DurableTimeoutError,
    FencedError,
    NonDeterminismError,
    SerializationError,
    StepNestingError,
    WorkflowFailure,
)
from synadia_durable.journal.entry import (
    NON_COMMAND_SEQ,
    TERMINAL_KINDS,
    JournalEntry,
    LoadedEntry,
    MsgId,
    stamp,
)
from synadia_durable.journal.store import JournalAppend

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Generator, Sequence

    from synadia_durable.core.children import Children
    from synadia_durable.core.scheduler import Scheduler
    from synadia_durable.journal.entry import ErrorShape
    from synadia_durable.journal.store import Journal
    from synadia_durable.types import DurableFuture, RetryPolicy, Workflow

__all__ = ["ContinueSpec", "CtxImpl", "Halt"]

T = TypeVar("T")

#: Kinds that consume a command position. ``start`` / ``step-end`` / ``signal`` / ``resolve`` and the
#: terminals do not (PROTOCOL §5.2).
COMMAND_KINDS: frozenset[str] = frozenset(
    {"step-begin", "now", "random", "uuid", "sleep", "await", "call"}
)

#: The outcome of an optimistic append, as the engine classifies it.
AppendStatus = Literal["ok", "fenced", "retry", "fail"]

HaltTag = Literal["suspend", "fenced", "retry", "continue", "fail", "cancelled"]


@dataclass(frozen=True, slots=True)
class Halt:
    """Why the executor stopped driving the workflow. Never visible to user code."""

    tag: HaltTag
    error: Any = None


@dataclass(frozen=True, slots=True)
class ContinueSpec:
    """The next generation a continue-as-new starts.

    Deterministic from the current run's ``base`` + ``generation``, so racing workers compute an
    identical id without coordinating.
    """

    next_run_id: str
    input: Any
    generation: int
    base: str
    #: The next generation's workflow version (defaults to the current generation's).
    version: int


@dataclass(frozen=True, slots=True)
class _Resolution:
    """A durable op's settled state, plus the stream-seq of the entry that settled it.

    The stream-seq is what makes races (signal vs timeout, and later ``ctx.race``) deterministic: it
    is journaled, so every replay computes the same winner.
    """

    state: Literal["value", "error", "timeout", "pending"]
    stream_seq: int = 0
    value: Any = None
    error: ErrorShape | None = None


class _DurableFuture:
    """A durable op that has been issued but not yet resolved — the fan-out primitive.

    ``ctx.start_call`` / ``start_sleep`` / ``start_wait`` return one; ``ctx.call`` / ``sleep`` /
    ``wait`` are "issue, then await" over it. Two properties make a fan-out deterministic, and both
    are decided at ISSUE time rather than at await time:

    * the command ``seq`` is assigned when the op is issued, so ``asyncio.gather`` cannot interleave
      seq assignment with the awaits *inside* each branch; and
    * arming is EAGER — a Task, not a bare coroutine — so the command is journaled and its side
      effect (timer / child) armed even before anything awaits the future.

    A JS promise is eager for free; a Python coroutine is inert until awaited, which is why this is
    one of the port's deliberate divergences.
    """

    __slots__ = ("_ctx", "arm", "kind", "name", "seq")

    def __init__(
        self,
        ctx: CtxImpl,
        seq: int,
        kind: Literal["call", "sleep", "await"],
        name: str,
        arm: asyncio.Task[AppendStatus],
    ) -> None:
        self._ctx = ctx
        self.seq = seq
        self.kind = kind
        self.name = name
        #: Completes once the command is journaled and its side effect (timer / child) is armed.
        self.arm = arm

    def __await__(self) -> Generator[Any, Any, Any]:
        """Resolve the op from the journal, or suspend the run — ``await f`` and ``gather`` land here.

        A fresh coroutine per await, so awaiting the same future twice reads the same journaled
        value rather than consuming anything twice (resolution is by command ``seq``).
        """
        return self._ctx._await_future(self).__await__()


#: The step executing on THIS call chain, for every ctx in the process: ``(ctx, step seq)``. ONE
#: variable at module level, never one per ctx — a ContextVar made anywhere else is never collected
#: properly (every Context that saw it holds it), and the TypeScript twin of a store-per-execution
#: made a Node 22 worker slower with every run it had ever driven. A ContextVar (not a counter)
#: because it scopes per function even when sibling steps run concurrently — asyncio copies the
#: context into each new Task, so a step's scope follows its own branch and is invisible to
#: siblings. The entry names its ctx, so another ctx's step is not "inside" ours.
_STEP_SCOPE: contextvars.ContextVar[tuple[CtxImpl, int] | None] = contextvars.ContextVar(
    "de_step_scope", default=None
)


class CtxImpl:
    """The deterministic context handed to a workflow function."""

    def __init__(
        self,
        *,
        run_id: str,
        workflow: Workflow[Any, Any],
        journal: Journal,
        scheduler: Scheduler,
        children: Children,
        loaded: list[LoadedEntry],
        dependencies: dict[str, Any] | None = None,
        clock: Callable[[], int] | None = None,
        attempt: int = 1,
        generation: int = 0,
        base: str | None = None,
    ) -> None:
        self.run_id = run_id
        self.workflow = workflow.name
        self.version = workflow.version
        #: Redelivery attempt of the current frontier (1 on first delivery). Inherently
        #: NONDETERMINISTIC — it is not journaled, so a replay sees a different value than the
        #: execution it replays. Fine inside a step fn; branching workflow control flow on it is a
        #: replay-divergence footgun.
        self.attempt = attempt

        self._generation = generation
        self._base = base if base is not None else run_id
        self._continue_spec: ContinueSpec | None = None

        self._journal = journal
        self._scheduler = scheduler
        self._children = children
        self._deps = dependencies or {}
        self._clock = clock or _now_ms

        # Mutable view of the journal: loaded entries plus whatever this session appends.
        self._entries: list[LoadedEntry] = list(loaded)
        self._last_seq = self._entries[-1].stream_seq if self._entries else 0
        self._seq = 0

        cancel = next((le for le in loaded if le.entry["kind"] == "cancel-requested"), None)
        #: Stream-seq of the `cancel-requested` entry, which freezes any resolve at or after it.
        self._cancel_at: int | None = cancel.stream_seq if cancel is not None else None
        self._cancel_reason: str | None = (
            cast("dict[str, Any]", cancel.entry).get("reason") if cancel is not None else None
        )

        self._halted = False
        # Set only by a REAL fence (a lost CAS). Only a fence short-circuits further appends; a
        # suspend must not, or a concurrent branch's legitimate command-append (P2 fan-out) would be
        # turned into a spurious fence.
        self._fenced = False
        self._retry_error: BaseException | None = None
        self._fatal: ErrorShape | None = None

        loop = asyncio.get_event_loop()
        self._halt_future: asyncio.Future[Halt] = loop.create_future()
        # Awaited by every halting ctx.* call and never resolved: the workflow's continuation simply
        # never runs. This is what makes a halt uncatchable — there is nothing to catch.
        self._never: asyncio.Future[Any] = loop.create_future()
        # Serializes appends so concurrent ops chain their CAS on `_last_seq` instead of racing it
        # into a self-fence, and gives `flush_appends` a tail to drain. A no-op for a sequential
        # workflow (the chain is always settled); load-bearing the moment anything fans out.
        self._append_chain: asyncio.Task[AppendStatus] | None = None
        #: Every eagerly-armed branch, so teardown can drain (then abandon) what is still in flight.
        self._arm_tasks: list[asyncio.Task[AppendStatus]] = []

        self._cancel_handlers: list[Callable[[], Any]] = []
        self._in_cleanup = False
        #: Ends the wait on the cleanup handler now running, when one of its steps cannot go on.
        self._cleanup_abort: asyncio.Future[Halt] | None = None
        #: Handlers a halt stopped mid-step; disposed of by `close()`, never before it.
        self._abandoned_handlers: list[asyncio.Future[Any]] = []

    # ------------------------------------------------------------------ executor-facing plumbing

    def halt_future(self) -> asyncio.Future[Halt]:
        """The executor races this against the workflow's own completion."""
        return self._halt_future

    async def flush_appends(self) -> None:
        """Wait for everything already in motion to land, BEFORE any outcome is reported.

        The worker acks or NAKs the instant the executor returns, and an append still in flight past
        that point can be lost to a crash right after the ack — with nothing left to redeliver it,
        because an acked item never comes back. A parent suspended forever on a child whose start
        was never enqueued is the concrete failure, and it is a real bug the reference implementation
        hit (LEARNINGS #37).

        Two things are drained, looping until both are quiescent (an arm can enqueue an append; an
        append never arms anything):

        * the append chain — every journal write issued so far; and
        * the arm tasks — each fan-out branch's *side effect* too (the child start, the timer), not
          just its command append. Only a discarded (never-awaited) branch can still be arming at
          this point, since awaiting a future awaits its own arm; draining them is what makes "the
          command is journaled and the child is started" true at the ack rather than merely likely.
          Nothing blocks indefinitely: an arm's seam calls are bounded by the NATS client's own
          timeouts, and an arm never awaits a halt.
        """
        while True:
            pending = [t for t in self._arm_tasks if not t.done()]
            tail = self._append_chain
            tail_pending = tail is not None and not tail.done()
            if not pending and not tail_pending:
                self._reap_arms()
                return
            # asyncio.wait never raises what the awaited tasks raised — outcomes are the caller's
            # business (append failures are already classified into AppendStatus).
            await asyncio.wait([*pending, *([tail] if tail is not None and tail_pending else [])])

    def close(self) -> None:
        """Refuse any further ctx work.

        Called by the executor once an outcome is committed, so a late ``ctx.*`` from an unwinding
        handler cannot append anything. Belt-and-braces on top of the never-completing halt.

        Any branch still arming is abandoned here: its outcome can no longer matter, and a pending
        task left to the garbage collector is where ``Task was destroyed but it is pending!`` comes
        from. Abandoning is safe because arming is idempotent — a suspended run re-arms the same
        command on its next delivery (that is what ``_arm_*`` does on replay).
        """
        self._closed = True
        for task in self._arm_tasks:
            if not task.done():
                task.cancel()
        self._reap_arms()
        # An on_cancel handler that a halt stopped mid-step (run_cleanup) is parked for good.
        for handler in self._abandoned_handlers:
            if not handler.done():
                handler.cancel()
        self._abandoned_handlers.clear()

    def _reap_arms(self) -> None:
        """Retrieve every finished arm's exception, so asyncio does not log it as never-retrieved.

        Only a DISCARDED branch reaches here with an unretrieved failure — ``_await_future`` and
        ``race`` re-raise their own arm's error, and the run's outcome is decided without a branch
        nobody consumed either way. What this prevents is a stray "Task exception was never
        retrieved" arriving from the garbage collector minutes later with no context attached.
        ``asyncio.wait`` does not count as retrieving it; only touching the result does.
        """
        for task in self._arm_tasks:
            if task.done() and not task.cancelled():
                task.exception()

    _closed = False

    # ------------------------------------------------------------------ ctx.run (two-phase)

    async def run(
        self,
        name: str,
        fn: Callable[..., Any],
        *args: Any,
        retry: RetryPolicy | None = None,
        **kwargs: Any,
    ) -> Any:
        """Journal a durable step — two-phase, intent-first. The core primitive.

        ``fn`` may be ``async def`` or a plain ``def``; a plain one runs in a worker thread so it
        cannot stall the event loop (and with it every other run on this worker, and the ack-progress
        heartbeats). Extra ``*args``/``**kwargs`` are passed through to it.

        The step's result is journaled **exactly once**; ``fn`` itself may run **more than once**
        across the begin→end window (PROTOCOL §14). Key non-idempotent effects on ``ctx.key()``.
        """
        self._assert_not_in_step("ctx.run")
        seq = self._next_seq()
        begin = self._find_at(seq, "step-begin")
        end = self._find_at(seq, "step-end")

        if end is not None:
            # REPLAY (completed): return the recorded value; fn is NOT executed.
            self._assert_name(
                seq, cast("dict[str, Any]", begin).get("name") if begin else None, name
            )
            return cast("dict[str, Any]", end).get("value")

        if begin is not None:
            # RESUME (crashed after begin, before end): re-run fn — at-least-once, keyed by
            # ctx.key() — unless a cancel is journaled, because re-running would fire the side
            # effect for a run being killed.
            self._assert_name(seq, cast("dict[str, Any]", begin).get("name"), name)
            if self._cancel_pending():
                return await self._halt_cancelled()
            return await self._run_step_effect(seq, name, fn, args, kwargs, retry)

        # FRESH. A journaled cancel hard-stops BEFORE any new intent is appended: no new effects
        # fire once cancellation is requested (cleanup is exempt).
        if self._cancel_pending():
            return await self._halt_cancelled()
        # Fail loud if a DIFFERENT command kind was recorded here (e.g. the code turned a ctx.now()
        # into a ctx.run()).
        self._assert_kind(seq, "step-begin")
        # CAS the intent BEFORE the effect: a zombie loses here and never fires fn.
        status = await self._append_cas(
            stamp(
                {"kind": "step-begin", "seq": seq, "name": name, "key": f"{self.run_id}:{seq}"},
                self._clock(),
            ),
            MsgId.step_begin(self.run_id, seq),
        )
        if status == "retry":
            return await self._halt_retry()
        if status == "fenced":
            return await self._halt_fenced()
        return await self._run_step_effect(seq, name, fn, args, kwargs, retry)

    async def _run_step_effect(
        self,
        seq: int,
        name: str,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        retry: RetryPolicy | None,
    ) -> Any:
        token = _STEP_SCOPE.set((self, seq))
        try:
            value = await self._run_fn(fn, args, kwargs, retry)
        finally:
            _STEP_SCOPE.reset(token)

        status = await self._append_cas(
            stamp({"kind": "step-end", "seq": seq, "name": name, "value": value}, self._clock()),
            MsgId.step_end(self.run_id, seq),
        )
        if status == "retry":
            return await self._halt_retry()
        if status == "fail":
            return await self._halt_fail()  # step result unencodable — fail, do not retry
        if status == "fenced":
            # Reconcile: another worker may have completed this step. Adopt the canonical value if
            # it is there. The reload is INFRASTRUCTURE — a transient read failure must retry the
            # run, not raise through user code as a workflow failure.
            try:
                reloaded = await self._journal.load(self.run_id)
            except Exception as e:  # noqa: BLE001 — any read failure is transient here
                self._retry_error = e
                return await self._halt_retry()
            existing = next(
                (
                    le
                    for le in reloaded
                    if le.entry["kind"] == "step-end" and le.entry["seq"] == seq
                ),
                None,
            )
            if existing is not None:
                self._adopt(reloaded)
                return cast("dict[str, Any]", existing.entry).get("value")
            return await self._halt_fenced()
        return value

    async def _run_fn(
        self,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        retry: RetryPolicy | None,
    ) -> Any:
        """Invoke the step function, with optional in-process retry.

        The retries are NOT journaled: a crash resets the attempt count, which is why this is a
        convenience for flaky I/O rather than a durability mechanism.
        """
        attempts = 1 if retry is None else max(1, retry.attempts)
        attempt = 0
        while True:
            try:
                return await self._invoke(fn, args, kwargs)
            except Exception:
                attempt += 1
                if retry is None or attempt >= attempts:
                    raise
                await asyncio.sleep(retry.delay_for(attempt) / 1000)

    @staticmethod
    async def _invoke(fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        """Call a step function, whether it is ``async def`` or a plain ``def``.

        A plain ``def`` runs in a worker thread rather than on the event loop. Blocking clients
        (database drivers, ``requests``, most vendor SDKs) are the normal case in Python, and a
        blocking call on the loop would stall every other run on this worker AND the ack-progress
        heartbeats that stop the work item being redelivered mid-step. ``to_thread`` copies the
        current context, so ``ctx.key()`` still resolves inside it.
        """
        if inspect.iscoroutinefunction(fn):
            return await fn(*args, **kwargs)
        result = await asyncio.to_thread(fn, *args, **kwargs)
        # A sync function that returns an awaitable (a coroutine factory) still has to be awaited.
        return await result if inspect.isawaitable(result) else result

    # ------------------------------------------------------------------ deterministic builtins

    async def now(self) -> int:
        """The current wall-clock time in ms — captured once and replayed thereafter."""
        return cast("int", await self._value_step("now", self._clock))

    async def random(self) -> float:
        """A random float in [0, 1) — captured once and replayed thereafter."""
        return cast("float", await self._value_step("random", _random.random))

    async def uuid(self) -> str:
        """A fresh uuid4 string — captured once and replayed thereafter."""
        return cast("str", await self._value_step("uuid", lambda: str(_uuid.uuid4())))

    async def _value_step(
        self, kind: Literal["now", "random", "uuid"], generate: Callable[[], Any]
    ) -> Any:
        self._assert_not_in_step(f"ctx.{kind}")
        seq = self._next_seq()
        existing = self._find_at(seq, kind)
        if existing is not None:
            return cast("dict[str, Any]", existing).get("value")
        if self._cancel_pending():
            return await self._halt_cancelled()
        self._assert_kind(seq, kind)
        value = generate()
        status = await self._append_cas(
            stamp({"kind": kind, "seq": seq, "value": value}, self._clock()),
            MsgId.value(self.run_id, seq),
        )
        if status == "retry":
            return await self._halt_retry()
        if status == "fenced":
            return await self._halt_fenced()
        return value

    # ------------------------------------------------------------------ timers and signals

    async def sleep(self, name: str, ms: int) -> None:
        """Suspend the run until ``ms`` have elapsed. The deadline is journaled, not held in memory."""
        await self._await_future(self._start_sleep("ctx.sleep", name, ms))

    def start_sleep(self, name: str, ms: int) -> DurableFuture[None]:
        """Start a durable timer without blocking — await the future, or race it for a timeout."""
        return self._start_sleep("ctx.start_sleep", name, ms)

    def _start_sleep(self, op: str, name: str, ms: int) -> _DurableFuture:
        self._assert_not_in_step(op)
        seq = self._next_seq()  # assigned HERE, synchronously — see _DurableFuture
        return self._issue(seq, "sleep", name, self._arm_sleep(seq, name, ms))

    async def _arm_sleep(self, seq: int, name: str, ms: int) -> AppendStatus:
        """Journal the sleep (fresh, computing the deadline) or validate it (replay), then arm."""
        cmd = self._find_at(seq, "sleep")
        if cmd is None:
            if self._cancel_pending():
                # Hard-stop before journaling intent or arming a timer. Report "ok" (nothing was
                # armed); the await path then reads "pending" and converges on the already-resolved
                # cancelled halt, which is idempotent.
                self._halt(Halt("cancelled"))
                return "ok"
            self._assert_kind(seq, "sleep")
            due_ms = self._clock() + ms
            status = await self._append_cas(
                stamp({"kind": "sleep", "seq": seq, "name": name, "dueMs": due_ms}, self._clock()),
                MsgId.command(self.run_id, seq),
            )
            if status != "ok":
                return status
        else:
            recorded = cast("dict[str, Any]", cmd)
            if recorded.get("name") != name:
                raise NonDeterminismError(
                    self.run_id, seq, f'expected sleep "{recorded.get("name")}", got "{name}"'
                )
            due_ms = recorded["dueMs"]
        if self._find_resolve(seq) is not None:
            return "ok"  # already fired — nothing to (re-)arm
        return await self._ensure_wake(seq, due_ms)

    async def _ensure_wake(self, seq: int, due_ms: int) -> AppendStatus:
        try:
            await self._scheduler.schedule_wake(self.run_id, self.workflow, seq, due_ms)
        except Exception as e:  # noqa: BLE001 — scheduling is infrastructure: retry, never fail
            self._retry_error = e
            return "retry"
        return "ok"

    async def wait(self, name: str, timeout_ms: int | None = None) -> Any:
        """Block until an external signal of this name arrives (the wire kind is ``await``).

        Named ``wait`` because ``await`` is a Python keyword. With ``timeout_ms`` a durable timer
        races the signal; if the deadline wins, this raises :class:`DurableTimeoutError`, which is
        ordinary catchable control flow inside the workflow — not a failure.
        """
        return await self._await_future(self._start_wait("ctx.wait", name, timeout_ms))

    def start_wait(self, name: str, timeout_ms: int | None = None) -> DurableFuture[Any]:
        """Start waiting for a signal without blocking — await the future, or race it against others."""
        return self._start_wait("ctx.start_wait", name, timeout_ms)

    def _start_wait(self, op: str, name: str, timeout_ms: int | None) -> _DurableFuture:
        self._assert_not_in_step(op)
        seq = self._next_seq()
        return self._issue(seq, "await", name, self._arm_wait(seq, name, timeout_ms))

    async def _arm_wait(self, seq: int, name: str, timeout_ms: int | None) -> AppendStatus:
        cmd = self._find_at(seq, "await")
        if cmd is None:
            if self._cancel_pending():
                self._halt(Halt("cancelled"))
                return "ok"
            self._assert_kind(seq, "await")
            due_ms = self._clock() + timeout_ms if timeout_ms is not None else None
            body: dict[str, Any] = {"kind": "await", "seq": seq, "name": name}
            if due_ms is not None:
                body["dueMs"] = due_ms
            status = await self._append_cas(
                stamp(cast("JournalEntry", body), self._clock()),
                MsgId.command(self.run_id, seq),
            )
            if status != "ok":
                return status
        else:
            recorded = cast("dict[str, Any]", cmd)
            if recorded.get("name") != name:
                raise NonDeterminismError(
                    self.run_id, seq, f'expected await "{recorded.get("name")}", got "{name}"'
                )
            due_ms = recorded.get("dueMs")
        # Pending with a timeout → (re-)arm the timer. A resolved or pure-signal await arms nothing.
        if due_ms is not None and self._resolve_await(seq, name)[0] == "pending":
            return await self._ensure_wake(seq, due_ms)
        return "ok"

    def _resolve_await(self, seq: int, name: str) -> tuple[str, Any, int]:
        """Resolve the await at ``seq`` deterministically. Returns ``(kind, data, stream_seq)``.

        Signals of a name are matched to same-named awaits **in command order, FIFO**. An await
        consumes a signal only when that signal's stream-seq precedes the await's own timeout — so a
        timed-out await consumes nothing and never shifts the match for a later same-named await.
        Everything here is derived from journaled stream-seqs, so every replay computes the same
        pairing.
        """
        signals = [
            (cast("dict[str, Any]", le.entry).get("data"), le.stream_seq)
            for le in self._entries
            if le.entry["kind"] == "signal"
            and cast("dict[str, Any]", le.entry).get("name") == name
            and self._before_cancel(le.stream_seq)
        ]
        awaits = sorted(
            (
                (le.entry["seq"], cast("dict[str, Any]", le.entry).get("dueMs"))
                for le in self._entries
                if le.entry["kind"] == "await"
                and cast("dict[str, Any]", le.entry).get("name") == name
                and le.entry["seq"] <= seq
            ),
            key=lambda pair: pair[0],
        )

        sig_index = 0
        for await_seq, due_ms in awaits:
            timer_seq: int | None = None
            if due_ms is not None:
                resolve = self._find_resolve_loaded(await_seq)
                if resolve is not None and self._before_cancel(resolve.stream_seq):
                    timer_seq = resolve.stream_seq
            signal = signals[sig_index] if sig_index < len(signals) else None
            # The signal wins this await iff one is available and it landed no later than the timeout.
            if signal is not None and (timer_seq is None or signal[1] <= timer_seq):
                if await_seq == seq:
                    return ("signal", signal[0], signal[1])
                sig_index += 1  # an earlier await consumed it — advance the FIFO cursor
            elif await_seq == seq:
                return (
                    ("timeout", None, timer_seq) if timer_seq is not None else ("pending", None, 0)
                )
            # else: an earlier await consumed nothing (timed out or still pending) — cursor unchanged
        return ("pending", None, 0)

    # ------------------------------------------------------------------ sub-workflows

    async def call(self, workflow: Workflow[Any, T], input: Any, *, run_id: str | None = None) -> T:
        """Run a sub-workflow and resume with its result. A child failure fails the parent."""
        return cast(
            "T", await self._await_future(self._start_call("ctx.call", workflow, input, run_id))
        )

    def start_call(
        self, workflow: Workflow[Any, Any], input: Any, *, run_id: str | None = None
    ) -> DurableFuture[Any]:
        """Start a sub-workflow WITHOUT blocking, returning a durable future.

        Gather several to fan out children concurrently; ``ctx.call`` is ``await ctx.start_call``.
        """
        return self._start_call("ctx.start_call", workflow, input, run_id)

    def _start_call(
        self, op: str, workflow: Workflow[Any, Any], input: Any, child_run_id: str | None
    ) -> _DurableFuture:
        self._assert_not_in_step(op)
        seq = self._next_seq()
        return self._issue(
            seq, "call", workflow.name, self._arm_call(seq, workflow, input, child_run_id)
        )

    async def _arm_call(
        self,
        seq: int,
        workflow: Workflow[Any, Any],
        input: Any,
        child_run_id: str | None,
    ) -> AppendStatus:
        cmd = self._find_at(seq, "call")
        if cmd is None:
            if self._cancel_pending():
                # A cancelled run must not spawn work.
                self._halt(Halt("cancelled"))
                return "ok"
            self._assert_kind(seq, "call")
            child = child_run_id if child_run_id is not None else f"{self.run_id}_{seq}"
            status = await self._append_cas(
                stamp(
                    {
                        "kind": "call",
                        "seq": seq,
                        "name": workflow.name,
                        "childRunId": child,
                        "wf": workflow.name,
                        "input": input,
                    },
                    self._clock(),
                ),
                MsgId.command(self.run_id, seq),
            )
            if status != "ok":
                return status  # fenced / retry / fail (child input unencodable)
            return await self._ensure_child(child, workflow.name, workflow.version, input, seq)

        recorded = cast("dict[str, Any]", cmd)
        if recorded.get("wf") != workflow.name:
            raise NonDeterminismError(
                self.run_id,
                seq,
                f'expected call to "{recorded.get("wf")}", got "{workflow.name}"',
            )
        if self._find_resolve(seq) is not None:
            return "ok"  # already finished — no need to (re-)start the child
        return await self._ensure_child(
            recorded["childRunId"], recorded["wf"], workflow.version, recorded.get("input"), seq
        )

    async def _ensure_child(
        self, child_run_id: str, wf: str, version: int, input: Any, seq: int
    ) -> AppendStatus:
        from synadia_durable.core.children import ChildStart, ParentRef

        try:
            await self._children.start(
                ChildStart(
                    child_run_id=child_run_id,
                    workflow=wf,
                    version=version,
                    input=input,
                    parent=ParentRef(run_id=self.run_id, seq=seq, wf=self.workflow),
                )
            )
        except Exception as e:  # noqa: BLE001 — starting a child is infrastructure: retry
            self._retry_error = e
            return "retry"
        return "ok"

    # ------------------------------------------------------------------ resolving a durable op

    def _issue(
        self,
        seq: int,
        kind: Literal["call", "sleep", "await"],
        name: str,
        arm: Coroutine[Any, Any, AppendStatus],
    ) -> _DurableFuture:
        """Arm a durable op EAGERLY and hand back its future.

        The Task is what makes ``start_*`` fan out: a bare coroutine would not journal its command
        or arm its side effect until something awaited it, so ``gather`` over two branches would
        start the second only after the first had suspended — i.e. never.
        """
        task = asyncio.ensure_future(arm)
        self._arm_tasks.append(task)
        return _DurableFuture(self, seq, kind, name, task)

    async def race(self, futures: Sequence[DurableFuture[Any]]) -> Any:
        """Resolve with the JOURNAL-EARLIEST settled branch; suspend only while every branch pends.

        The safe alternative to racing durable futures with ``asyncio`` primitives: those resolve on
        wall-clock order within one execution, which is not stable across a replay, and a pending
        branch would suspend the run before a ready one could win. The winner here is chosen by
        stream-seq — journaled, so every replay picks the same branch — and losing branches keep
        running, their late results journaled and ignored.
        """
        if not futures:
            raise WorkflowFailure(self.run_id, "ctx.race requires at least one future")
        fs = [self._as_durable(f) for f in futures]
        statuses = await asyncio.gather(*(f.arm for f in fs))
        if "fenced" in statuses:
            return await self._halt_fenced()
        if "retry" in statuses:
            return await self._halt_retry()
        if "fail" in statuses:
            return await self._halt_fail()
        winner: tuple[_Resolution, _DurableFuture] | None = None
        for f in fs:
            res = self._resolution_of(f)
            if res.state == "pending":
                continue
            if winner is None or res.stream_seq < winner[0].stream_seq:
                winner = (res, f)
        if winner is None:
            return await self._halt_suspend()  # all pending — every wake is armed
        return await self._settle(winner[0], winner[1])

    def _as_durable(self, future: DurableFuture[Any]) -> _DurableFuture:
        """Reject anything that is not one of OUR futures — ``race`` is journal-ordered, not clock-ordered."""
        if not isinstance(future, _DurableFuture):
            raise WorkflowFailure(
                self.run_id,
                "ctx.race accepts only durable futures from ctx.start_call / start_sleep / "
                f"start_wait, got {type(future).__name__}",
            )
        return future

    async def _await_future(self, future: _DurableFuture) -> Any:
        status = await future.arm
        if status == "fenced":
            return await self._halt_fenced()
        if status == "retry":
            return await self._halt_retry()
        if status == "fail":
            return await self._halt_fail()
        return await self._settle(self._resolution_of(future), future)

    async def _settle(self, res: _Resolution, future: _DurableFuture) -> Any:
        if res.state == "pending":
            return await self._halt_suspend()
        if res.state == "value":
            return res.value
        if res.state == "timeout":
            what = f'await "{future.name}"' if future.kind == "await" else f'"{future.name}"'
            raise DurableTimeoutError(f"run {self.run_id}: {what} timed out (command {future.seq})")
        error = res.error or {"name": "Error", "message": "sub-workflow failed"}
        raise WorkflowFailure(
            self.run_id,
            f'sub-workflow "{future.name}" failed: {error["name"]}: {error["message"]}',
        )

    def _resolution_of(self, future: _DurableFuture) -> _Resolution:
        if future.kind in ("call", "sleep"):
            loaded = next(
                (
                    le
                    for le in self._entries
                    if le.entry["kind"] == "resolve"
                    and cast("dict[str, Any]", le.entry).get("resolvesSeq") == future.seq
                    and self._before_cancel(le.stream_seq)
                ),
                None,
            )
            if loaded is None:
                return _Resolution("pending")
            entry = cast("dict[str, Any]", loaded.entry)
            if entry.get("error") is not None:
                return _Resolution("error", loaded.stream_seq, error=entry["error"])
            value = None if future.kind == "sleep" else entry.get("value")
            return _Resolution("value", loaded.stream_seq, value=value)

        kind, data, stream_seq = self._resolve_await(future.seq, future.name)
        if kind == "pending":
            return _Resolution("pending")
        if kind == "signal":
            return _Resolution("value", stream_seq, value=data)
        return _Resolution("timeout", stream_seq)

    # ------------------------------------------------------------------ continue-as-new

    async def continue_as_new(self, input: Any, *, version: int | None = None) -> NoReturn:
        """End this generation and atomically start a fresh one, keeping the journal bounded.

        Code after this call never runs. The original handle follows the chain to whichever
        generation finally returns.
        """
        self._assert_not_in_step("ctx.continue_as_new")
        if self._in_cleanup:
            # Same rule as suspending ops: cleanup is LOCAL. Halting from inside a handler would
            # return a never-completing awaitable INTO the cleanup loop and hang the cancel forever.
            raise DurableError(
                f"run {self.run_id}: an on_cancel handler may not call continue_as_new "
                f"(use local ctx.run steps only)"
            )
        if version is not None and (
            not isinstance(version, int) or isinstance(version, bool) or version < 1
        ):
            raise ValueError("ctx.continue_as_new: version must be a positive integer")
        if self._cancel_at is not None:
            # Cancel requested before this generation rolled over → cancel in place; the next
            # generation is never started. (A cancel arriving AFTER `continued` is forwarded by the
            # worker to the next generation.)
            await self._halt_cancelled()
        generation = self._generation + 1
        self._continue_spec = ContinueSpec(
            next_run_id=f"{self._base}_g{generation}",
            input=input,
            generation=generation,
            base=self._base,
            version=version if version is not None else self.version,
        )
        self._halt(Halt("continue"))
        await self._wait_forever()

    def next_generation_spec(self) -> ContinueSpec:
        return invariant_not_none(
            self._continue_spec, f"run {self.run_id}: continue outcome without a spec"
        )

    async def append_terminal_continue(self) -> AppendStatus | Literal["adopted"]:
        """Append the `continued` terminal (executor-driven, after the fully-consumed guard)."""
        spec = invariant_not_none(
            self._continue_spec, f"run {self.run_id}: continue without a spec"
        )
        status = await self._append_terminal_cas(
            stamp(
                {
                    "kind": "continued",
                    "seq": NON_COMMAND_SEQ,
                    "nextRunId": spec.next_run_id,
                    "input": spec.input,
                    "generation": spec.generation,
                    "base": spec.base,
                    "version": spec.version,
                },
                self._clock(),
            )
        )
        if status in ("retry", "fail"):
            return status
        # Confirm the terminal really is a `continued`. The terminal msgID is shared with
        # result/fail/cancelled, so a deduplicated append (or a lost CAS) can report "ok" without
        # OUR entry landing — only continue if the journal actually records a `continued`.
        reloaded = await self._journal.load(self.run_id)
        if not any(le.entry["kind"] == "continued" for le in reloaded):
            return "fenced"
        # A `continued` exists but WE lost the CAS: adopt it, exactly as the result / fail /
        # cancelled paths do. ``nextRunId`` is deterministic, but the rollover INPUT and the target
        # VERSION are not — they come from this incarnation, and two incarnations racing the
        # boundary can compute different ones. Reporting our own would let the worker start the next
        # generation from a value the journal does not record, which is the journal quietly ceasing
        # to be the source of truth. ("ok" here cannot be a dedup of somebody else's terminal: a
        # `continued` is the last entry, so if one exists the tail moved and our CAS could not have
        # matched.)
        return "ok" if status == "ok" else "adopted"

    # ------------------------------------------------------------------ the rest of the surface

    def key(self) -> str:
        """The current step's idempotency key, ``{runId}:{stepSeq}`` — for user side effects."""
        seq = self._current_step()
        # `or 0` would be wrong: step seq 0 is falsy but perfectly real.
        return f"{self.run_id}:{0 if seq is None else seq}"

    def get(self, key: str) -> Any:
        """Resolve an injected dependency (a NATS connection, an http client, config, …)."""
        if key not in self._deps:
            raise KeyError(f'ctx.get: no dependency registered for "{key}"')
        return self._deps[key]

    def check(self, condition: bool, message: str | None = None) -> None:
        """Invariant guard — fail the run loudly if false. Named ``check`` (``assert`` is a keyword).

        Unlike a bare Python ``assert``, this is not stripped under ``-O``, which matters when the
        thing being guarded decides whether a side effect fires.
        """
        if not condition:
            raise WorkflowFailure(self.run_id, message or "ctx.check failed")

    def on_cancel(self, fn: Callable[[], Any]) -> None:
        """Register a compensation handler, run (LIFO) if this run is cancelled, before the terminal.

        Handlers run in a non-cancellable context and may only do local ``ctx.run`` steps.
        """
        self._cancel_handlers.append(fn)

    async def run_cleanup(self) -> Halt | None:
        """Run the on_cancel handlers (LIFO) before the executor writes the `cancelled` terminal.

        Handler errors are best-effort: a failing handler never blocks the cancel.

        Returns the halt that ABORTED the cleanup, if one did. A cleanup step journals like any
        other, so its append can lose the CAS or hit an infrastructure error — and the run is
        already halted (`cancelled` is how we got here), so that second halt cannot reach the
        executor through the halt future. Without this the step awaited the never-future INSIDE the
        handler this method awaits: one transient publish error during a cancel and `execute_run`
        never returned — the worker held the item, the lease and a concurrency slot for good. A
        `fenced` or `retry` abort ends the cleanup (the redelivery replays it from the journal); an
        unencodable step result only ends its own handler, like any other handler error.
        """
        if not self._cancel_handlers:
            return None
        loop = asyncio.get_running_loop()
        self._in_cleanup = True
        try:
            for handler in reversed(self._cancel_handlers):
                try:
                    result = handler()
                except Exception:  # noqa: BLE001 — best-effort: a handler error never blocks the cancel
                    continue
                if not inspect.isawaitable(result):
                    continue
                abort: asyncio.Future[Halt] = loop.create_future()
                self._cleanup_abort = abort
                task = asyncio.ensure_future(result)
                await asyncio.wait({task, abort}, return_when=asyncio.FIRST_COMPLETED)
                if task.done():
                    # Retrieve, so a handler error is swallowed rather than logged as never-retrieved.
                    with contextlib.suppress(Exception):
                        task.result()
                    continue
                # The handler is parked on the never-future and must stay there: cancelling it now
                # would cancel that future too, and wake the MAIN flow before the ctx is closed
                # (LEARNINGS #45). The executor disposes of it in its teardown, after `close()`.
                self._abandoned_handlers.append(task)
                aborted = abort.result()
                if aborted.tag != "fail":
                    return aborted
            return None
        finally:
            self._in_cleanup = False
            self._cleanup_abort = None

    # ------------------------------------------------------------------ terminals (executor-driven)

    async def append_terminal_result(self, value: Any) -> AppendStatus | Literal["adopted"]:
        status = await self._append_terminal_cas(
            stamp({"kind": "result", "seq": NON_COMMAND_SEQ, "value": value}, self._clock())
        )
        if status in ("retry", "fail", "ok"):
            return status
        return await self._reconcile_terminal()

    async def append_terminal_fail(self, error: ErrorShape) -> AppendStatus | Literal["adopted"]:
        status = await self._append_terminal_cas(
            stamp({"kind": "fail", "seq": NON_COMMAND_SEQ, "error": error}, self._clock())
        )
        if status in ("retry", "ok"):
            return status
        return await self._reconcile_terminal()

    async def append_terminal_cancelled(self) -> AppendStatus | Literal["adopted"]:
        body: dict[str, Any] = {"kind": "cancelled", "seq": NON_COMMAND_SEQ}
        if self._cancel_reason is not None:
            body["reason"] = self._cancel_reason
        status = await self._append_terminal_cas(stamp(cast("JournalEntry", body), self._clock()))
        if status in ("retry", "ok"):
            return status
        return await self._reconcile_terminal()

    async def _reconcile_terminal(self) -> Literal["adopted", "fenced"]:
        """After a terminal CAS-loss: adopt the canonical terminal if one exists, else fence.

        Mirrors the step-end reconcile — the journal is the truth, so the loser adopts rather than
        reporting its own outcome.
        """
        return "adopted" if await self.canonical_terminal() is not None else "fenced"

    async def canonical_terminal(self) -> JournalEntry | None:
        reloaded = await self._journal.load(self.run_id)
        return next(
            (le.entry for le in reloaded if le.entry["kind"] in TERMINAL_KINDS),
            None,
        )

    def last_error(self) -> BaseException | None:
        """The infrastructure error that triggered a retry-halt (for the executor to report)."""
        return self._retry_error

    def fatal_error(self) -> ErrorShape:
        return invariant_not_none(
            self._fatal, f"run {self.run_id}: fatal_error() without a fatal error"
        )

    # ------------------------------------------------------------------ internals

    def _next_seq(self) -> int:
        seq = self._seq
        self._seq += 1
        return seq

    def _append_cas(self, entry: JournalEntry, msg_id: str) -> asyncio.Task[AppendStatus]:
        async def _run() -> AppendStatus:
            # Short-circuit ONLY after a real fence: the journal advanced under us, so every further
            # append would fence anyway. A suspend must fall through — a concurrent branch's
            # command-append is legitimate intent we still want journaled.
            if self._fenced:
                return "fenced"
            return await self._append_cas_raw(entry, msg_id)

        return self._enqueue_append(_run)

    def _append_terminal_cas(self, entry: JournalEntry) -> asyncio.Task[AppendStatus]:
        """Append a terminal. Bypasses the fence short-circuit deliberately: continue-as-new halts
        the ctx BEFORE the executor writes the `continued` terminal, so this write is executor-driven
        and intentional, never a stray append from suspended user code."""

        async def _append() -> AppendStatus:
            # ONE terminal per run is the CAS's job, and the CAS can only do it if we never aim it
            # past a terminal we already know about. Our view can hold one: the step-end reconcile
            # adopts a fresh journal, and the incarnation that beat us to that step may have gone
            # on to finish the run. Appending anyway would pass the CAS (the terminal is at or
            # before our tail), leaving only the shared terminal msgID between the run and a second
            # terminal — and a "duplicate" ack reads as "mine landed", so we would report our OWN
            # outcome. Report a lost race instead; the caller adopts the journal's terminal.
            if any(le.entry["kind"] in TERMINAL_KINDS for le in self._entries):
                return "fenced"
            return await self._append_cas_raw(entry, MsgId.terminal(self.run_id))

        return self._enqueue_append(_append)

    def _enqueue_append(
        self, run: Callable[[], Coroutine[Any, Any, AppendStatus]]
    ) -> asyncio.Task[AppendStatus]:
        """Serialize an append behind every append already in flight for this run.

        ``seq``s are assigned synchronously at the call site, but each CAS (read ``_last_seq`` →
        append → advance it) has to run one at a time: two concurrent fan-out branches would
        otherwise read the same ``_last_seq``, and the loser would self-fence on a race it should
        have won. The chain POSITION is claimed synchronously here, so appends land in call order,
        and one append's outcome never disturbs the next (they are values, not exceptions).
        """
        previous = self._append_chain

        async def _linked() -> AppendStatus:
            if previous is not None and not previous.done():
                # Wait for order, not for the result: `asyncio.wait` re-raises nothing, so a
                # predecessor that blew up cannot take this append down with it.
                await asyncio.wait([previous])
            return await run()

        task = asyncio.ensure_future(_linked())
        self._append_chain = task
        return task

    async def _append_cas_raw(self, entry: JournalEntry, msg_id: str) -> AppendStatus:
        try:
            result = await self._journal.append(
                self.run_id,
                JournalAppend(entry=entry, msg_id=msg_id, expect_last_seq=self._last_seq),
            )
        except FencedError:
            self._fenced = True  # a real lost CAS: short-circuit the rest of the appends
            return "fenced"
        except SerializationError as e:
            # Deterministic: re-encoding the same value always fails, so retrying is futile.
            self._fatal = {"name": type(e).__name__, "message": str(e)}
            return "fail"
        except Exception as e:  # noqa: BLE001 — anything else is infrastructure: retry
            self._retry_error = e
            return "retry"
        if not any(le.stream_seq == result.stream_seq for le in self._entries):
            self._entries.append(LoadedEntry(entry=entry, stream_seq=result.stream_seq))
        if result.stream_seq > self._last_seq:
            self._last_seq = result.stream_seq
        return "ok"

    async def _wait_forever(self) -> NoReturn:
        """Await something that never completes, so the workflow's continuation never runs.

        This — not an exception — is what makes a halt uncatchable: there is nothing to catch, and no
        `finally` in the main flow runs. Cleanup goes through ``on_cancel``.
        """
        await self._never
        raise InvariantViolation("the never-future completed; a halt must never resume user code")

    async def _halt_fenced(self) -> NoReturn:
        self._halt(Halt("fenced"))
        await self._wait_forever()

    async def _halt_cancelled(self) -> NoReturn:
        self._halt(Halt("cancelled"))
        await self._wait_forever()

    async def _halt_retry(self) -> NoReturn:
        self._halt(Halt("retry", self._retry_error))
        await self._wait_forever()

    async def _halt_fail(self) -> NoReturn:
        fatal = invariant_not_none(
            self._fatal, f"run {self.run_id}: fail-halt without a fatal error"
        )
        self._halt(Halt("fail", fatal))
        await self._wait_forever()

    async def _halt_suspend(self) -> NoReturn:
        if self._in_cleanup:
            # Cleanup is a non-cancellable, LOCAL context — suspending inside it is rejected.
            raise DurableError(
                f"run {self.run_id}: an on_cancel handler may not suspend "
                f"(use local ctx.run steps only)"
            )
        # A pending op while cancellation is requested IS the cancel point.
        self._halt(Halt("cancelled") if self._cancel_at is not None else Halt("suspend"))
        await self._wait_forever()

    def _halt(self, halt: Halt) -> None:
        # Cleanup runs AFTER the `cancelled` halt, so a halt raised by one of its steps finds the
        # run already halted; it still has to stop the cleanup (see run_cleanup).
        abort = self._cleanup_abort
        if self._in_cleanup and abort is not None and not abort.done():
            abort.set_result(halt)
        if self._halted:
            return
        self._halted = True
        if not self._halt_future.done():
            self._halt_future.set_result(halt)

    def _cancel_pending(self) -> bool:
        """True when a journaled cancel must hard-stop a FRESH or resumed command.

        The cancel point is the next command boundary: no new intent is appended and no step fn
        executes once cancellation is journaled. Recorded commands are unaffected — they replay, and
        an op resolved before the cancel still delivers its value.
        """
        return self._cancel_at is not None and not self._in_cleanup

    def _before_cancel(self, stream_seq: int) -> bool:
        """A journaled entry counts toward resolution only if it landed before a cancel froze the run."""
        return self._cancel_at is None or stream_seq < self._cancel_at

    def _adopt(self, reloaded: list[LoadedEntry]) -> None:
        self._entries = list(reloaded)
        if self._entries:
            self._last_seq = max(self._last_seq, self._entries[-1].stream_seq)

    def _find_at(self, seq: int, kind: str) -> JournalEntry | None:
        return next(
            (
                le.entry
                for le in self._entries
                if le.entry["seq"] == seq and le.entry["kind"] == kind
            ),
            None,
        )

    def _find_resolve_loaded(self, seq: int) -> LoadedEntry | None:
        return next(
            (
                le
                for le in self._entries
                if le.entry["kind"] == "resolve"
                and cast("dict[str, Any]", le.entry).get("resolvesSeq") == seq
            ),
            None,
        )

    def _find_resolve(self, seq: int) -> JournalEntry | None:
        loaded = self._find_resolve_loaded(seq)
        return loaded.entry if loaded is not None else None

    def _command_kind_at(self, seq: int) -> str | None:
        """The command kind recorded at this position, if any (ignores start/step-end/resolve/…)."""
        return next(
            (
                le.entry["kind"]
                for le in self._entries
                if le.entry["seq"] == seq and le.entry["kind"] in COMMAND_KINDS
            ),
            None,
        )

    def _assert_kind(self, seq: int, expected: str) -> None:
        found = self._command_kind_at(seq)
        if found is not None and found != expected:
            raise NonDeterminismError(
                self.run_id, seq, f'expected a "{expected}" but seq {seq} recorded a "{found}"'
            )

    def assert_fully_consumed(self) -> None:
        """After the workflow returns, ensure it consumed every recorded command.

        A replay that takes a SHORTER path than the journal records is nondeterministic even though
        nothing mismatched along the way — this is the guard that catches it.
        """
        max_cmd = -1
        for le in self._entries:
            if le.entry["kind"] in COMMAND_KINDS and le.entry["seq"] > max_cmd:
                max_cmd = le.entry["seq"]
        if max_cmd >= self._seq:
            raise NonDeterminismError(
                self.run_id,
                self._seq,
                f"replay consumed {self._seq} command(s) but the journal recorded through seq "
                f"{max_cmd} (workflow took a shorter path)",
            )

    def _current_step(self) -> int | None:
        """The seq of OUR step executing on this call chain, or None outside one."""
        scope = _STEP_SCOPE.get()
        return scope[1] if scope is not None and scope[0] is self else None

    def _assert_not_in_step(self, op: str) -> None:
        """Reject a ctx command issued inside a ctx.run step fn — it would interleave command seqs."""
        if self._closed:
            raise _Abandoned(f"run {self.run_id}: {op} after the run's outcome was committed")
        if self._current_step() is not None:
            raise StepNestingError(
                self.run_id,
                f"run {self.run_id}: {op} cannot be called inside a ctx.run step function",
            )

    def _assert_name(self, seq: int, recorded: str | None, actual: str) -> None:
        invariant(recorded is not None, f"run {self.run_id}: missing step-begin at seq {seq}")
        if recorded != actual:
            raise NonDeterminismError(
                self.run_id, seq, f'expected step "{recorded}", got "{actual}"'
            )


class _Abandoned(BaseException):
    """Raised by a ctx call made after the run's outcome was committed.

    A ``BaseException`` on purpose: user code's ``except Exception`` must not be able to swallow it
    and carry on journaling into a run the executor has already finalized.
    """


def _now_ms() -> int:
    return int(time.time() * 1000)
