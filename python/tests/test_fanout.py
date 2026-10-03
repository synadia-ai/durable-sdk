"""Durable fan-out — concurrent branches, ``asyncio.gather``, and ``ctx.race``.

Pure, no server: every crash, resume and race here is reachable in-process against
``InMemoryJournal``, which enforces the same CAS as JetStream does.

Mirrors ``test/unit/fanout-foundation.test.ts``, with the combinators Python actually uses:
``asyncio.gather`` is ``Promise.all`` and ``gather(..., return_exceptions=True)`` is
``Promise.allSettled``. Two things are Python's own and are pinned here rather than assumed —
a durable future is armed EAGERLY (a coroutine would be inert until awaited, so a fan-out would
start its second branch only after the first had suspended, i.e. never), and its command ``seq`` is
assigned when the op is ISSUED, not when it is awaited. See ``docs/DESIGN-python-sdk.md`` §3.2.
"""

from __future__ import annotations

import asyncio
import gc
from typing import Any

from synadia_durable.core.children import ChildStart
from synadia_durable.core.execute import ExecOutcome, execute_run
from synadia_durable.core.workflow import define_workflow
from synadia_durable.errors import FencedError, StepNestingError
from synadia_durable.journal.entry import (
    NON_COMMAND_SEQ,
    TERMINAL_KINDS,
    JournalEntry,
    LoadedEntry,
    MsgId,
    stamp,
)
from synadia_durable.journal.store import AppendResult, InMemoryJournal, JournalAppend
from synadia_durable.types import Ctx, DurableFuture, Workflow

TS = 1781667505735
RUN = "fan"


async def _child_body(_ctx: Any, _input: Any) -> Any:
    raise AssertionError("a child workflow body must not run inside its parent")


CHILD = define_workflow("child", _child_body)


class _Scheduler:
    """Records the wakes it was asked to arm."""

    def __init__(self) -> None:
        self.wakes: list[tuple[int, int]] = []

    async def schedule_wake(self, run_id: str, workflow: str, seq: int, due_ms: int) -> None:  # noqa: ARG002 — the seam's signature
        self.wakes.append((seq, due_ms))


class _Children:
    """Records the child starts it was asked for.

    ``fail`` makes the seam unavailable; ``delay_s`` makes starting a child take long enough that
    the executor would return with it still in flight — which is what the drain has to prevent.
    """

    def __init__(self, fail: bool = False, delay_s: float = 0.0) -> None:
        self.started: list[ChildStart] = []
        self.fail = fail
        self.delay_s = delay_s

    async def start(self, child: ChildStart) -> None:
        if self.fail:
            raise ConnectionError("cannot reach the work queue")
        if self.delay_s:
            await asyncio.sleep(self.delay_s)
        self.started.append(child)


class _Journal(InMemoryJournal):
    """An ``InMemoryJournal`` that YIELDS before each append, as any real transport does.

    This is not a nicety, and getting it wrong makes a whole file of concurrency tests vacuous. In
    JavaScript every ``await`` yields, so two concurrent branches always interleave and the reference
    implementation's in-memory tests reproduce the self-fence. In Python, awaiting a coroutine that
    never suspends runs it straight through, so a plain ``InMemoryJournal`` makes each append atomic
    — CAS read, append and ``_last_seq`` advance in one uninterruptible step — and the race the
    append chain exists to prevent simply cannot occur. One ``sleep(0)`` puts back the interleaving a
    NATS publish would have. (Verified: with this seam, removing the chain's ordering fences these
    tests; without it, removing the chain changes nothing.)
    """

    async def append(self, run_id: str, append: JournalAppend) -> AppendResult:
        await asyncio.sleep(0)
        return await super().append(run_id, append)


class _SlowJournal(_Journal):
    """A journal whose append of one entry kind takes long enough to still be in flight.

    The point of the delay is the ack: a worker acks the instant the executor returns, so anything
    unlanded at that moment is lost with nothing left to redeliver it.
    """

    def __init__(self, kind: str, delay_s: float = 0.02) -> None:
        super().__init__()
        self.kind = kind
        self.delay_s = delay_s
        #: Set once the delayed entry has actually landed — read it with no ``await`` in between, or
        #: the read itself gives the in-flight append the tick it needed and proves nothing.
        self.landed = False

    async def append(self, run_id: str, append: JournalAppend) -> AppendResult:
        if append.entry["kind"] == self.kind:
            await asyncio.sleep(self.delay_s)
            result = await super().append(run_id, append)
            self.landed = True
            return result
        return await super().append(run_id, append)


class Fan:
    """One run, its journal and the seams — the shape a worker would assemble."""

    def __init__(
        self, workflow: Workflow[Any, Any], journal: InMemoryJournal | None = None
    ) -> None:
        self.workflow = workflow
        self.journal = journal or _Journal()
        self.scheduler = _Scheduler()
        self.children = _Children()

    async def append(self, entry: JournalEntry, msg_id: str) -> int:
        loaded = await self.journal.load(RUN)
        last = loaded[-1].stream_seq if loaded else 0
        result = await self.journal.append(RUN, JournalAppend(entry, msg_id, last))
        return result.stream_seq

    async def start_entry(self, input: Any = None) -> None:
        body: dict[str, Any] = {
            "kind": "start",
            "seq": 0,
            "wf": self.workflow.name,
            "wfVersion": self.workflow.version,
            "input": input,
        }
        await self.append(stamp(body, TS), MsgId.start(RUN))  # type: ignore[arg-type]

    async def signal(self, name: str, data: Any, signal_id: str) -> int:
        return await self.append(
            stamp(
                {
                    "kind": "signal",
                    "seq": NON_COMMAND_SEQ,
                    "name": name,
                    "data": data,
                    "signalId": signal_id,
                },
                TS,
            ),
            MsgId.signal(RUN, signal_id),
        )

    async def resolve(self, seq: int, **fields: Any) -> int:
        return await self.append(
            stamp({"kind": "resolve", "seq": NON_COMMAND_SEQ, "resolvesSeq": seq, **fields}, TS),  # type: ignore[arg-type]
            MsgId.resolve(RUN, seq),
        )

    async def call_command(self, seq: int, input: Any = None) -> int:
        return await self.append(
            stamp(
                {
                    "kind": "call",
                    "seq": seq,
                    "name": CHILD.name,
                    "childRunId": f"{RUN}_{seq}",
                    "wf": CHILD.name,
                    "input": input,
                },
                TS,
            ),
            MsgId.command(RUN, seq),
        )

    async def await_command(self, seq: int, name: str) -> int:
        return await self.append(
            stamp({"kind": "await", "seq": seq, "name": name}, TS),
            MsgId.command(RUN, seq),
        )

    async def cancel_requested(self) -> int:
        return await self.append(
            stamp({"kind": "cancel-requested", "seq": NON_COMMAND_SEQ}, TS),
            MsgId.cancel(RUN),
        )

    async def drive(self, *, replay: bool = False) -> ExecOutcome:
        """Drive the run once. ``replay=True``: from the history BEFORE the terminal — a journal
        that holds one is never executed, so watching a finished run replay has to be asked for."""
        loaded = await self.journal.load(RUN)
        if replay:
            cut = next(
                (i for i, le in enumerate(loaded) if le.entry["kind"] in TERMINAL_KINDS),
                len(loaded),
            )
            loaded = loaded[:cut]
        return await execute_run(
            RUN,
            workflow=self.workflow,
            journal=self.journal,
            loaded=loaded,
            scheduler=self.scheduler,
            children=self.children,
            clock=lambda: TS,
        )

    async def entries(self) -> list[LoadedEntry]:
        return await self.journal.load(RUN)

    async def at(self, kind: str, seq: int) -> list[dict[str, Any]]:
        return [
            dict(le.entry)
            for le in await self.entries()
            if le.entry["kind"] == kind and le.entry["seq"] == seq
        ]

    async def kinds(self) -> list[str]:
        return [le.entry["kind"] for le in await self.entries()]


async def _seeded(workflow: Workflow[Any, Any], input: Any = None) -> Fan:
    f = Fan(workflow)
    await f.start_entry(input)
    return f


# ---------------------------------------------------------------- concurrent appends (the chain)


def _parallel_steps(counter: dict[str, int], names: tuple[str, ...] = ("a", "b", "c")) -> Any:
    """N steps run CONCURRENTLY, each its own journaled step.

    Without the append chain the second step's ``step-begin`` self-fences: both branches read the
    same ``_last_seq`` and the loser gets a ``FencedError`` on a race it should never have been in.
    """

    async def body(ctx: Ctx, _input: Any) -> str:
        def effect(name: str) -> str:
            counter[name] = counter.get(name, 0) + 1
            return name.upper()

        out = await asyncio.gather(*(ctx.run(name, effect, name) for name in names))
        return "".join(str(x) for x in out)

    return define_workflow("w", body)


async def test_gather_over_ctx_run_completes_without_self_fencing() -> None:
    counter: dict[str, int] = {}
    f = await _seeded(_parallel_steps(counter))
    outcome = await f.drive()
    assert outcome.kind == "completed"
    assert outcome.value == "ABC"
    assert counter == {"a": 1, "b": 1, "c": 1}  # each step's fn ran exactly once


async def test_each_concurrent_step_is_journaled_exactly_once() -> None:
    f = await _seeded(_parallel_steps({}))
    await f.drive()
    for seq in range(3):
        assert len(await f.at("step-begin", seq)) == 1
        assert len(await f.at("step-end", seq)) == 1


async def test_concurrent_steps_replay_without_re_running() -> None:
    counter: dict[str, int] = {}
    f = await _seeded(_parallel_steps(counter))
    await f.drive()
    assert counter == {"a": 1, "b": 1, "c": 1}

    replay = Fan(_parallel_steps(counter), f.journal)
    outcome = await replay.drive(replay=True)
    assert outcome.kind == "completed"  # it loses the terminal CAS and adopts the one that is there
    assert counter == {"a": 1, "b": 1, "c": 1}, "a recorded step must never re-execute"


async def test_multiple_begun_steps_resume_and_re_run_only_those() -> None:
    counter: dict[str, int] = {}
    f = await _seeded(_parallel_steps(counter, ("a", "b")))
    # Two concurrent begins landed, neither ended: a crash mid-fan-out.
    for seq, name in ((0, "a"), (1, "b")):
        await f.append(
            stamp({"kind": "step-begin", "seq": seq, "name": name, "key": f"{RUN}:{seq}"}, TS),
            MsgId.step_begin(RUN, seq),
        )
    outcome = await f.drive()
    assert outcome.kind == "completed"
    assert outcome.value == "AB"
    assert counter == {"a": 1, "b": 1}, "both in-flight steps re-ran once (at-least-once)"


async def test_ctx_key_is_scoped_per_concurrent_step() -> None:
    keys: dict[str, str] = {}

    async def body(ctx: Ctx, _input: Any) -> str:
        def effect(name: str) -> str:
            keys[name] = ctx.key()  # each step must see ITS OWN idempotency key
            return name

        await asyncio.gather(*(ctx.run(name, effect, name) for name in ("a", "b")))
        return "ok"

    f = await _seeded(define_workflow("w", body))
    await f.drive()
    # A ContextVar, not a counter: the scope follows each branch and is invisible to its sibling.
    assert keys == {"a": f"{RUN}:0", "b": f"{RUN}:1"}


async def test_out_of_order_step_ends_still_replay() -> None:
    """Resolution is by command ``seq``, never by journal position."""
    counter: dict[str, int] = {}
    f = await _seeded(_parallel_steps(counter, ("a", "b")))
    for seq, name in ((0, "a"), (1, "b")):
        await f.append(
            stamp({"kind": "step-begin", "seq": seq, "name": name, "key": f"{RUN}:{seq}"}, TS),
            MsgId.step_begin(RUN, seq),
        )
    # seq 1 finished BEFORE seq 0 — ends journaled out of seq order.
    for seq, name in ((1, "b"), (0, "a")):
        await f.append(
            stamp({"kind": "step-end", "seq": seq, "name": name, "value": name.upper()}, TS),
            MsgId.step_end(RUN, seq),
        )
    outcome = await f.drive()
    assert outcome.kind == "completed"
    assert outcome.value == "AB"
    assert counter == {}, "both replayed from the journal — neither fn ran"


async def test_a_fenced_branch_halts_the_whole_run() -> None:
    class FenceSecondBegin(_Journal):
        async def append(self, run_id: str, append: JournalAppend) -> AppendResult:
            if append.msg_id == MsgId.step_begin(RUN, 1):
                raise FencedError(run_id, "injected: another worker won")
            return await super().append(run_id, append)

    f = Fan(_parallel_steps({}, ("a", "b")), FenceSecondBegin())
    await f.start_entry()
    outcome = await f.drive()
    # Half a fan-out must never look like a whole one: the run halts, it does not half-complete.
    assert outcome.kind == "fenced"


async def test_a_raising_branch_fails_the_run() -> None:
    async def body(ctx: Ctx, _input: Any) -> str:
        def boom() -> str:
            raise ValueError("kaboom")

        await asyncio.gather(ctx.run("ok", lambda: 1), ctx.run("boom", boom))
        return "done"

    f = await _seeded(define_workflow("w", body))
    outcome = await f.drive()
    assert outcome.kind == "failed"
    assert outcome.error is not None
    assert outcome.error["message"] == "kaboom"


async def test_gather_over_mixed_non_suspending_ops_journals_each_at_its_own_seq() -> None:
    async def body(ctx: Ctx, _input: Any) -> dict[str, Any]:
        step, now, uid = await asyncio.gather(ctx.run("step", lambda: "S"), ctx.now(), ctx.uuid())
        return {"step": step, "now": now, "uuid_is_str": isinstance(uid, str)}

    f = await _seeded(define_workflow("w", body))
    outcome = await f.drive()
    assert outcome.kind == "completed"
    assert outcome.value == {"step": "S", "now": TS, "uuid_is_str": True}
    assert await f.at("now", 1) != []  # run = seq 0 (begin+end), now = seq 1, uuid = seq 2
    assert await f.at("uuid", 2) != []


async def test_a_start_call_inside_a_step_fails_loudly() -> None:
    """The nesting guard covers the SYNC start_* methods too, not only the awaitable ones."""

    async def body(ctx: Ctx, _input: Any) -> str:
        def inner() -> Any:
            return ctx.start_call(CHILD, 1)  # illegal: a command issued inside a step fn

        await ctx.run("outer", inner)
        return "ok"

    f = await _seeded(define_workflow("w", body))
    outcome = await f.drive()
    assert outcome.kind == "failed"
    assert outcome.error is not None
    assert outcome.error["name"] == StepNestingError.__name__


# ---------------------------------------------------------------- durable futures


async def test_gather_over_two_start_calls_starts_both_in_one_pass() -> None:
    async def body(ctx: Ctx, _input: Any) -> int:
        a, b = await asyncio.gather(ctx.start_call(CHILD, 1), ctx.start_call(CHILD, 2))
        return int(a) + int(b)

    f = await _seeded(define_workflow("w", body))
    first = await f.drive()
    assert first.kind == "suspended"
    assert sorted(c.parent.seq for c in f.children.started) == [0, 1], "BOTH started in one pass"

    # Both children finish — resolves journaled OUT OF ORDER (seq 1 before seq 0).
    await f.resolve(1, value=2)
    await f.resolve(0, value=1)

    f.children = _Children()
    second = await f.drive()
    assert second.kind == "completed"
    assert second.value == 3
    assert f.children.started == [], "a resolved call must not restart its child"


async def test_a_fan_out_that_crashed_mid_flight_journals_the_rest_on_replay() -> None:
    """The real-world crash: one branch's intent survived, the other never landed.

    Replay must add exactly the missing command — not duplicate the surviving one — and re-arm both
    children, since neither has resolved (starting a child is idempotent on the child's runId).
    """

    async def body(ctx: Ctx, _input: Any) -> Any:
        a, b = await asyncio.gather(ctx.start_call(CHILD, 1), ctx.start_call(CHILD, 2))
        return int(a) + int(b)

    f = await _seeded(define_workflow("w", body))
    await f.call_command(0, input=1)  # only branch 0's intent is in the journal
    outcome = await f.drive()
    assert outcome.kind == "suspended"
    assert len(await f.at("call", 0)) == 1, "the surviving command was not written twice"
    assert len(await f.at("call", 1)) == 1, "the missing command was journaled"
    assert sorted(c.parent.seq for c in f.children.started) == [0, 1]


async def test_a_failed_child_fails_the_gathered_run() -> None:
    async def body(ctx: Ctx, _input: Any) -> int:
        a, b = await asyncio.gather(ctx.start_call(CHILD, 1), ctx.start_call(CHILD, 2))
        return int(a) + int(b)

    f = await _seeded(define_workflow("w", body))
    await f.drive()
    await f.resolve(0, error={"name": "Error", "message": "child boom"})
    await f.resolve(1, value=2)
    outcome = await f.drive()
    assert outcome.kind == "failed"
    assert outcome.error is not None
    assert "child boom" in outcome.error["message"]


async def test_gather_over_two_waits_resolves_only_when_both_signals_arrive() -> None:
    async def body(ctx: Ctx, _input: Any) -> int:
        x, y = await asyncio.gather(ctx.start_wait("x"), ctx.start_wait("y"))
        return int(x) + int(y)

    f = await _seeded(define_workflow("w", body))
    assert (await f.drive()).kind == "suspended"  # both awaits journaled, neither signalled

    await f.signal("y", 20, "s-y")
    assert (await f.drive()).kind == "suspended", "still waiting on x"

    await f.signal("x", 5, "s-x")
    outcome = await f.drive()
    assert outcome.kind == "completed"
    assert outcome.value == 25  # matched by NAME, whatever the arrival order


async def test_a_three_way_fan_out_suspends_instead_of_fencing_a_sibling() -> None:
    """The fence fix as an observable invariant: pass 1 must be ``suspended``, never ``fenced``.

    ``start_wait`` arms fastest (no child start, no timer), so a suspend that short-circuited the
    append chain would turn a sibling's perfectly legitimate command-append into a fence and abort
    the run. Only a real lost CAS may fence. (Asserting journal contents right after the suspend
    would be racy; the run completing on replay is the deterministic proof.)
    """

    async def body(ctx: Ctx, _input: Any) -> str:
        parts = await asyncio.gather(*(ctx.start_wait(n) for n in ("a", "b", "c")))
        return "".join(str(p) for p in parts)

    f = await _seeded(define_workflow("w", body))
    assert (await f.drive()).kind == "suspended"
    for name in ("a", "b", "c"):
        await f.signal(name, name.upper(), f"s-{name}")
    outcome = await f.drive()
    assert outcome.kind == "completed"
    assert outcome.value == "ABC"


async def test_gather_over_mixed_future_kinds_completes() -> None:
    async def body(ctx: Ctx, _input: Any) -> str:
        child, slept, sig = await asyncio.gather(
            ctx.start_call(CHILD, 7),
            ctx.start_sleep("t", 100),
            ctx.start_wait("sig"),
        )
        return f"{child}-{'slept' if slept is None else slept}-{sig}"

    f = await _seeded(define_workflow("w", body))
    assert (await f.drive()).kind == "suspended"
    assert f.scheduler.wakes == [(1, TS + 100)]
    await f.resolve(0, value=7)
    await f.resolve(1)
    await f.signal("sig", "S", "s1")
    outcome = await f.drive()
    assert outcome.kind == "completed"
    assert outcome.value == "7-slept-S"


async def test_an_infra_failure_while_arming_a_branch_retries_the_run() -> None:
    async def body(ctx: Ctx, _input: Any) -> str:
        await ctx.start_call(CHILD, 1)
        return "x"

    f = await _seeded(define_workflow("w", body))
    f.children = _Children(fail=True)
    outcome = await f.drive()
    # Infrastructure retries; only workflow logic fails a run.
    assert outcome.kind == "retry"
    assert "fail" not in await f.kinds()


async def test_a_replay_that_drops_a_fan_out_branch_fails_loud() -> None:
    f = Fan(define_workflow("w", _child_body))
    await f.start_entry()
    # A journal in which TWO children ran (commands seq 0 + 1, both resolved).
    for seq in (0, 1):
        await f.append(
            stamp(
                {
                    "kind": "call",
                    "seq": seq,
                    "name": "child",
                    "childRunId": f"{RUN}_{seq}",
                    "wf": "child",
                    "input": seq,
                },
                TS,
            ),
            MsgId.command(RUN, seq),
        )
        await f.resolve(seq, value=seq)

    async def shrunk(ctx: Ctx, _input: Any) -> str:
        await ctx.start_call(CHILD, 0)  # only ONE branch — a shorter path than the journal records
        return "done"

    f.workflow = define_workflow("w", shrunk)
    outcome = await f.drive()
    assert outcome.kind == "failed"
    assert outcome.error is not None
    assert outcome.error["name"] == "NonDeterminismError"


async def test_a_discarded_future_is_armed_before_the_outcome_is_reported() -> None:
    """Discarding a future is misuse — pinned so the sharp edge stays visible.

    It does not block the run and its result is ignored. What is NOT left to chance is the arming:
    ``flush_appends`` drains the branch's side effect too, so by the time the worker acks, the
    command is journaled and the child started. The slow seam is the control — without the drain the
    executor returns first and teardown abandons the half-armed branch.
    """

    async def body(ctx: Ctx, _input: Any) -> str:
        ctx.start_call(CHILD, 1)  # never awaited
        return "done"

    f = await _seeded(define_workflow("w", body))
    f.children = _Children(delay_s=0.02)
    outcome = await f.drive()
    assert outcome.kind == "completed"
    assert outcome.value == "done"  # the run does NOT block on the discarded future
    assert len(f.children.started) == 1, "the child was started before the outcome was reported"
    assert "call" in await f.kinds(), "and its command was journaled"


async def test_a_suspended_run_waits_for_an_in_flight_branch_to_be_journaled() -> None:
    """LEARNINGS #37, in Python: a suspend acks, and an acked work item never comes back.

    One branch suspends the run while a sibling's command append is still in flight. If the executor
    reported the suspension first, that intent could be lost to a crash straight after the ack —
    with nothing left to redeliver it, and a parent parked forever on a child that never started.
    """

    async def body(ctx: Ctx, _input: Any) -> Any:
        # Issue order matters here: the suspending branch's own append has to be AHEAD of the slow
        # one in the chain, or it would simply queue behind it and there would be no window at all.
        waiting = ctx.start_wait("x")  # seq 0 — arms fast, then suspends the run
        slow = ctx.start_call(CHILD, 1)  # seq 1 — its append is still in flight at that point
        await waiting
        return await slow

    journal = _SlowJournal("call")
    f = Fan(define_workflow("w", body), journal)
    await f.start_entry()
    outcome = await f.drive()
    # No await between the executor returning and this read: that is the whole assertion.
    assert journal.landed, "the sibling's intent must land BEFORE the suspension is reported"
    assert outcome.kind == "suspended"
    assert len(await f.at("call", 1)) == 1
    assert [c.parent.seq for c in f.children.started] == [1], "and its child was started"


async def test_a_suspended_run_waits_for_an_in_flight_child_start() -> None:
    """The same window one layer out: the command is journaled but the child start is still in flight.

    Only the command append rides the chain, so draining the chain alone would leave this half of the
    branch racing the ack. The arm itself is drained too (LEARNINGS #48).
    """

    async def body(ctx: Ctx, _input: Any) -> Any:
        slow = ctx.start_call(CHILD, 1)
        await ctx.start_wait("x")
        return await slow

    f = await _seeded(define_workflow("w", body))
    f.children = _Children(delay_s=0.02)
    outcome = await f.drive()
    assert outcome.kind == "suspended"
    assert [c.parent.seq for c in f.children.started] == [0]


async def test_a_discarded_branch_that_diverges_leaks_nothing() -> None:
    """A divergence inside an arm nobody awaits must not become a stray log line at GC.

    ``asyncio.wait`` does not retrieve a task's exception, so draining is not enough on its own: the
    engine reaps the finished arms, and the loop's exception handler stays silent. The run itself is
    unaffected — a branch nobody consumed is not part of the outcome.
    """
    seen: list[str] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: seen.append(str(context.get("message"))))
    try:

        async def body(ctx: Ctx, _input: Any) -> str:
            ctx.start_sleep("renamed", 1000)  # diverges from the recorded name, and is discarded
            return "done"

        f = await _seeded(define_workflow("w", body))
        await f.append(
            stamp({"kind": "sleep", "seq": 0, "name": "recorded", "dueMs": TS + 1}, TS),
            MsgId.command(RUN, 0),
        )
        outcome = await f.drive()
        del f
        gc.collect()
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(previous)

    assert outcome.kind == "completed"
    assert outcome.value == "done"
    assert seen == [], f"an arm's failure leaked to the event loop: {seen}"


async def test_the_command_seq_is_assigned_when_a_future_is_issued() -> None:
    """Issue order fixes ``seq``; await order cannot move it (``docs/DESIGN-python-sdk.md`` §3.2)."""

    async def body(ctx: Ctx, _input: Any) -> str:
        first = ctx.start_wait("a")  # seq 0
        second = ctx.start_wait("b")  # seq 1
        b = await second  # awaited FIRST — this must not renumber anything
        a = await first
        return f"{a}{b}"

    f = await _seeded(define_workflow("w", body))
    assert (await f.drive()).kind == "suspended"
    # Both commands are journaled after one pass, even though only one was awaited: arming is eager.
    assert [e["name"] for e in await f.at("await", 0)] == ["a"]
    assert [e["name"] for e in await f.at("await", 1)] == ["b"]

    await f.signal("a", "A", "sa")
    await f.signal("b", "B", "sb")
    outcome = await f.drive()
    assert outcome.kind == "completed"
    assert outcome.value == "AB"


async def test_a_wide_fan_out_journals_every_branch_exactly_once() -> None:
    width = 12

    async def body(ctx: Ctx, _input: Any) -> int:
        values = await asyncio.gather(*(ctx.start_call(CHILD, i) for i in range(width)))
        return sum(int(v) for v in values)

    f = await _seeded(define_workflow("w", body))
    assert (await f.drive()).kind == "suspended"
    assert sorted(c.parent.seq for c in f.children.started) == list(range(width))
    assert sorted(c.child_run_id for c in f.children.started) == sorted(
        f"{RUN}_{i}" for i in range(width)
    )
    for seq in range(width):
        assert len(await f.at("call", seq)) == 1, f"branch {seq} journaled more than once"
        await f.resolve(seq, value=seq)
    outcome = await f.drive()
    assert outcome.kind == "completed"
    assert outcome.value == sum(range(width))


async def test_a_gathered_branch_cannot_swallow_a_halt() -> None:
    """The halt guarantee in fan-out shape: one branch halts while a sibling is still live.

    Python cannot promise that no user code runs — an abandoned coroutine is closed by the
    interpreter at GC, which runs its ``finally`` blocks anyway — so the guarantee is stated where it
    is enforceable: a workflow may OBSERVE the unwind (here, as the teardown's cancellation arriving
    inside ``gather``), but it cannot swallow the halt, journal anything after it, or change the
    outcome. See ``docs/DESIGN-python-sdk.md`` §3.1 and LEARNINGS #45.
    """
    reached: list[str] = []

    async def body(ctx: Ctx, _input: Any) -> str:
        try:
            await asyncio.gather(ctx.start_wait("x"), ctx.start_wait("y"))
        except BaseException:  # noqa: BLE001 — deliberately the broadest catch there is
            reached.append("caught")
            await ctx.run("after", lambda: "must never be journaled")
            reached.append("journaled")
            return "swallowed the suspension"
        reached.append("resumed")
        return "done"

    f = await _seeded(define_workflow("w", body))
    outcome = await f.drive()
    # The workflow "returned" a value from its except block. It changed nothing.
    assert outcome.kind == "suspended"
    assert outcome.value is None
    assert "result" not in await f.kinds()
    assert "step-begin" not in await f.kinds(), "nothing may be journaled past a halt"
    assert "journaled" not in reached, "the closed context refused the append"
    assert "resumed" not in reached, "a suspension is not a continuation"


# ---------------------------------------------------------------- ctx.race


async def test_race_resolves_the_journal_earliest_branch_not_the_first_listed() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.race([ctx.start_wait("x"), ctx.start_wait("y")])

    f = await _seeded(define_workflow("w", body))
    assert (await f.drive()).kind == "suspended"
    # "y" (branch 1) lands BEFORE "x" (branch 0) → y wins, despite x being first in the list.
    await f.signal("y", "Y", "sy")
    await f.signal("x", "X", "sx")
    outcome = await f.drive()
    assert outcome.kind == "completed"
    assert outcome.value == "Y"


async def test_race_against_a_timer_lets_the_timer_win() -> None:
    async def body(ctx: Ctx, _input: Any) -> str:
        won = await ctx.race([ctx.start_wait("sig"), ctx.start_sleep("t", 1000)])
        return str(won) if won is not None else "timed-out"

    f = await _seeded(define_workflow("w", body))
    assert (await f.drive()).kind == "suspended"
    await f.resolve(1)  # the timer fires; no signal ever arrives
    outcome = await f.drive()
    assert outcome.kind == "completed"
    assert outcome.value == "timed-out"


async def test_race_suspends_while_every_branch_is_pending() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.race([ctx.start_wait("a"), ctx.start_wait("b")])

    f = await _seeded(define_workflow("w", body))
    assert (await f.drive()).kind == "suspended"


async def test_an_empty_race_fails_the_run() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.race([])

    f = await _seeded(define_workflow("w", body))
    outcome = await f.drive()
    assert outcome.kind == "failed"
    assert outcome.error is not None
    assert "at least one future" in outcome.error["message"]


async def test_race_rejects_an_awaitable_that_is_not_a_durable_future() -> None:
    """Racing a plain coroutine would resolve on wall-clock order — not stable across a replay."""

    async def body(ctx: Ctx, _input: Any) -> Any:
        foreign: Any = asyncio.sleep(0)
        try:
            return await ctx.race([foreign])
        finally:
            foreign.close()

    f = await _seeded(define_workflow("w", body))
    outcome = await f.drive()
    assert outcome.kind == "failed"
    assert outcome.error is not None
    assert "durable futures" in outcome.error["message"]


async def test_race_surfaces_the_failure_of_the_journal_earliest_branch() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.race([ctx.start_call(CHILD, 1), ctx.start_call(CHILD, 2)])

    f = await _seeded(define_workflow("w", body))
    await f.drive()
    # Branch 0 FAILS first (lower stream-seq); branch 1 would have succeeded later.
    await f.resolve(0, error={"name": "Error", "message": "first-and-failed"})
    await f.resolve(1, value=2)
    outcome = await f.drive()
    assert outcome.kind == "failed"
    assert outcome.error is not None
    assert "first-and-failed" in outcome.error["message"]


async def test_race_over_a_single_future_suspends_then_resolves() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.race([ctx.start_wait("only")])

    f = await _seeded(define_workflow("w", body))
    assert (await f.drive()).kind == "suspended"
    await f.signal("only", "V", "s1")
    outcome = await f.drive()
    assert outcome.kind == "completed"
    assert outcome.value == "V"


async def test_a_losing_race_branch_keeps_its_command_and_resolves_later() -> None:
    """Losers are not cancelled: their commands stay journaled, and a later replay still reads them."""

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.race([ctx.start_wait("fast"), ctx.start_wait("slow")])

    f = await _seeded(define_workflow("w", body))
    await f.drive()
    await f.signal("fast", "F", "sf")
    assert (await f.drive()).value == "F"
    # The loser's command is still there, and a late signal for it changes nothing.
    assert [e["name"] for e in await f.at("await", 1)] == ["slow"]


# ---------------------------------------------------------------- gather(return_exceptions=True)
#
# The complement of a plain gather, and it turns on one distinction:
#   * an IN-BAND failure (a failed child, a timed-out wait) is a real exception — it is CAPTURED and
#     the workflow keeps running;
#   * an OUT-OF-BAND halt (pending → suspend, lost CAS → fenced) is a coroutine that never completes,
#     so there is nothing to capture and the halt still wins.


async def test_gather_return_exceptions_captures_a_failed_child() -> None:
    async def body(ctx: Ctx, _input: Any) -> list[str]:
        settled = await asyncio.gather(
            ctx.start_call(CHILD, 1), ctx.start_call(CHILD, 2), return_exceptions=True
        )
        return ["rejected" if isinstance(x, BaseException) else f"ok:{x}" for x in settled]

    f = await _seeded(define_workflow("w", body))
    assert (await f.drive()).kind == "suspended"
    await f.resolve(0, error={"name": "Error", "message": "boom"})
    await f.resolve(1, value=2)
    outcome = await f.drive()
    # Contrast the plain gather: the run COMPLETES, because the failure was captured in band.
    assert outcome.kind == "completed"
    assert outcome.value == ["rejected", "ok:2"]


async def test_a_pending_sibling_still_suspends_a_capturing_gather() -> None:
    async def body(ctx: Ctx, _input: Any) -> str:
        await asyncio.gather(
            ctx.start_call(CHILD, 1), ctx.start_call(CHILD, 2), return_exceptions=True
        )
        return "done"

    f = await _seeded(define_workflow("w", body))
    await f.drive()
    await f.resolve(0, error={"name": "Error", "message": "boom"})  # captured in band
    # Branch 1 is still pending: it returns the never-completing awaitable and halts out of band, so
    # the whole run suspends even though the sibling's failure was already "handled".
    assert (await f.drive()).kind == "suspended"


async def test_gather_return_exceptions_captures_a_timed_out_wait() -> None:
    async def body(ctx: Ctx, _input: Any) -> list[str]:
        settled = await asyncio.gather(
            ctx.start_wait("a", timeout_ms=1000),  # seq 0 — will time out
            ctx.start_wait("b"),  # seq 1 — will be signalled
            return_exceptions=True,
        )
        return ["timed-out" if isinstance(x, BaseException) else f"ok:{x}" for x in settled]

    f = await _seeded(define_workflow("w", body))
    assert (await f.drive()).kind == "suspended"
    assert f.scheduler.wakes == [(0, TS + 1000)]
    await f.resolve(0)  # the timer for "a" fires with no signal of that name → timeout
    await f.signal("b", 9, "sb")
    outcome = await f.drive()
    assert outcome.kind == "completed"
    assert outcome.value == ["timed-out", "ok:9"]


# ---------------------------------------------------------------- degenerate shapes


async def test_awaiting_the_same_future_twice_reads_the_same_value() -> None:
    async def body(ctx: Ctx, _input: Any) -> int:
        f: DurableFuture[Any] = ctx.start_wait("x")
        first = await f
        second = await f  # same command seq → same resolution, nothing consumed twice
        return int(first) + int(second)

    f = await _seeded(define_workflow("w", body))
    assert (await f.drive()).kind == "suspended"
    await f.signal("x", 5, "sx")  # exactly ONE signal...
    outcome = await f.drive()
    assert outcome.kind == "completed"
    assert outcome.value == 10  # ...read twice


async def test_a_future_awaited_by_two_branches_at_once_resolves_once() -> None:
    async def body(ctx: Ctx, _input: Any) -> list[Any]:
        shared: DurableFuture[Any] = ctx.start_wait("x")
        both = await asyncio.gather(_read(shared), _read(shared))
        return list(both)

    async def _read(f: DurableFuture[Any]) -> Any:
        return await f

    f = await _seeded(define_workflow("w", body))
    assert (await f.drive()).kind == "suspended"
    assert len(await f.at("await", 0)) == 1, "one future, one command"
    await f.signal("x", "V", "sx")
    outcome = await f.drive()
    assert outcome.kind == "completed"
    assert outcome.value == ["V", "V"]


# ---------------------------------------------------------------- cancellation × fan-out
#
# Cancellation is an uncatchable hard-stop at the next COMMAND boundary. Under fan-out the boundary
# is whichever branch is still pending, and which branches count is decided by stream-seq: a resolve
# that landed before the `cancel-requested` still delivers its value, one that landed after is frozen
# out. Two runs with the same journal ENTRIES and a different ORDER get different outcomes, which is
# exactly why this is journaled rather than remembered.


async def _sum_of_two_children(ctx: Ctx, _input: Any) -> Any:
    a, b = await asyncio.gather(ctx.start_call(CHILD, 1), ctx.start_call(CHILD, 2))
    return int(a) + int(b)


async def test_a_cancel_hard_stops_a_run_parked_on_a_fan_out() -> None:
    f = await _seeded(define_workflow("w", _sum_of_two_children))
    assert (await f.drive()).kind == "suspended"  # both children started, both pending
    await f.cancel_requested()
    outcome = await f.drive()
    assert outcome.kind == "cancelled"


async def test_a_fan_out_resolved_before_the_cancel_completes_anyway() -> None:
    """No frontier, no stop point: the workflow returns and the cancel never lands (PROTOCOL §11.7)."""
    f = await _seeded(define_workflow("w", _sum_of_two_children))
    await f.call_command(0)
    await f.call_command(1)
    await f.resolve(0, value=10)  # both resolves land...
    await f.resolve(1, value=20)
    await f.cancel_requested()  # ...BEFORE the cancel-requested
    outcome = await f.drive()
    assert outcome.kind == "completed"
    assert outcome.value == 30, "both pre-cancel resolves were admitted"


async def test_a_pending_sibling_is_the_cancel_frontier() -> None:
    async def body(ctx: Ctx, _input: Any) -> str:
        await asyncio.gather(ctx.start_call(CHILD, 1), ctx.start_wait("go"))
        return "done"

    f = await _seeded(define_workflow("w", body))
    await f.call_command(0)
    await f.await_command(1, "go")
    await f.resolve(0, value=10)  # branch 0 resolved before the cancel
    await f.cancel_requested()  # branch 1 never signalled → pending → the hard-stop
    assert (await f.drive()).kind == "cancelled"


async def test_a_resolve_that_landed_after_the_cancel_is_frozen_out_of_a_fan_out() -> None:
    """Same workflow and the same entries as the completing case — only the ORDER differs."""
    f = await _seeded(define_workflow("w", _sum_of_two_children))
    await f.call_command(0)
    await f.call_command(1)
    await f.resolve(0, value=10)  # branch 0 beat the cancel
    await f.cancel_requested()
    await f.resolve(1, value=20)  # branch 1 did not → frozen out → reads as pending
    assert (await f.drive()).kind == "cancelled"


async def test_a_cancelled_fan_out_starts_no_further_children() -> None:
    """A run being killed must not spawn work: fresh branches hard-stop before arming anything."""

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await asyncio.gather(*(ctx.start_call(CHILD, n) for n in range(3)))

    f = await _seeded(define_workflow("w", body))
    await f.cancel_requested()
    outcome = await f.drive()
    assert outcome.kind == "cancelled"
    assert f.children.started == []
    assert "call" not in await f.kinds(), "no intent is journaled past a cancel"
