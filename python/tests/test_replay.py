"""The replay engine — pure, no server, no timing.

This is the file that decides whether the engine is correct. Everything here runs against
``InMemoryJournal``, which is exactly why the core has no I/O in it: crash, resume, fencing,
divergence and cancellation are all reachable in-process, in milliseconds, deterministically.

Mirrors the intent of ``test/unit/replay.test.ts`` — the reference implementation's crown jewel.
"""

from __future__ import annotations

from typing import Any

import pytest

from synadia_durable.core.children import ChildStart
from synadia_durable.core.execute import ExecOutcome, execute_run
from synadia_durable.core.workflow import define_workflow
from synadia_durable.errors import (
    DurableTimeoutError,
    FencedError,
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
from synadia_durable.journal.store import InMemoryJournal, JournalAppend
from synadia_durable.types import Ctx, RetryPolicy, Workflow

TS = 1781667505735
RUN = "r1"


async def _child_body(_ctx: Any, _input: Any) -> Any:
    """A child workflow's body, never executed here.

    ``ctx.call`` only needs the child's ``(name, version)`` — the parent never runs it, a worker
    picking up the child's own work item does. Raising makes an accidental in-parent execution loud.
    """
    raise AssertionError("a child workflow body must not run inside its parent")


class RecordingScheduler:
    """A Scheduler that records what was armed instead of arming anything."""

    def __init__(self, fail: bool = False) -> None:
        self.wakes: list[tuple[str, int, int]] = []
        self.fail = fail

    async def schedule_wake(self, run_id: str, workflow: str, seq: int, due_ms: int) -> None:  # noqa: ARG002 — `workflow` is part of the seam's signature
        if self.fail:
            raise ConnectionError("scheduler unavailable")
        self.wakes.append((run_id, seq, due_ms))


class RecordingChildren:
    """A Children that records the starts it was asked for."""

    def __init__(self, fail: bool = False) -> None:
        self.started: list[ChildStart] = []
        self.fail = fail

    async def start(self, child: ChildStart) -> None:
        if self.fail:
            raise ConnectionError("cannot reach the work queue")
        self.started.append(child)


def before_terminal(loaded: list[LoadedEntry]) -> list[LoadedEntry]:
    """A finished run's journal minus its terminal (and anything after it)."""
    for i, le in enumerate(loaded):
        if le.entry["kind"] in TERMINAL_KINDS:
            return loaded[:i]
    return loaded


class Harness:
    """One run, its journal, and the seams — the shape a worker would assemble."""

    def __init__(
        self, workflow: Workflow[Any, Any], journal: InMemoryJournal | None = None
    ) -> None:
        self.workflow = workflow
        self.journal = journal or InMemoryJournal()
        self.scheduler = RecordingScheduler()
        self.children = RecordingChildren()
        self.clock_ms = TS

    def clock(self) -> int:
        return self.clock_ms

    async def append(self, entry: JournalEntry, msg_id: str) -> int:
        loaded = await self.journal.load(RUN)
        last = loaded[-1].stream_seq if loaded else 0
        result = await self.journal.append(RUN, JournalAppend(entry, msg_id, last))
        return result.stream_seq

    async def start_entry(self, input: Any = None, **extra: Any) -> None:
        body: dict[str, Any] = {
            "kind": "start",
            "seq": 0,
            "wf": self.workflow.name,
            "wfVersion": self.workflow.version,
            "input": input,
            **extra,
        }
        await self.append(stamp(body, TS), MsgId.start(RUN))  # type: ignore[arg-type]

    async def signal(self, name: str, data: Any, signal_id: str = "s1") -> int:
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

    async def cancel_requested(self, reason: str | None = None) -> int:
        body: dict[str, Any] = {"kind": "cancel-requested", "seq": NON_COMMAND_SEQ}
        if reason is not None:
            body["reason"] = reason
        return await self.append(stamp(body, TS), MsgId.cancel(RUN))  # type: ignore[arg-type]

    async def drive(
        self, attempt: int = 1, deps: dict[str, Any] | None = None, *, replay: bool = False
    ) -> ExecOutcome:
        """Drive the run once, as a worker would.

        ``replay=True`` hands the executor the history BEFORE the terminal: ``execute_run`` never
        executes a journal that already holds one (it reports the recorded outcome and runs
        nothing), so a test that wants to watch a FINISHED run replay has to say so. The replay
        then ends by losing the terminal CAS to the entry that is really there, and adopting it.
        """
        loaded = await self.journal.load(RUN)
        if replay:
            loaded = before_terminal(loaded)
        return await execute_run(
            RUN,
            workflow=self.workflow,
            journal=self.journal,
            loaded=loaded,
            scheduler=self.scheduler,
            children=self.children,
            clock=self.clock,
            attempt=attempt,
            dependencies=deps,
        )

    async def kinds(self) -> list[str]:
        return [le.entry["kind"] for le in await self.journal.load(RUN)]

    async def find(self, kind: str) -> dict[str, Any] | None:
        for le in await self.journal.load(RUN):
            if le.entry["kind"] == kind:
                return dict(le.entry)
        return None


async def run_once(workflow: Workflow[Any, Any], input: Any = None) -> tuple[Harness, ExecOutcome]:
    h = Harness(workflow)
    await h.start_entry(input)
    return h, await h.drive()


# ---------------------------------------------------------------- the happy path


async def test_a_workflow_runs_its_steps_and_completes() -> None:
    async def body(ctx: Ctx, input: dict[str, Any]) -> str:
        first = await ctx.run("upper", lambda: str(input["name"]).upper())
        second = await ctx.run("greet", lambda: f"hello {first}")
        return str(second)

    h, outcome = await run_once(define_workflow("greet", body), {"name": "ada"})
    assert outcome.kind == "completed"
    assert outcome.value == "hello ADA"
    assert await h.kinds() == [
        "start",
        "step-begin",
        "step-end",
        "step-begin",
        "step-end",
        "result",
    ]


async def test_a_step_records_its_idempotency_key() -> None:
    keys: list[str] = []

    async def body(ctx: Ctx, _input: Any) -> None:
        await ctx.run("one", lambda: keys.append(ctx.key()))
        await ctx.run("two", lambda: keys.append(ctx.key()))

    await run_once(define_workflow("keys", body))
    # `{runId}:{seq}` — stable across every re-run of that step, which is what makes it usable as a
    # provider-side idempotency key.
    assert keys == [f"{RUN}:0", f"{RUN}:1"]


async def test_ctx_key_outside_a_step_is_seq_zero_not_a_crash() -> None:
    seen: list[str] = []

    async def body(ctx: Ctx, _input: Any) -> None:
        seen.append(ctx.key())

    await run_once(define_workflow("k", body))
    assert seen == [f"{RUN}:0"]


# ---------------------------------------------------------------- replay


async def test_a_recorded_step_is_replayed_not_re_executed() -> None:
    runs: list[int] = []

    def effect() -> str:
        runs.append(1)
        return "value"

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("effect", effect)

    wf = define_workflow("replay", body)
    h = Harness(wf)
    await h.start_entry()
    first = await h.drive()
    assert first.kind == "completed"
    assert len(runs) == 1

    # Drive the SAME journal again — the terminal is already there, but the point is the step.
    second = await Harness(wf, h.journal).drive(replay=True)
    assert second.kind in ("completed", "fenced")
    assert len(runs) == 1, "a recorded step must never re-execute its function"


async def test_replay_returns_the_recorded_value_not_a_recomputed_one() -> None:
    counter = {"n": 0}

    async def body(ctx: Ctx, _input: Any) -> Any:
        def effect() -> int:
            counter["n"] += 1
            return counter["n"]

        first = await ctx.run("count", effect)
        await ctx.run("second", lambda: None)
        return first

    wf = define_workflow("recorded", body)
    h = Harness(wf)
    await h.start_entry()
    await h.drive()

    # A fresh execution over the same journal must see 1 — the recorded value — not 2.
    h2 = Harness(wf, h.journal)
    ctx_values: list[Any] = []

    async def observing(ctx: Ctx, _input: Any) -> Any:
        value = await ctx.run("count", effect_stub)
        ctx_values.append(value)
        await ctx.run("second", lambda: None)
        return value

    def effect_stub() -> int:
        counter["n"] += 100  # would be obvious if it ran
        return counter["n"]

    h2.workflow = define_workflow("recorded", observing)
    await h2.drive(replay=True)
    assert ctx_values == [1]
    assert counter["n"] == 1


async def test_a_step_that_began_but_never_ended_re_runs_its_function() -> None:
    """At-least-once effects: the begin→end window is exactly where a crash re-runs the step.

    This is the guarantee ``ctx.key()`` exists for, and it is NOT crash-only — a concurrent wake or
    an ack-timeout redelivery lands here too.
    """
    runs: list[int] = []

    def effect() -> str:
        runs.append(1)
        return "v"

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("effect", effect)

    wf = define_workflow("resume", body)
    h = Harness(wf)
    await h.start_entry()
    # Seed a step-begin with no step-end: a crash in the window.
    await h.append(
        stamp({"kind": "step-begin", "seq": 0, "name": "effect", "key": f"{RUN}:0"}, TS),
        MsgId.step_begin(RUN, 0),
    )
    outcome = await h.drive()
    assert outcome.kind == "completed"
    assert len(runs) == 1
    assert (await h.kinds()).count("step-begin") == 1  # not appended twice


# ---------------------------------------------------------------- determinism guards


async def test_a_renamed_step_fails_the_run_loudly() -> None:
    async def v1(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("charge", lambda: 1)

    async def v2(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("charge-card", lambda: 1)  # renamed under an in-flight run

    h = Harness(define_workflow("wf", v1))
    await h.start_entry()
    await h.append(
        stamp({"kind": "step-begin", "seq": 0, "name": "charge", "key": f"{RUN}:0"}, TS),
        MsgId.step_begin(RUN, 0),
    )
    h.workflow = define_workflow("wf", v2)
    outcome = await h.drive()
    assert outcome.kind == "failed"
    assert outcome.error is not None
    assert outcome.error["name"] == "NonDeterminismError"


async def test_a_different_command_kind_at_a_position_fails_the_run() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("thing", lambda: 1)  # journal recorded a `now` here

    h = Harness(define_workflow("wf", body))
    await h.start_entry()
    await h.append(stamp({"kind": "now", "seq": 0, "value": TS}, TS), MsgId.value(RUN, 0))
    outcome = await h.drive()
    assert outcome.kind == "failed"
    assert outcome.error is not None
    assert "recorded a" in outcome.error["message"]


async def test_a_shorter_replay_path_fails_even_though_nothing_mismatched() -> None:
    """The subtle one: every command that DID replay matched, but the workflow stopped early."""

    async def two_steps(ctx: Ctx, _input: Any) -> Any:
        await ctx.run("a", lambda: 1)
        return await ctx.run("b", lambda: 2)

    async def one_step(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("a", lambda: 1)

    h = Harness(define_workflow("wf", two_steps))
    await h.start_entry()
    await h.drive()

    h2 = Harness(define_workflow("wf", one_step), InMemoryJournal())
    # Rebuild the journal without the terminal, so the shorter path is what fails rather than the
    # already-recorded result.
    for le in await h.journal.load(RUN):
        if le.entry["kind"] == "result":
            continue
        await h2.append(le.entry, f"copy:{le.stream_seq}")
    outcome = await h2.drive()
    assert outcome.kind == "failed"
    assert outcome.error is not None
    assert "shorter path" in outcome.error["message"]


# ---------------------------------------------------------------- deterministic builtins


@pytest.mark.parametrize("kind", ["now", "random", "uuid"])
async def test_a_builtin_is_captured_once_and_replayed(kind: str) -> None:
    seen: list[Any] = []

    async def body(ctx: Ctx, _input: Any) -> Any:
        value = await getattr(ctx, kind)()
        seen.append(value)
        return value

    wf = define_workflow("builtin", body)
    h = Harness(wf)
    await h.start_entry()
    first = await h.drive()

    # Replay against the same journal (minus the terminal) must produce the identical value.
    h2 = Harness(wf, InMemoryJournal())
    for le in await h.journal.load(RUN):
        if le.entry["kind"] == "result":
            continue
        await h2.append(le.entry, f"copy:{le.stream_seq}")
    second = await h2.drive()

    assert first.value == second.value
    assert seen[0] == seen[1]


async def test_now_uses_the_injected_clock() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.now()

    h = Harness(define_workflow("clock", body))
    h.clock_ms = 42
    await h.start_entry()
    outcome = await h.drive()
    assert outcome.value == 42


# ---------------------------------------------------------------- sleep


async def test_sleep_suspends_and_arms_exactly_one_wake() -> None:
    async def body(ctx: Ctx, _input: Any) -> str:
        await ctx.sleep("settle", 60_000)
        return "woke"

    h = Harness(define_workflow("sleeper", body))
    await h.start_entry()
    outcome = await h.drive()

    assert outcome.kind == "suspended"
    assert h.scheduler.wakes == [(RUN, 0, TS + 60_000)]
    sleep_entry = await h.find("sleep")
    assert sleep_entry is not None
    assert sleep_entry["dueMs"] == TS + 60_000
    assert "result" not in await h.kinds()


async def test_a_resolved_sleep_resumes_the_run() -> None:
    async def body(ctx: Ctx, _input: Any) -> str:
        await ctx.sleep("settle", 60_000)
        return "woke"

    h = Harness(define_workflow("sleeper", body))
    await h.start_entry()
    await h.drive()
    await h.resolve(0)  # the timer fired

    outcome = await h.drive()
    assert outcome.kind == "completed"
    assert outcome.value == "woke"


async def test_replaying_a_pending_sleep_does_not_recompute_the_deadline() -> None:
    async def body(ctx: Ctx, _input: Any) -> None:
        await ctx.sleep("settle", 60_000)

    h = Harness(define_workflow("sleeper", body))
    await h.start_entry()
    await h.drive()
    h.clock_ms = TS + 30_000  # time passed before the redelivery

    await h.drive()
    # Both arms use the RECORDED deadline; a recomputed one would push the wake further out on
    # every redelivery and a run could sleep forever.
    assert {wake[2] for wake in h.scheduler.wakes} == {TS + 60_000}


async def test_a_scheduler_failure_retries_rather_than_failing_the_run() -> None:
    async def body(ctx: Ctx, _input: Any) -> None:
        await ctx.sleep("settle", 1000)

    h = Harness(define_workflow("sleeper", body))
    h.scheduler = RecordingScheduler(fail=True)
    await h.start_entry()
    outcome = await h.drive()
    assert outcome.kind == "retry"
    assert isinstance(outcome.cause, ConnectionError)


# ---------------------------------------------------------------- wait (signals)


async def test_wait_suspends_until_a_signal_arrives() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        decision = await ctx.wait("approval")
        return decision

    h = Harness(define_workflow("approver", body))
    await h.start_entry()
    assert (await h.drive()).kind == "suspended"
    assert h.scheduler.wakes == [], "a wait without a timeout must not arm a timer"

    await h.signal("approval", {"ok": True})
    outcome = await h.drive()
    assert outcome.kind == "completed"
    assert outcome.value == {"ok": True}


async def test_a_signal_that_arrived_before_the_wait_is_still_matched() -> None:
    # Ordering in the journal, not arrival time at the worker, is what decides.
    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.wait("early")

    h = Harness(define_workflow("early", body))
    await h.start_entry()
    await h.signal("early", "here first")
    outcome = await h.drive()
    assert outcome.kind == "completed"
    assert outcome.value == "here first"


async def test_same_named_signals_match_waits_fifo() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        first = await ctx.wait("tick")
        second = await ctx.wait("tick")
        return [first, second]

    h = Harness(define_workflow("fifo", body))
    await h.start_entry()
    await h.signal("tick", "one", "s1")
    await h.signal("tick", "two", "s2")
    outcome = await h.drive()
    assert outcome.kind == "completed"
    assert outcome.value == ["one", "two"]


async def test_a_timed_wait_arms_a_timer_and_times_out_catchably() -> None:
    async def body(ctx: Ctx, _input: Any) -> str:
        try:
            await ctx.wait("approval", timeout_ms=1000)
        except DurableTimeoutError:
            return "fell back"
        return "approved"

    h = Harness(define_workflow("timed", body))
    await h.start_entry()
    assert (await h.drive()).kind == "suspended"
    assert h.scheduler.wakes == [(RUN, 0, TS + 1000)]

    await h.resolve(0)  # the timeout timer fired
    outcome = await h.drive()
    # A timeout is control flow, not a failure: the workflow caught it and carried on.
    assert outcome.kind == "completed"
    assert outcome.value == "fell back"


async def test_a_signal_beats_a_later_timeout() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.wait("approval", timeout_ms=1000)

    h = Harness(define_workflow("race", body))
    await h.start_entry()
    await h.drive()
    await h.signal("approval", "yes")  # lands first...
    await h.resolve(0)  # ...then the timer fires anyway

    outcome = await h.drive()
    # Resolved by journal ORDER, so every replay picks the same winner.
    assert outcome.kind == "completed"
    assert outcome.value == "yes"


async def test_a_timeout_beats_a_later_signal() -> None:
    async def body(ctx: Ctx, _input: Any) -> str:
        try:
            return str(await ctx.wait("approval", timeout_ms=1000))
        except DurableTimeoutError:
            return "timed out"

    h = Harness(define_workflow("race2", body))
    await h.start_entry()
    await h.drive()
    await h.resolve(0)  # timer first
    await h.signal("approval", "too late")

    outcome = await h.drive()
    assert outcome.value == "timed out"


async def test_a_timed_out_wait_does_not_consume_a_signal_meant_for_the_next_one() -> None:
    """The subtle FIFO rule: a timed-out await consumes nothing, so it must not shift the match."""

    async def body(ctx: Ctx, _input: Any) -> Any:
        try:
            await ctx.wait("tick", timeout_ms=1000)
            first = "signal"
        except DurableTimeoutError:
            first = "timeout"
        second = await ctx.wait("tick")
        return [first, second]

    h = Harness(define_workflow("fifo2", body))
    await h.start_entry()
    await h.drive()
    await h.resolve(0)  # the first wait times out
    await h.signal("tick", "for the second")

    outcome = await h.drive()
    assert outcome.kind == "completed"
    assert outcome.value == ["timeout", "for the second"]


# ---------------------------------------------------------------- sub-workflows


async def test_call_starts_a_child_and_suspends() -> None:
    child = define_workflow("ship", _child_body)

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.call(child, {"order": "o-1"})

    h = Harness(define_workflow("parent", body))
    await h.start_entry()
    outcome = await h.drive()

    assert outcome.kind == "suspended"
    assert len(h.children.started) == 1
    started = h.children.started[0]
    assert started.child_run_id == f"{RUN}_0"  # deterministic: {parent}_{seq}
    assert started.workflow == "ship"
    assert started.parent.run_id == RUN
    assert started.parent.seq == 0


async def test_a_child_result_resumes_the_parent() -> None:
    child = define_workflow("ship", _child_body)

    async def body(ctx: Ctx, _input: Any) -> Any:
        result = await ctx.call(child, {})
        return {"shipped": result}

    h = Harness(define_workflow("parent", body))
    await h.start_entry()
    await h.drive()
    await h.resolve(0, value={"tracking": "TRK-1"})

    outcome = await h.drive()
    assert outcome.kind == "completed"
    assert outcome.value == {"shipped": {"tracking": "TRK-1"}}


async def test_a_failed_child_raises_a_catchable_error_in_the_parent() -> None:
    child = define_workflow("ship", _child_body)

    async def body(ctx: Ctx, _input: Any) -> str:
        try:
            await ctx.call(child, {})
        except WorkflowFailure:
            return "compensated"  # the basis for saga compensation
        return "shipped"

    h = Harness(define_workflow("parent", body))
    await h.start_entry()
    await h.drive()
    await h.resolve(0, error={"name": "WorkflowFailure", "message": "carrier refused"})

    outcome = await h.drive()
    assert outcome.kind == "completed"
    assert outcome.value == "compensated"


async def test_a_resolved_call_does_not_restart_the_child() -> None:
    child = define_workflow("ship", _child_body)

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.call(child, {})

    h = Harness(define_workflow("parent", body))
    await h.start_entry()
    await h.drive()
    assert len(h.children.started) == 1
    await h.resolve(0, value="done")

    await h.drive()
    assert len(h.children.started) == 1, "a finished child must not be re-started on replay"


async def test_a_child_start_failure_retries_rather_than_failing_the_parent() -> None:
    child = define_workflow("ship", _child_body)

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.call(child, {})

    h = Harness(define_workflow("parent", body))
    h.children = RecordingChildren(fail=True)
    await h.start_entry()
    outcome = await h.drive()
    assert outcome.kind == "retry"


async def test_calling_a_different_workflow_at_a_recorded_position_fails_loudly() -> None:
    ship = define_workflow("ship", _child_body)
    refund = define_workflow("refund", _child_body)

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.call(ship, {})

    h = Harness(define_workflow("parent", body))
    await h.start_entry()
    await h.drive()

    async def changed(ctx: Ctx, _input: Any) -> Any:
        return await ctx.call(refund, {})

    h.workflow = define_workflow("parent", changed)
    outcome = await h.drive()
    assert outcome.kind == "failed"
    assert outcome.error is not None
    assert outcome.error["name"] == "NonDeterminismError"


# ---------------------------------------------------------------- continue-as-new


async def test_continue_as_new_writes_a_continued_terminal_and_names_the_next_generation() -> None:
    async def body(ctx: Ctx, input: dict[str, Any]) -> Any:
        await ctx.continue_as_new({"cycle": input["cycle"] + 1})
        raise AssertionError("code after continue_as_new must never run")

    h = Harness(define_workflow("cron", body))
    await h.start_entry({"cycle": 1})
    outcome = await h.drive()

    assert outcome.kind == "continue"
    assert outcome.next is not None
    assert outcome.next.next_run_id == f"{RUN}_g1"
    assert outcome.next.generation == 1
    assert outcome.next.input == {"cycle": 2}
    assert outcome.next.version == 1
    assert "continued" in await h.kinds()


async def test_continue_as_new_can_migrate_the_chain_to_another_version() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        await ctx.continue_as_new({}, version=2)

    h = Harness(define_workflow("cron", body))
    await h.start_entry({})
    outcome = await h.drive()
    assert outcome.next is not None
    assert outcome.next.version == 2
    # Recorded on the entry too, so a crash mid-rollover re-drives to the SAME version.
    entry = await h.find("continued")
    assert entry is not None
    assert entry["version"] == 2


async def test_a_later_generation_keeps_the_chain_base() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        await ctx.continue_as_new({})

    h = Harness(define_workflow("cron", body))
    await h.start_entry({}, generation=3, base="chain")
    outcome = await h.drive()
    assert outcome.next is not None
    assert outcome.next.next_run_id == "chain_g4"
    assert outcome.next.base == "chain"


@pytest.mark.parametrize("bad", [0, -1, 1.5])
async def test_continue_as_new_rejects_a_bad_version(bad: Any) -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        await ctx.continue_as_new({}, version=bad)

    _, outcome = await run_once(define_workflow("cron", body))
    assert outcome.kind == "failed"


# ---------------------------------------------------------------- out-of-band control


async def test_user_code_cannot_swallow_a_suspension() -> None:
    """The property the whole halt design exists for.

    Python cannot promise "no user code runs" the way the reference implementation can — an
    abandoned coroutine is closed by the interpreter at GC, which runs ``finally`` blocks anyway. So
    the guarantee is stated where it bites: a workflow may *observe* the unwind, but it cannot
    swallow the halt, cannot journal anything after it, and cannot change the outcome.
    """
    reached: list[str] = []

    async def body(ctx: Ctx, _input: Any) -> str:
        try:
            await ctx.sleep("nap", 1000)
        except BaseException:  # noqa: BLE001 — deliberately the broadest catch there is
            reached.append("caught")
            return "swallowed the suspension"
        reached.append("resumed")
        return "continued past it"

    h = Harness(define_workflow("uncatchable", body))
    await h.start_entry()
    outcome = await h.drive()

    # The workflow "returned" a value from its except block. It changed nothing.
    assert outcome.kind == "suspended"
    assert outcome.value is None
    assert "result" not in await h.kinds()
    # And it never resumed the normal path — the suspension was not turned into a continuation.
    assert "resumed" not in reached


async def test_user_code_cannot_journal_after_the_outcome_is_committed() -> None:
    """The teardown's real guard: the context is closed before the workflow task is unwound."""
    errors: list[str] = []

    async def body(ctx: Ctx, _input: Any) -> None:
        try:
            await ctx.sleep("nap", 1000)
        except BaseException:  # noqa: BLE001
            try:
                await ctx.run("sneaky", lambda: "should never be journaled")
            except BaseException as e:  # noqa: BLE001
                errors.append(type(e).__name__)

    h = Harness(define_workflow("closed", body))
    await h.start_entry()
    outcome = await h.drive()

    assert outcome.kind == "suspended"
    assert "step-begin" not in await h.kinds(), "a closed context must not append"
    assert errors
    assert errors[0] in ("_Abandoned", "CancelledError")


async def test_a_workflow_that_blocks_on_cancellation_does_not_hang_the_executor() -> None:
    # A rogue workflow catching the teardown cancellation and sleeping must not hold the worker;
    # the executor gives up on it rather than waiting.
    import asyncio as _asyncio

    from synadia_durable.core import execute as execute_module

    async def body(ctx: Ctx, _input: Any) -> None:
        try:
            await ctx.sleep("nap", 1000)
        except BaseException:  # noqa: BLE001
            await _asyncio.sleep(30)

    original = execute_module.TEARDOWN_TIMEOUT_S
    execute_module.TEARDOWN_TIMEOUT_S = 0.05
    try:
        h = Harness(define_workflow("rogue", body))
        await h.start_entry()
        outcome = await _asyncio.wait_for(h.drive(), timeout=5)
    finally:
        execute_module.TEARDOWN_TIMEOUT_S = original
    assert outcome.kind == "suspended"


async def test_a_fence_stops_the_run_without_a_terminal() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("effect", lambda: "v")

    h = Harness(define_workflow("zombie", body))
    await h.start_entry()

    # Another worker advanced the journal between our load and our append.
    loaded = await h.journal.load(RUN)
    stale = list(loaded)
    await h.append(stamp({"kind": "now", "seq": 0, "value": 1}, TS), MsgId.value(RUN, 0))
    outcome = await execute_run(
        RUN,
        workflow=h.workflow,
        journal=h.journal,
        loaded=stale,  # the zombie's stale view
        scheduler=h.scheduler,
        children=h.children,
        clock=h.clock,
    )
    assert outcome.kind == "fenced"
    assert "result" not in await h.kinds()


async def test_a_zombie_is_fenced_before_its_side_effect_fires() -> None:
    """Intent-first is the point: a fresh zombie loses the begin-CAS and never runs ``fn``."""
    fired: list[int] = []

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("charge", lambda: fired.append(1))

    h = Harness(define_workflow("zombie", body))
    await h.start_entry()
    stale = await h.journal.load(RUN)
    await h.append(stamp({"kind": "now", "seq": 0, "value": 1}, TS), MsgId.value(RUN, 0))

    await execute_run(
        RUN,
        workflow=h.workflow,
        journal=h.journal,
        loaded=stale,
        scheduler=h.scheduler,
        children=h.children,
        clock=h.clock,
    )
    assert fired == [], "the effect must not fire after losing the intent CAS"


async def test_an_infrastructure_failure_retries_and_journals_nothing() -> None:
    class BrokenJournal(InMemoryJournal):
        async def append(self, run_id: str, append: JournalAppend) -> Any:
            if append.entry["kind"] == "step-begin":
                raise ConnectionError("publish failed")
            return await super().append(run_id, append)

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("effect", lambda: "v")

    h = Harness(define_workflow("infra", body), BrokenJournal())
    await h.start_entry()
    outcome = await h.drive()
    assert outcome.kind == "retry"
    assert isinstance(outcome.cause, ConnectionError)
    assert "fail" not in await h.kinds(), "infrastructure must never fail a run terminally"


async def test_an_unencodable_step_result_fails_the_run_instead_of_retrying_forever() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("bad", lambda: float("nan"))

    _, outcome = await run_once(define_workflow("unencodable", body))
    # Deterministic: the same bytes will never encode, so retrying is an infinite loop.
    assert outcome.kind == "failed"
    assert outcome.error is not None
    assert "SerializationError" in outcome.error["name"]


async def test_a_workflow_that_raises_fails_terminally() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        await ctx.run("ok", lambda: 1)
        raise ValueError("business rule violated")

    h, outcome = await run_once(define_workflow("boom", body))
    assert outcome.kind == "failed"
    assert outcome.error == {"name": "ValueError", "message": "business rule violated"}
    assert "fail" in await h.kinds()


async def test_ctx_check_fails_the_run_with_its_message() -> None:
    async def body(ctx: Ctx, _input: Any) -> None:
        ctx.check(False, "inventory went negative")

    _, outcome = await run_once(define_workflow("guard", body))
    assert outcome.kind == "failed"
    assert outcome.error is not None
    assert outcome.error["message"] == "inventory went negative"


# ---------------------------------------------------------------- nesting and dependencies


async def test_a_ctx_command_inside_a_step_fails_loudly() -> None:
    """Nesting would interleave command seqs: the run would complete once, then fail every replay."""

    async def body(ctx: Ctx, _input: Any) -> Any:
        async def inner() -> Any:
            return await ctx.now()  # illegal: a command inside a step

        return await ctx.run("outer", inner)

    _, outcome = await run_once(define_workflow("nested", body))
    assert outcome.kind == "failed"
    assert outcome.error is not None
    assert outcome.error["name"] == StepNestingError.__name__


async def test_ctx_key_and_get_are_allowed_inside_a_step() -> None:
    seen: dict[str, Any] = {}

    async def body(ctx: Ctx, _input: Any) -> None:
        def inner() -> None:
            seen["key"] = ctx.key()
            seen["dep"] = ctx.get("db")
            ctx.check(True)

        await ctx.run("inner", inner)

    h = Harness(define_workflow("allowed", body))
    await h.start_entry()
    outcome = await h.drive(deps={"db": "postgres://"})
    assert outcome.kind == "completed"
    assert seen == {"key": f"{RUN}:0", "dep": "postgres://"}


async def test_a_missing_dependency_fails_the_run_with_a_useful_message() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        return ctx.get("nope")

    _, outcome = await run_once(define_workflow("dep", body))
    assert outcome.kind == "failed"
    assert outcome.error is not None
    assert "nope" in outcome.error["message"]


# ---------------------------------------------------------------- step functions


async def test_a_sync_step_function_runs_off_the_event_loop() -> None:
    import threading

    loop_thread = threading.current_thread().ident
    observed: dict[str, Any] = {}

    async def body(ctx: Ctx, _input: Any) -> Any:
        def blocking() -> str:
            observed["thread"] = threading.current_thread().ident
            return "done"

        return await ctx.run("blocking", blocking)

    _, outcome = await run_once(define_workflow("sync", body))
    assert outcome.value == "done"
    # Blocking the loop would stall every other run on the worker AND the ack heartbeats.
    assert observed["thread"] != loop_thread


async def test_step_arguments_are_passed_through() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("add", lambda a, b: a + b, 2, b=3)

    _, outcome = await run_once(define_workflow("args", body))
    assert outcome.value == 5


async def test_a_retry_policy_retries_within_one_execution() -> None:
    attempts = {"n": 0}

    async def body(ctx: Ctx, _input: Any) -> Any:
        def flaky() -> str:
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise ConnectionError("503")
            return "ok"

        return await ctx.run("flaky", flaky, retry=RetryPolicy(attempts=5))

    _, outcome = await run_once(define_workflow("retry", body))
    assert outcome.kind == "completed"
    assert attempts["n"] == 3
    # One journaled step, three executions of its function — the §14 guarantee in miniature.


async def test_a_retry_policy_that_exhausts_fails_the_run() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        def always() -> str:
            raise ConnectionError("still down")

        return await ctx.run("flaky", always, retry=RetryPolicy(attempts=2))

    _, outcome = await run_once(define_workflow("retry2", body))
    assert outcome.kind == "failed"


async def test_attempt_is_visible_to_the_workflow() -> None:
    seen: list[int] = []

    async def body(ctx: Ctx, _input: Any) -> None:
        seen.append(ctx.attempt)

    h = Harness(define_workflow("attempt", body))
    await h.start_entry()
    await h.drive(attempt=4)
    assert seen == [4]


# ---------------------------------------------------------------- cancellation


async def test_a_journaled_cancel_hard_stops_at_the_next_fresh_command() -> None:
    fired: list[str] = []

    async def body(ctx: Ctx, _input: Any) -> str:
        await ctx.run("first", lambda: fired.append("first"))
        await ctx.run("second", lambda: fired.append("second"))
        return "done"

    h = Harness(define_workflow("cancellable", body))
    await h.start_entry()
    await h.drive()  # runs to completion normally? no — it completes; so seed differently

    # Fresh run: record only the first step, then a cancel.
    h2 = Harness(define_workflow("cancellable", body), InMemoryJournal())
    await h2.start_entry()
    await h2.append(
        stamp({"kind": "step-begin", "seq": 0, "name": "first", "key": f"{RUN}:0"}, TS),
        MsgId.step_begin(RUN, 0),
    )
    await h2.append(
        stamp({"kind": "step-end", "seq": 0, "name": "first", "value": None}, TS),
        MsgId.step_end(RUN, 0),
    )
    await h2.cancel_requested("operator")
    fired.clear()

    outcome = await h2.drive()
    assert outcome.kind == "cancelled"
    assert fired == [], "no new effect may fire once a cancel is journaled"
    assert "cancelled" in await h2.kinds()
    entry = await h2.find("cancelled")
    assert entry is not None
    assert entry["reason"] == "operator"


async def test_a_recorded_command_still_replays_under_a_cancel() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        value = await ctx.run("recorded", lambda: "should not re-run")
        await ctx.run("fresh", lambda: "never")
        return value

    h = Harness(define_workflow("c", body))
    await h.start_entry()
    await h.append(
        stamp({"kind": "step-begin", "seq": 0, "name": "recorded", "key": f"{RUN}:0"}, TS),
        MsgId.step_begin(RUN, 0),
    )
    await h.append(
        stamp({"kind": "step-end", "seq": 0, "name": "recorded", "value": "from the journal"}, TS),
        MsgId.step_end(RUN, 0),
    )
    await h.cancel_requested()
    outcome = await h.drive()
    # The recorded step replayed (it must — replay is how we get to the frontier); the FRESH one is
    # where the run stops.
    assert outcome.kind == "cancelled"


async def test_completion_wins_when_no_stop_point_is_reached() -> None:
    """A run that returns without needing anything new completes, even under a cancel."""

    async def body(ctx: Ctx, _input: Any) -> str:
        return str(await ctx.run("only", lambda: "value"))  # already recorded below

    h = Harness(define_workflow("racer", body))
    await h.start_entry()
    await h.append(
        stamp({"kind": "step-begin", "seq": 0, "name": "only", "key": f"{RUN}:0"}, TS),
        MsgId.step_begin(RUN, 0),
    )
    await h.append(
        stamp({"kind": "step-end", "seq": 0, "name": "only", "value": "value"}, TS),
        MsgId.step_end(RUN, 0),
    )
    await h.cancel_requested()

    outcome = await h.drive()
    assert outcome.kind == "completed"
    assert outcome.value == "value"


async def test_a_step_that_began_but_never_ended_does_not_re_run_under_a_cancel() -> None:
    # Whether the effect already fired is the same uncertainty as any crash; re-running it for a run
    # being killed is the one thing we can decide NOT to do.
    fired: list[int] = []

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("charge", lambda: fired.append(1))

    h = Harness(define_workflow("c", body))
    await h.start_entry()
    await h.append(
        stamp({"kind": "step-begin", "seq": 0, "name": "charge", "key": f"{RUN}:0"}, TS),
        MsgId.step_begin(RUN, 0),
    )
    await h.cancel_requested()

    outcome = await h.drive()
    assert outcome.kind == "cancelled"
    assert fired == []


async def test_on_cancel_handlers_run_lifo_before_the_terminal() -> None:
    order: list[str] = []

    async def body(ctx: Ctx, _input: Any) -> None:
        ctx.on_cancel(lambda: order.append("first-registered"))
        ctx.on_cancel(lambda: order.append("second-registered"))
        await ctx.sleep("nap", 1000)

    h = Harness(define_workflow("compensating", body))
    await h.start_entry()
    await h.cancel_requested()
    outcome = await h.drive()

    assert outcome.kind == "cancelled"
    assert order == ["second-registered", "first-registered"]


async def test_a_failing_on_cancel_handler_never_blocks_the_cancel() -> None:
    async def body(ctx: Ctx, _input: Any) -> None:
        ctx.on_cancel(lambda: (_ for _ in ()).throw(RuntimeError("compensation failed")))
        await ctx.sleep("nap", 1000)

    h = Harness(define_workflow("c", body))
    await h.start_entry()
    await h.cancel_requested()
    outcome = await h.drive()
    assert outcome.kind == "cancelled"


async def test_continue_as_new_under_a_cancel_cancels_in_place() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        await ctx.continue_as_new({})

    h = Harness(define_workflow("cron", body))
    await h.start_entry({})
    await h.cancel_requested()
    outcome = await h.drive()
    # The next generation is never started — cancelling a chain must actually stop it.
    assert outcome.kind == "cancelled"
    assert "continued" not in await h.kinds()


async def test_a_resolve_that_landed_before_the_cancel_still_delivers_its_value() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        value = await ctx.wait("approval")
        await ctx.run("after", lambda: "fresh")
        return value

    h = Harness(define_workflow("c", body))
    await h.start_entry()
    await h.drive()  # parks on the wait
    await h.signal("approval", "delivered")
    await h.cancel_requested()

    outcome = await h.drive()
    # The signal landed BEFORE the cancel, so the wait resolves; the run then stops at the next
    # fresh command rather than pretending the signal never arrived.
    assert outcome.kind == "cancelled"


async def test_a_resolve_that_landed_after_the_cancel_is_frozen_out() -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.wait("approval")

    h = Harness(define_workflow("c", body))
    await h.start_entry()
    await h.drive()
    await h.cancel_requested()
    await h.signal("approval", "too late")

    outcome = await h.drive()
    assert outcome.kind == "cancelled"


# ---------------------------------------------------------------- terminal reconciliation


async def test_a_terminal_cas_loser_adopts_the_journaled_outcome() -> None:
    """The journal is the truth, so the loser reports the WINNER's value — not its own."""

    async def body(_ctx: Ctx, _input: Any) -> str:
        return "mine"

    h = Harness(define_workflow("t", body))
    await h.start_entry()
    stale = await h.journal.load(RUN)
    # Another worker finalized the run first, with a different value.
    await h.append(
        stamp({"kind": "result", "seq": NON_COMMAND_SEQ, "value": "theirs"}, TS),
        MsgId.terminal(RUN) + ":other",
    )

    outcome = await execute_run(
        RUN,
        workflow=h.workflow,
        journal=h.journal,
        loaded=stale,
        scheduler=h.scheduler,
        children=h.children,
        clock=h.clock,
    )
    assert outcome.kind == "completed"
    assert outcome.value == "theirs"


async def test_a_run_records_at_most_one_terminal() -> None:
    async def body(_ctx: Ctx, _input: Any) -> str:
        return "value"

    h = Harness(define_workflow("t", body))
    await h.start_entry()
    await h.drive()
    await h.drive()  # a redelivered work item re-drives the same run
    kinds = await h.kinds()
    assert kinds.count("result") == 1


async def test_a_fenced_step_end_adopts_the_canonical_value() -> None:
    """Two workers ran the same frontier step; the CAS picks one, and the loser must adopt it."""

    class RacingJournal(InMemoryJournal):
        def __init__(self) -> None:
            super().__init__()
            self.injected = False

        async def append(self, run_id: str, append: JournalAppend) -> Any:
            if append.entry["kind"] == "step-end" and not self.injected:
                # A competitor lands its step-end for the same seq first.
                self.injected = True
                loaded = await self.load(run_id)
                last = loaded[-1].stream_seq if loaded else 0
                await super().append(
                    run_id,
                    JournalAppend(
                        stamp(
                            {"kind": "step-end", "seq": 0, "name": "effect", "value": "theirs"}, TS
                        ),
                        "competitor:end",
                        last,
                    ),
                )
            return await super().append(run_id, append)

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("effect", lambda: "mine")

    h = Harness(define_workflow("race", body), RacingJournal())
    await h.start_entry()
    outcome = await h.drive()

    assert outcome.kind == "completed"
    assert outcome.value == "theirs", "the journal's value wins, not this execution's"


async def test_the_engine_never_appends_after_the_outcome_is_committed() -> None:
    # The context closes once the executor decides; a late ctx call from an unwinding frame must be
    # inert rather than journaling into a finalized run.
    async def body(_ctx: Ctx, _input: Any) -> str:
        return "done"

    h = Harness(define_workflow("closed", body))
    await h.start_entry()
    await h.drive()
    before = await h.kinds()
    assert before.count("result") == 1
    assert before[-1] == "result"


async def test_an_absent_start_entry_is_an_invariant_violation_not_a_crash() -> None:
    from synadia_durable._internal.invariant import InvariantViolation

    async def body(_ctx: Ctx, _input: Any) -> None:
        return None

    with pytest.raises(InvariantViolation):
        await execute_run(
            RUN,
            workflow=define_workflow("x", body),
            journal=InMemoryJournal(),
            loaded=[],
        )


async def test_the_seams_fail_loudly_when_not_wired() -> None:
    # A workflow that sleeps with no scheduler would otherwise park forever with nothing to wake it
    # — the worst failure mode, because it looks like patience.
    async def body(ctx: Ctx, _input: Any) -> None:
        await ctx.sleep("nap", 1000)

    h = Harness(define_workflow("unwired", body))
    await h.start_entry()
    loaded = await h.journal.load(RUN)
    outcome = await execute_run(
        RUN, workflow=h.workflow, journal=h.journal, loaded=loaded, clock=h.clock
    )
    assert outcome.kind == "retry"  # infrastructure, so it redelivers rather than failing the run


async def test_a_fence_is_never_raised_through_user_code() -> None:
    # A fence is transient plumbing. A workflow author never sees one — it is an outcome, not an
    # exception their code can meet.
    seen: list[str] = []

    async def body(ctx: Ctx, _input: Any) -> Any:
        try:
            return await ctx.run("effect", lambda: "v")
        except FencedError:  # pragma: no cover — the point is that this is unreachable
            seen.append("caught a fence")
            return "recovered"

    h = Harness(define_workflow("nofence", body))
    await h.start_entry()
    stale = await h.journal.load(RUN)
    await h.append(stamp({"kind": "now", "seq": 0, "value": 1}, TS), MsgId.value(RUN, 0))
    outcome = await execute_run(
        RUN,
        workflow=h.workflow,
        journal=h.journal,
        loaded=stale,
        scheduler=h.scheduler,
        children=h.children,
        clock=h.clock,
    )
    assert outcome.kind == "fenced"
    assert seen == []


async def test_a_continued_cas_loser_adopts_the_canonical_rollover() -> None:
    """The adopt arm with the most to get wrong, because a `continued` terminal names the NEXT run.

    ``nextRunId`` is deterministic, but the rollover INPUT and target VERSION come from whichever
    incarnation computed them, and two racing the boundary can differ. A loser that reported its own
    spec would have the worker start the next generation from a value the journal does not record —
    the journal quietly ceasing to be the source of truth for the chain.
    """

    async def body(ctx: Ctx, _input: Any) -> Any:
        await ctx.continue_as_new({"attempt": "mine"})

    h = Harness(define_workflow("t", body))
    await h.start_entry()
    stale = await h.journal.load(RUN)
    # Another worker rolled the run over first, with a different input and a different version.
    await h.append(
        stamp(
            {
                "kind": "continued",
                "seq": NON_COMMAND_SEQ,
                "nextRunId": f"{RUN}_g1",
                "input": {"attempt": "theirs"},
                "generation": 1,
                "base": RUN,
                "version": 7,
            },
            TS,
        ),
        MsgId.terminal(RUN) + ":other",
    )

    outcome = await execute_run(
        RUN,
        workflow=h.workflow,
        journal=h.journal,
        loaded=stale,
        scheduler=h.scheduler,
        children=h.children,
        clock=h.clock,
    )
    assert outcome.kind == "continue"
    assert outcome.next is not None
    assert outcome.next.input == {"attempt": "theirs"}  # adopted, not "mine"
    assert outcome.next.version == 7
    assert outcome.next.next_run_id == f"{RUN}_g1"
    assert (await h.kinds()).count("continued") == 1
