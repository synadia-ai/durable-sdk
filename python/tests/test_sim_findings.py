"""What the deterministic simulation of the TypeScript core FOUND, pinned in the port.

The simulator (``test/harness/sim``, ``test/unit/simulate.test.ts``) drives the reference engine;
the engine here is a port of it, and every defect it found was checked against this tree by
rebuilding the interleaving by hand. Mirrors ``test/unit/sim-findings.test.ts`` — each test forces
its race with a journal that misbehaves at a chosen append, and no scheduler at all.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from synadia_durable.core.execute import execute_run
from synadia_durable.core.workflow import define_workflow
from synadia_durable.journal.entry import (
    NON_COMMAND_SEQ,
    TERMINAL_KINDS,
    JournalEntry,
    MsgId,
    stamp,
)
from synadia_durable.journal.store import AppendResult, InMemoryJournal, Journal, JournalAppend
from synadia_durable.types import Ctx, Workflow

TS = 1781667505735


async def _append(journal: Journal, run_id: str, entry: JournalEntry, msg_id: str) -> None:
    loaded = await journal.load(run_id)
    last = loaded[-1].stream_seq if loaded else 0
    await journal.append(run_id, JournalAppend(entry, msg_id, last))


async def _start(journal: Journal, run_id: str) -> None:
    await _append(
        journal,
        run_id,
        stamp({"kind": "start", "seq": 0, "wf": "w", "wfVersion": 1, "input": {}}, TS),
        MsgId.start(run_id),
    )


async def _kinds(journal: Journal, run_id: str) -> list[str]:
    return [le.entry["kind"] for le in await journal.load(run_id)]


async def _drive(journal: Journal, run_id: str, workflow: Workflow[Any, Any]) -> Any:
    return await execute_run(
        run_id, workflow=workflow, journal=journal, loaded=await journal.load(run_id)
    )


class WithoutDedup:
    """A journal whose duplicate window has EXPIRED: every msgID is unique, so nothing but the CAS
    decides what lands. "One terminal per run" must hold here too — dedup is a convenience."""

    def __init__(self, inner: Journal) -> None:
        self.inner = inner
        self.n = 0

    async def load(self, run_id: str) -> Any:
        return await self.inner.load(run_id)

    async def append(self, run_id: str, append: JournalAppend) -> AppendResult:
        self.n += 1
        salted = JournalAppend(append.entry, f"{append.msg_id}~{self.n}", append.expect_last_seq)
        return await self.inner.append(run_id, salted)


# ------------------------------------------------ a journal that holds a terminal is never executed


async def test_a_cancel_journaled_after_continued_leaves_the_rollover_standing() -> None:
    journal = WithoutDedup(InMemoryJournal())
    await _start(journal, "t1")
    await _append(
        journal,
        "t1",
        stamp(
            {
                "kind": "continued",
                "seq": NON_COMMAND_SEQ,
                "nextRunId": "t1_g1",
                "input": {"n": 7},
                "generation": 1,
                "base": "t1",
                "version": 1,
            },
            TS,
        ),
        MsgId.terminal("t1"),
    )
    await _append(
        journal, "t1", stamp({"kind": "cancel-requested", "seq": NON_COMMAND_SEQ}, TS), "t1:cancel"
    )
    seen = {"invoked": 0, "compensated": 0}

    async def undo() -> int:
        seen["compensated"] += 1
        return 1

    async def body(ctx: Ctx, _input: Any) -> Any:
        seen["invoked"] += 1
        ctx.on_cancel(lambda: ctx.run("undo", undo))
        await ctx.continue_as_new({"n": 8})  # a different input than the journal's, on purpose

    out = await _drive(journal, "t1", define_workflow("w", body))
    # Before the fix: `cancelled` (continue_as_new cancels in place under a journaled cancel), the
    # compensation fired, and a second terminal landed after the first.
    assert out.kind == "continue"
    assert out.next is not None
    assert (out.next.next_run_id, out.next.input) == ("t1_g1", {"n": 7})
    assert seen == {"invoked": 0, "compensated": 0}
    assert await _kinds(journal, "t1") == ["start", "continued", "cancel-requested"]


async def test_the_step_end_reconcile_adopting_a_finished_journal_writes_no_second_terminal() -> (
    None
):
    inner = InMemoryJournal()
    await _start(inner, "t2")
    await _append(
        inner,
        "t2",
        stamp({"kind": "step-begin", "seq": 0, "name": "a", "key": "t2:0"}, TS),
        MsgId.step_begin("t2", 0),
    )

    class Racing(WithoutDedup):
        """Just before OUR step-end lands, another incarnation completes the step AND the run."""

        raced = False

        async def append(self, run_id: str, append: JournalAppend) -> AppendResult:
            if append.entry["kind"] == "step-end" and not self.raced:
                self.raced = True
                await _append(
                    inner,
                    "t2",
                    stamp({"kind": "step-end", "seq": 0, "name": "a", "value": "theirs"}, TS),
                    "other:e",
                )
                await _append(
                    inner,
                    "t2",
                    stamp({"kind": "result", "seq": NON_COMMAND_SEQ, "value": "their result"}, TS),
                    "other:t",
                )
            return await super().append(run_id, append)

    async def step() -> str:
        return "mine"

    async def body(ctx: Ctx, _input: Any) -> str:
        return f"mine, after {await ctx.run('a', step)}"

    out = await _drive(Racing(inner), "t2", define_workflow("w", body))
    # Before the fix: our terminal append passed its CAS (the adopted tail IS their terminal), so a
    # SECOND `result` landed; inside the window the duplicate ack read as "mine landed" instead.
    assert (out.kind, out.value) == ("completed", "their result")
    terminals = [k for k in await _kinds(inner, "t2") if k in TERMINAL_KINDS]
    assert terminals == ["result"]


# ------------------------------------------------ a halt inside on_cancel cleanup ends the execution


def _cancellable() -> Workflow[Any, Any]:
    async def undo() -> str:
        return "undone"

    async def work() -> str:
        return "never runs — the cancel is already journaled"

    async def body(ctx: Ctx, _input: Any) -> str:
        ctx.on_cancel(lambda: ctx.run("undo", undo))
        await ctx.run("work", work)
        return "done"

    return define_workflow("w", body)


async def _cancelled_run(run_id: str) -> InMemoryJournal:
    journal = InMemoryJournal()
    await _start(journal, run_id)
    await _append(
        journal,
        run_id,
        stamp({"kind": "cancel-requested", "seq": NON_COMMAND_SEQ}, TS),
        MsgId.cancel(run_id),
    )
    return journal


async def test_an_infrastructure_error_on_a_cleanup_step_is_a_retry_not_a_hang() -> None:
    inner = await _cancelled_run("h1")

    class FailsOnce:
        failed = False

        async def load(self, run_id: str) -> Any:
            return await inner.load(run_id)

        async def append(self, run_id: str, append: JournalAppend) -> AppendResult:
            if append.entry["kind"] == "step-begin" and not self.failed:
                self.failed = True
                raise ConnectionError("publish failed")
            return await inner.append(run_id, append)

    # Before the fix this never returned: the cleanup step awaited the never-future INSIDE the
    # handler run_cleanup was awaiting. One transient publish failure during a cancel.
    out = await asyncio.wait_for(_drive(FailsOnce(), "h1", _cancellable()), timeout=3)
    assert out.kind == "retry"
    assert not [k for k in await _kinds(inner, "h1") if k in TERMINAL_KINDS]
    # …and the redelivery finishes it: the cleanup step runs, then the terminal.
    again = await asyncio.wait_for(_drive(inner, "h1", _cancellable()), timeout=3)
    assert again.kind == "cancelled"
    assert await _kinds(inner, "h1") == [
        "start",
        "cancel-requested",
        "step-begin",
        "step-end",
        "cancelled",
    ]


async def test_a_lost_cas_on_a_cleanup_step_is_a_fence_not_a_hang() -> None:
    inner = await _cancelled_run("h2")

    class Racing:
        raced = False

        async def load(self, run_id: str) -> Any:
            return await inner.load(run_id)

        async def append(self, run_id: str, append: JournalAppend) -> AppendResult:
            if append.entry["kind"] == "step-begin" and not self.raced:
                self.raced = True
                await _append(
                    inner,
                    "h2",
                    stamp(
                        {
                            "kind": "signal",
                            "seq": NON_COMMAND_SEQ,
                            "name": "s",
                            "data": 1,
                            "signalId": "x",
                        },
                        TS,
                    ),
                    "h2:sig:x",
                )
            return await inner.append(run_id, append)

    out = await asyncio.wait_for(_drive(Racing(), "h2", _cancellable()), timeout=3)
    assert out.kind == "fenced"


# ------------------------------------------------ one step-scope variable, at module level


def test_the_package_creates_its_context_variables_at_module_level_only() -> None:
    """A ContextVar made per ctx is never collected properly (every Context that saw it holds it),
    and the TypeScript twin of that shape — an AsyncLocalStorage per execution — made a Node 22
    worker slower with every run it had ever driven. Pinned by shape: timing it would be flaky."""
    import pathlib
    import re

    import synadia_durable

    root = pathlib.Path(synadia_durable.__file__).parent
    hits = [
        f"{path.relative_to(root)}: {line.strip()}"
        for path in sorted(root.rglob("*.py"))
        for line in path.read_text().splitlines()
        if re.search(r"=\s*contextvars\.ContextVar\(|=\s*ContextVar\(", line)
    ]
    assert hits, "the step scope is a ContextVar"
    assert all(re.match(r"^[\w/]+\.py: _?[A-Z_]+\b", h) for h in hits), hits


# ------------------------------------------------ OPEN findings, pinned by what SHOULD happen


@pytest.mark.xfail(
    strict=True,
    reason="OPEN (step-failure-not-journaled): a step that raised leaves a step-begin with no "
    "step-end, so every replay runs it again — and if it then succeeds, the workflow takes the "
    "other branch and the run dies of NonDeterminismError. `strict` turns this RED when fixed.",
)
async def test_a_step_that_raised_and_was_caught_is_not_run_again_on_replay() -> None:
    journal = InMemoryJournal()
    await _start(journal, "o1")
    charges = {"n": 0}

    async def charge() -> str:
        charges["n"] += 1
        if charges["n"] == 1:
            raise ConnectionError("card network timeout")
        return "CHG-1"

    async def apologise() -> str:
        return "mailed"

    async def body(ctx: Ctx, _input: Any) -> str:
        how = "charged"
        try:
            await ctx.run("charge", charge)
        except ConnectionError:
            await ctx.run("apologise", apologise)
            how = "apologised"
        await ctx.sleep("cool-off", 10)
        return how

    class Sched:
        async def schedule_wake(self, *_a: Any) -> None:
            return None

    wf = define_workflow("w", body)

    async def drive() -> Any:
        return await execute_run(
            "o1", workflow=wf, journal=journal, loaded=await journal.load("o1"), scheduler=Sched()
        )

    assert (await drive()).kind == "suspended"
    await _append(
        journal,
        "o1",
        stamp({"kind": "resolve", "seq": NON_COMMAND_SEQ, "resolvesSeq": 2}, TS),
        MsgId.resolve("o1", 2),
    )
    second = await drive()
    assert charges["n"] == 1
    assert (second.kind, second.value) == ("completed", "apologised")


@pytest.mark.xfail(
    strict=True,
    reason="OPEN (premature-suspend): with a gather over a rejected branch and a pending one, the "
    "pending branch's halt reaches the executor while the except-block is still running. The run "
    "is reported suspended, the in-flight step is cancelled mid-effect (a step-begin with no "
    "step-end), EVERY later wake fires it again, and the workflow's return is never recorded — it "
    "waits forever for an approval its code stopped wanting. `strict` turns this RED when fixed.",
)
async def test_a_run_is_not_reported_suspended_while_its_code_is_still_running() -> None:
    journal = InMemoryJournal()
    await _start(journal, "o2")
    refunds = {"n": 0}

    async def child_body(_ctx: Ctx, _input: Any) -> str:
        return "unused"

    child = define_workflow("child", child_body)

    async def refund() -> str:
        refunds["n"] += 1
        await asyncio.sleep(0.015)
        return "refunded"

    async def body(ctx: Ctx, _input: Any) -> str:
        failing = ctx.start_call(child, {})
        approval = ctx.start_wait("approval")  # never sent
        try:
            await asyncio.gather(failing, approval)
        except Exception:  # noqa: BLE001 — the point is that ANY failure takes the fallback
            await ctx.run("refund", refund)
            return "the child failed — gave up"
        return "approved"

    class Kids:
        async def start(self, _child: Any) -> None:
            return None

    wf = define_workflow("w", body)

    async def drive() -> Any:
        return await execute_run(
            "o2", workflow=wf, journal=journal, loaded=await journal.load("o2"), children=Kids()
        )

    assert (await drive()).kind == "suspended"
    await _append(
        journal,
        "o2",
        stamp(
            {
                "kind": "resolve",
                "seq": NON_COMMAND_SEQ,
                "resolvesSeq": 0,
                "error": {"name": "Error", "message": "boom"},
            },
            TS,
        ),
        MsgId.resolve("o2", 0),
    )
    out = await drive()
    assert refunds["n"] == 1
    assert (out.kind, out.value) == ("completed", "the child failed — gave up")
