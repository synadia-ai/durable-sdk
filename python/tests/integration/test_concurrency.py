"""Bounded concurrency: many runs at once, one run at a time (against a real nats-server).

Two properties, and they pull in opposite directions. Items for DIFFERENT runs must proceed in
parallel, or one slow step head-of-line-blocks every other run of that workflow on the worker. Items
for the SAME run must not, because two concurrent drives of one run would just fence and re-run
effects — the documented at-least-once window, which a worker must never amplify locally.

The first test's control is the second: the same six runs with ``concurrency=1`` take the serialized
time. No code edit needed to prove the bound is real.
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import pytest

import existing
from conftest import rtt_from_env
from engine import create_client, create_worker
from shell.journal_store import NatsJournal
from shell.names import Names
from synadia_durable import define_workflow
from synadia_durable.journal.entry import TERMINAL_KINDS
from synadia_durable.types import Ctx

pytestmark = pytest.mark.asyncio

MB = 1024 * 1024
RESULT_TIMEOUT_MS = 30_000
#: Long enough that serialized execution is unmistakable, short enough to keep the suite quick.
STEP_S = 1.0


@pytest.fixture
async def running(nc: Any, prefix: str) -> Any:
    async def _make(workflows: list[Any], **kwargs: Any) -> tuple[Any, Any]:
        worker = create_worker(nc, workflows, prefix=prefix, stream_max_bytes=16 * MB, **kwargs)
        await worker.start()
        made.append(worker)
        return worker, create_client(nc, prefix=prefix, stream_max_bytes=16 * MB)

    made: list[Any] = []
    try:
        yield _make
    finally:
        for worker in made:
            await worker.stop()


def _slow_step() -> str:
    """A blocking step — the normal case in Python, dispatched to a worker thread by ``ctx.run``."""
    time.sleep(STEP_S)
    return "done"


async def test_six_runs_of_a_slow_step_finish_in_parallel(running: Any) -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("slow", _slow_step)

    wf = define_workflow("pyconc1", body)
    _worker, client = await running([wf], concurrency=8)
    # Warm up first: the client provisions on its first call, and at a WAN round trip
    # (DE_TEST_RTT_MS) that alone is over a second — it is not what is being measured.
    await (await client.start(wf, None)).result(RESULT_TIMEOUT_MS)

    started = time.monotonic()
    handles = [await client.start(wf, None) for _ in range(6)]
    results = await asyncio.gather(*(h.result(RESULT_TIMEOUT_MS) for h in handles))
    elapsed = time.monotonic() - started

    assert results == ["done"] * 6
    # Serialized this would be 6 × (the step + a run's ~11 dependent round trips); in parallel it
    # is one step plus the six sequential start() calls (3 round trips each) and dispatch overhead.
    rtt_s = rtt_from_env() / 1000
    bound = STEP_S * 3 + 40 * rtt_s
    assert bound < 6 * (STEP_S + 11 * rtt_s)
    assert elapsed < bound, f"six runs took {elapsed:.1f}s — they did not run in parallel"


async def test_the_same_six_runs_serialize_at_concurrency_one(running: Any) -> None:
    """The control for the test above, and the bound itself: a slot limit that does not limit is a bug."""

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("slow", _slow_step)

    wf = define_workflow("pyconc2", body)
    _worker, client = await running([wf], concurrency=1)

    started = time.monotonic()
    handles = [await client.start(wf, None) for _ in range(6)]
    results = await asyncio.gather(*(h.result(RESULT_TIMEOUT_MS) for h in handles))
    elapsed = time.monotonic() - started

    assert results == ["done"] * 6
    assert elapsed > STEP_S * 4, f"six runs took {elapsed:.1f}s — the bound was not enforced"


async def test_a_burst_of_wakes_for_one_run_never_drives_it_concurrently(running: Any) -> None:
    """Per-run serialization. Concurrency must not turn one run's wake storm into overlapping drives.

    Each delivered signal is a work item for the same run, so without the per-run chain they would
    execute side by side here — the same duplication the gating lease exists to cut, manufactured
    locally by this worker. The workflow body reports the overlap itself.
    """
    state = {"active": 0, "peak": 0}

    async def body(ctx: Ctx, _input: Any) -> Any:
        state["active"] += 1
        state["peak"] = max(state["peak"], state["active"])
        try:
            await asyncio.sleep(0.25)  # widen the window a concurrent drive would land in
            return await ctx.wait("go")
        finally:
            state["active"] -= 1

    wf = define_workflow("pyconc3", body)
    _worker, client = await running([wf], concurrency=8)

    handle = await client.start(wf, {})
    await asyncio.sleep(0.6)  # let the first drive park on the signal
    await asyncio.gather(*(client.signal(handle.run_id, "go", i) for i in range(5)))

    # Which of the five wins is whichever landed on the stream first, and five concurrent publishes
    # have no defined order — the run resumes with one of them, deterministically on replay because
    # the journal fixes it, but not predictably from here.
    assert await handle.result(RESULT_TIMEOUT_MS) in range(5)
    assert state["peak"] == 1, f"the run was driven {state['peak']}× concurrently on one worker"


async def test_stop_drains_the_work_it_already_dispatched(nc: Any, prefix: str) -> None:
    """A dispatched item owes the server a decision; a shutdown that abandons one hides a run.

    An item neither acked nor NAK'd stays ack-pending for the whole AckWait, so its run is
    untouchable by every other worker until then — precisely the runs a rolling deploy was working
    on. The drain covers the dispatched task now, not just the pump that fetched it.
    """

    executing = threading.Event()

    def slower_than_the_pump_drain() -> str:
        # It has to outlive the pumps' own exit (bounded by their fetch timeout), or the first drain
        # stage would cover this by accident and the test would prove nothing.
        executing.set()
        time.sleep(4.0)
        return "done"

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("slow", slower_than_the_pump_drain)

    wf = define_workflow("pyconc4", body)
    worker = create_worker(nc, [wf], prefix=prefix, stream_max_bytes=16 * MB)
    await worker.start()
    client = create_client(nc, prefix=prefix, stream_max_bytes=16 * MB)
    handle = await client.start(wf, None)
    # durable-sdk: wait until the step is executing, rather than 0.4 s: over a WAN round trip
    # (F29) the item reaches the worker later than that.
    await asyncio.to_thread(executing.wait, existing.scaled(10))
    assert executing.is_set(), "the step never started"

    await worker.stop()  # mid-step: the drain must let it finish and write its terminal

    journal = NatsJournal(nc, Names(prefix))
    loaded = await journal.load(handle.run_id)
    assert any(le.entry["kind"] in TERMINAL_KINDS for le in loaded), (
        "stop() returned while the item was still mid-flight"
    )
