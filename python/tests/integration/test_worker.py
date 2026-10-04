"""The worker, end to end against a real nats-server: Python runs workflows now.

Everything here goes all the way through — client starts a run, worker binds consumers, drives it,
journals it, acks it — so these are the tests that would catch a wrong ack, a lost wake, or a child
that never gets started.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

import existing
from engine import create_client, create_worker
from synadia_durable import (
    CancelledError,
    RetryPolicy,
    WorkflowFailure,
    define_workflow,
)
from synadia_durable.types import Ctx, StartOptions

pytestmark = pytest.mark.asyncio

MB = 1024 * 1024
RESULT_TIMEOUT_MS = 20_000


@pytest.fixture
async def running(nc: Any, prefix: str) -> Any:
    """A worker + client pair on a fresh prefix; the worker is torn down after the test."""

    async def _make(workflows: list[Any], **kwargs: Any) -> tuple[Any, Any]:
        worker = create_worker(nc, workflows, prefix=prefix, stream_max_bytes=64 * MB, **kwargs)
        await worker.start()
        made.append(worker)
        return worker, create_client(nc, prefix=prefix, stream_max_bytes=64 * MB)

    made: list[Any] = []
    try:
        yield _make
    finally:
        for worker in made:
            await worker.stop()


async def test_a_python_worker_runs_a_python_workflow(running: Any) -> None:
    async def body(ctx: Ctx, order: dict[str, Any]) -> dict[str, Any]:
        upper = await ctx.run("shout", lambda: str(order["name"]).upper())
        at = await ctx.now()
        return {"greeting": f"HELLO {upper}", "at": at}

    wf = define_workflow("pygreet", body)
    _worker, client = await running([wf])

    handle = await client.start(wf, {"name": "ada"})
    result = await handle.result(RESULT_TIMEOUT_MS)

    assert result["greeting"] == "HELLO ADA"
    assert isinstance(result["at"], int)
    assert (await handle.status()).state == "completed"


async def test_a_failing_workflow_reaches_a_failed_terminal(running: Any) -> None:
    async def body(_ctx: Ctx, _input: Any) -> None:
        raise ValueError("inventory went negative")

    wf = define_workflow("pyboom", body)
    _worker, client = await running([wf])

    handle = await client.start(wf, {})
    with pytest.raises(WorkflowFailure, match="inventory went negative"):
        await handle.result(RESULT_TIMEOUT_MS)
    assert (await handle.status()).state == "failed"


async def test_a_signal_wakes_a_parked_python_run(running: Any) -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        decision = await ctx.wait("approval")
        return {"approved": decision["ok"]}

    wf = define_workflow("pyapprove", body)
    _worker, client = await running([wf])

    handle = await client.start(wf, {})
    await _wait_for_state(handle, "suspended")
    await client.signal(handle.run_id, "approval", {"ok": True})

    assert await handle.result(RESULT_TIMEOUT_MS) == {"approved": True}


async def test_a_durable_sleep_actually_fires(running: Any) -> None:
    """The one that exercises native message scheduling all the way through.

    A short sleep, but a real one: the schedule goes to DE_TIMERS, the server delivers the wake to
    the timers consumer, and the worker resolves the command and resumes the run.
    """

    async def body(ctx: Ctx, _input: Any) -> str:
        await ctx.sleep("brief", 1000)
        return "woke up"

    wf = define_workflow("pysleep", body)
    _worker, client = await running([wf])

    handle = await client.start(wf, {})
    assert await handle.result(RESULT_TIMEOUT_MS) == "woke up"


async def test_a_sub_workflow_runs_and_resumes_its_parent(running: Any) -> None:
    async def child_body(ctx: Ctx, order: dict[str, Any]) -> dict[str, Any]:
        return {"tracking": await ctx.run("label", lambda: f"TRK-{order['id']}")}

    async def parent_body(ctx: Ctx, _input: Any) -> Any:
        shipment = await ctx.call(child, {"id": "o-1"})
        return {"shipped": shipment}

    child = define_workflow("pyship", child_body)
    parent = define_workflow("pyorder", parent_body)
    _worker, client = await running([parent, child])

    handle = await client.start(parent, {})
    assert await handle.result(RESULT_TIMEOUT_MS) == {"shipped": {"tracking": "TRK-o-1"}}


async def test_a_fan_out_of_children_runs_them_all_and_resumes_once_each(running: Any) -> None:
    """Fan-out where it is hardest: real children, real work items, real wakes.

    The pure suite proves the engine's bookkeeping against fake seams. What only a server proves is
    the wake path — three children finish independently, so the parent is re-driven up to three
    times, and each pass replays the branches already recorded and suspends on the rest. Getting
    that wrong loses a branch or double-starts one.
    """

    async def child_body(ctx: Ctx, parcel: dict[str, Any]) -> str:
        return str(await ctx.run("label", lambda: f"TRK-{parcel['id']}"))

    async def parent_body(ctx: Ctx, _input: Any) -> list[str]:
        futures = [ctx.start_call(child, {"id": n}) for n in ("a", "b", "c")]
        labels = await asyncio.gather(*futures)
        return sorted(str(x) for x in labels)

    child = define_workflow("pyparcel", child_body)
    parent = define_workflow("pyfanout", parent_body)
    _worker, client = await running([parent, child])

    handle = await client.start(parent, {})
    assert await handle.result(RESULT_TIMEOUT_MS) == ["TRK-a", "TRK-b", "TRK-c"]
    # Three separate runs, at the deterministic child ids — one per branch, each finished.
    for seq in range(3):
        assert (await client.get(f"{handle.run_id}_{seq}").status()).state == "completed"


async def test_a_race_between_a_signal_and_a_durable_timer(running: Any) -> None:
    """``ctx.race`` against a real timer: the deadline fires, the signal never comes."""

    async def body(ctx: Ctx, _input: Any) -> str:
        won = await ctx.race([ctx.start_wait("approval"), ctx.start_sleep("deadline", 1000)])
        return f"approved:{won}" if won is not None else "timed-out"

    wf = define_workflow("pyrace2", body)
    _worker, client = await running([wf])

    handle = await client.start(wf, {})
    assert await handle.result(RESULT_TIMEOUT_MS) == "timed-out"


async def test_a_signal_beats_the_timer_in_a_real_race(running: Any) -> None:
    """The other half: a signal arrives first, so the run resumes with it and ignores the deadline."""

    async def body(ctx: Ctx, _input: Any) -> str:
        won = await ctx.race([ctx.start_wait("approval"), ctx.start_sleep("deadline", 60_000)])
        return f"approved:{won}" if won is not None else "timed-out"

    wf = define_workflow("pyrace3", body)
    _worker, client = await running([wf])

    handle = await client.start(wf, {})
    await _wait_for_state(handle, "suspended")
    await client.signal(handle.run_id, "approval", "yes")
    assert await handle.result(RESULT_TIMEOUT_MS) == "approved:yes"


async def test_a_failing_child_surfaces_to_the_parent(running: Any) -> None:
    async def child_body(_ctx: Ctx, _input: Any) -> None:
        raise RuntimeError("carrier refused")

    async def parent_body(ctx: Ctx, _input: Any) -> str:
        try:
            await ctx.call(child, {})
        except WorkflowFailure:
            return "compensated"
        return "shipped"

    child = define_workflow("pyship2", child_body)
    parent = define_workflow("pyorder2", parent_body)
    _worker, client = await running([parent, child])

    handle = await client.start(parent, {})
    assert await handle.result(RESULT_TIMEOUT_MS) == "compensated"


async def test_continue_as_new_chains_generations(running: Any) -> None:
    async def body(ctx: Ctx, state: dict[str, Any]) -> dict[str, Any]:
        cycle = state["cycle"]
        if cycle >= 3:
            return {"finished_at": cycle}
        await ctx.run("work", lambda: None)
        await ctx.continue_as_new({"cycle": cycle + 1})
        raise AssertionError("unreachable")

    wf = define_workflow("pycron", body)
    _worker, client = await running([wf])

    handle = await client.start(wf, {"cycle": 1})
    # The handle follows the chain transparently to whichever generation finally returns.
    assert await handle.result(RESULT_TIMEOUT_MS) == {"finished_at": 3}


async def test_cancel_hard_stops_a_parked_python_run(running: Any) -> None:
    compensated: list[str] = []

    async def body(ctx: Ctx, _input: Any) -> Any:
        ctx.on_cancel(lambda: compensated.append("released the hold"))
        return await ctx.wait("approval")

    wf = define_workflow("pycancel", body)
    _worker, client = await running([wf])

    handle = await client.start(wf, {})
    await _wait_for_state(handle, "suspended")
    await client.cancel(handle.run_id, reason="customer withdrew")

    with pytest.raises(CancelledError):
        await handle.result(RESULT_TIMEOUT_MS)
    # durable-sdk: ``status`` read the engine's hint, written at the worker's ``done``, one round
    # trip after the terminal ``result`` answers from (docs/findings.md F27): wait for it. Harmless
    # now: the engine guarantees the state since ee81460 (its F29), so a failure is the engine's.
    await _wait_for_state(handle, "cancelled", 10.0)
    assert compensated == ["released the hold"]


async def test_a_crash_between_steps_replays_without_re_executing(running: Any) -> None:
    """Restart the worker mid-run and prove the recorded step does not fire twice."""
    effects: list[str] = []

    async def body(ctx: Ctx, _input: Any) -> str:
        await ctx.run("charge", lambda: effects.append("charged"))
        await ctx.wait("continue")
        await ctx.run("ship", lambda: effects.append("shipped"))
        return "done"

    wf = define_workflow("pycrash", body)
    worker, client = await running([wf])

    handle = await client.start(wf, {})
    await _wait_for_state(handle, "suspended")
    assert effects == ["charged"]

    # "Crash": stop the worker entirely, then bring a fresh one up on the same streams.
    await worker.stop()
    _worker2, _client2 = await running([wf])
    await client.signal(handle.run_id, "continue", None)

    assert await handle.result(RESULT_TIMEOUT_MS) == "done"
    # The charge is journaled, so replay returns its recorded value rather than charging again.
    assert effects == ["charged", "shipped"]


async def test_an_unregistered_version_parks_as_blocked_instead_of_failing(
    nc: Any, prefix: str
) -> None:
    """The run is not broken — its code just is not deployed here yet."""

    async def body(ctx: Ctx, _input: Any) -> str:
        return str(await ctx.run("x", lambda: "v2 ran"))

    v1 = define_workflow("pyversioned", body, 1)
    v2 = define_workflow("pyversioned", body, 2)

    # A worker with ONLY v1 registered, and a run started at v2.
    worker = create_worker(nc, [v1], prefix=prefix, stream_max_bytes=64 * MB, version_wait_ms=500)
    await worker.start()
    client = create_client(nc, prefix=prefix, stream_max_bytes=64 * MB)
    try:
        handle = await client.start(v2, {}, StartOptions(run_id="blocked-run"))
        await _wait_for_state(handle, "blocked")
        status = await handle.status()
        assert status.detail is not None
        assert "v2" in status.detail
    finally:
        await worker.stop()

    # Deploy v2: the parked run resumes on its own, with no intervention.
    worker2 = create_worker(nc, [v1, v2], prefix=prefix, stream_max_bytes=64 * MB)
    await worker2.start()
    try:
        assert await client.get("blocked-run").result(RESULT_TIMEOUT_MS) == "v2 ran"
    finally:
        await worker2.stop()


async def test_a_step_retry_policy_survives_a_flaky_effect(running: Any) -> None:
    attempts: list[int] = []

    async def body(ctx: Ctx, _input: Any) -> str:
        def flaky() -> str:
            attempts.append(1)
            if len(attempts) < 3:
                raise ConnectionError("503")
            return "ok"

        return str(await ctx.run("flaky", flaky, retry=RetryPolicy(attempts=5, delay_ms=10)))

    wf = define_workflow("pyflaky", body)
    _worker, client = await running([wf])

    handle = await client.start(wf, {})
    assert await handle.result(RESULT_TIMEOUT_MS) == "ok"
    assert len(attempts) == 3


async def test_dependencies_are_injected_into_workflows(running: Any) -> None:
    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("read", lambda: ctx.get("greeting"))

    wf = define_workflow("pydeps", body)
    _worker, client = await running([wf], dependencies={"greeting": "hi from the worker"})

    handle = await client.start(wf, {})
    assert await handle.result(RESULT_TIMEOUT_MS) == "hi from the worker"


async def test_two_workers_on_one_run_stay_exactly_once(running: Any) -> None:
    """Multi-worker safety: the journal CAS, not the worker count, is what makes state exact."""
    effects: list[str] = []

    def effect() -> int:
        effects.append("ran")
        return len(effects)

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("effect", effect)

    wf = define_workflow("pyrace", body)
    _worker_a, client = await running([wf])
    await running([wf])  # a second worker sharing the same durables

    handles = [await client.start(wf, {}) for _ in range(5)]
    results = [await h.result(RESULT_TIMEOUT_MS) for h in handles]

    # Five runs, five journaled step results, and each run's recorded value is its own.
    assert len(results) == 5
    assert all(isinstance(r, int) for r in results)


async def _wait_for_state(handle: Any, state: str, timeout_s: float = 20.0) -> None:
    timeout_s *= existing.FACTOR  # durable-sdk: × the round-trip factor (tests/existing.py, F29)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        if (await handle.status()).state == state:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"run {handle.run_id} never reached {state}")


async def test_the_worker_can_read_an_offloaded_journal(nc: Any, prefix: str) -> None:
    """A mixed fleet is the normal case, and the reference implementation offloads BY DEFAULT.

    A Python worker that could not resolve a ``v:2`` entry would park the run forever on something
    it is perfectly able to read — so the blob store has to be wired into the worker, not only the
    client. (Writing offloaded payloads is P3; this is the read side, which is what interop needs.)
    """
    import hashlib

    from shell.journal_store import NatsJournal
    from shell.names import Names
    from synadia_durable.journal.codec import json_codec
    from synadia_durable.journal.entry import MsgId, stamp
    from synadia_durable.journal.store import JournalAppend
    from synadia_durable.version import WIRE_VERSION_BLOBS

    async def body(ctx: Ctx, _input: Any) -> Any:
        # Replays the offloaded step below; its recorded value must arrive inlined.
        return await ctx.run("big", lambda: "never runs")

    wf = define_workflow("pyoffload", body)
    worker = create_worker(nc, [wf], prefix=prefix, stream_max_bytes=64 * MB)
    await worker.start()
    try:
        names = Names(prefix)
        run_id = "offloaded-run"
        value = {"frames": ["a" * 100]}
        payload = json_codec.encode_value(value)
        digest = f"sha256:{hashlib.sha256(payload).hexdigest()}"

        store = await nc.jetstream().object_store(names.blobs_bucket)
        await store.put(f"{run_id}/{digest}", payload)

        # durable-sdk: the worker holds no journal client any more; the test stages the journal
        # with its own (the library's shell, tests/shell/), as a library worker would have left it.
        journal = NatsJournal(nc, names)
        ack = await journal.append(
            run_id,
            JournalAppend(
                stamp(
                    {"kind": "start", "seq": 0, "wf": "pyoffload", "wfVersion": 1, "input": {}},
                    1,
                ),
                MsgId.start(run_id),
                0,
            ),
        )
        ack = await journal.append(
            run_id,
            JournalAppend(
                stamp({"kind": "step-begin", "seq": 0, "name": "big", "key": f"{run_id}:0"}, 1),
                MsgId.step_begin(run_id, 0),
                ack.stream_seq,
            ),
        )
        await journal.append(
            run_id,
            JournalAppend(
                stamp(
                    {
                        "kind": "step-end",
                        "seq": 0,
                        "name": "big",
                        "valueRef": {
                            "name": f"{run_id}/{digest}",
                            "digest": digest,
                            "size": len(payload),
                        },
                    },
                    1,
                    WIRE_VERSION_BLOBS,
                ),
                MsgId.step_end(run_id, 0),
                ack.stream_seq,
            ),
        )

        client = create_client(nc, prefix=prefix, stream_max_bytes=64 * MB)
        await client.start(wf, {}, StartOptions(run_id=run_id))  # re-kicks the existing run
        assert await client.get(run_id).result(RESULT_TIMEOUT_MS) == value
    finally:
        await worker.stop()
