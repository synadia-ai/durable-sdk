"""The metrics seam and ``health()`` (against a real nats-server).

Everything the last five tranches added — the lost-wake repair, the lease, the heartbeats, bounded
concurrency, offload — computes numbers an operator needs and, until now, threw all of them away.
This is the seam that lets a host app see them, and the tests are about the two properties that make
a telemetry seam trustworthy: the events actually fire on the paths they claim, and a broken sink
cannot take the engine down with it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

import pytest

import existing
from engine import create_client, create_worker
from synadia_durable import define_workflow
from synadia_durable.types import Ctx

pytestmark = pytest.mark.asyncio

MB = 1024 * 1024
RESULT_TIMEOUT_MS = 30_000


class Recorder:
    """A sink that keeps everything, so a test can ask what the engine reported."""

    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def on_event(self, name: str, fields: Mapping[str, Any]) -> None:
        self.events.append((name, dict(fields)))

    def named(self, name: str) -> list[dict[str, Any]]:
        return [fields for event, fields in self.events if event == name]

    def outcomes(self, name: str) -> list[Any]:
        return [f.get("outcome") for f in self.named(name)]


class ExplodingSink:
    """A sink that throws on every call. A real one will, eventually, on some field it did not expect."""

    def __init__(self) -> None:
        self.calls = 0

    def on_event(self, name: str, fields: Mapping[str, Any]) -> None:  # noqa: ARG002 — the seam's shape
        self.calls += 1
        raise RuntimeError("this sink is broken")


@pytest.fixture
async def running(nc: Any, prefix: str) -> Any:
    async def _make(workflows: list[Any], **kwargs: Any) -> tuple[Any, Any]:
        worker = create_worker(nc, workflows, prefix=prefix, stream_max_bytes=16 * MB, **kwargs)
        await worker.start()
        made.append(worker)
        return worker, create_client(
            nc, prefix=prefix, stream_max_bytes=16 * MB, metrics=kwargs.get("metrics")
        )

    made: list[Any] = []
    try:
        yield _make
    finally:
        for worker in made:
            await worker.stop()


async def _settled(sink: Any) -> None:
    """Wait until the worker has emitted the run's completed ``work.done``.

    ``result()`` resolves on the status-key hint, which the worker publishes BEFORE it acks the item
    and emits ``work.done`` — so a test that reads worker-side events right after ``result()`` is
    racing the worker's last few hundred microseconds, and one that runs the maintenance pass needs
    the ack to have landed.
    """
    for _ in range(200):
        if "completed" in sink.outcomes("work.done"):
            return
        await asyncio.sleep(0.05)
    raise AssertionError("the worker never reported the run's completed work item")


async def test_one_ordinary_run_reports_the_whole_path(running: Any) -> None:
    """The events a dashboard is built from, from one run that does nothing unusual."""
    sink = Recorder()

    async def body(ctx: Ctx, _input: Any) -> Any:
        await ctx.run("step", lambda: "s")
        await ctx.sleep("brief", 50)
        return "done"

    wf = define_workflow("pymetrics1", body)
    _worker, client = await running([wf], metrics=sink)

    handle = await client.start(wf, None)
    assert await handle.result(RESULT_TIMEOUT_MS) == "done"
    await _settled(sink)

    # Journal writes, with the outcome that distinguishes a healthy append from a fenced one.
    appends = sink.named("journal.append")
    assert appends, "no journal.append events"
    assert set(sink.outcomes("journal.append")) <= {"ok", "duplicate"}
    # durable-sdk: assertion touched, ``"start"`` removed from the kinds. The ``start`` entry is
    # the engine's to append (API.md ``start``); the worker appends every entry after it.
    assert {a["kind"] for a in appends} >= {"step-begin", "step-end", "sleep", "result"}
    assert all(isinstance(a["ms"], int) and a["bytes"] > 0 for a in appends)

    # Journal reads: every replay starts with one.
    assert sink.named("journal.load"), "no journal.load events"
    assert set(sink.outcomes("journal.load")) == {"ok"}
    # durable-sdk: one per item, with the journal's size (the twin of the TypeScript worker's,
    # F21; test/integration/metrics.test.ts asserts the same).
    loads = sink.named("journal.load")
    assert len(loads) >= len(sink.named("work.dispatch"))
    assert all(ld["entries"] > 0 and ld["bytes"] > 0 for ld in loads)

    # Work items in and out, with the ack reason that says WHY.
    assert {d["kind"] for d in sink.named("work.dispatch")} >= {"start", "timer"}
    done = sink.named("work.done")
    assert {d["outcome"] for d in done} >= {"suspended", "completed"}
    assert all("wait_ms" in d and "delivery_count" in d for d in done)

    # durable-sdk: assertions removed — the two on ``lease.gate``. The gating run lease is the
    # engine's (taken before a poll hands the item out), so no worker arbitrates or reports it;
    # the engine neither logs nor counts a lost one (F8).


# durable-sdk: "the hygiene pass reports its gauge and its sweep" is dropped. The maintenance pass
# (purge, headroom, the lost-wake sweep) is the engine's (``--maintenance-interval``), the worker
# has no ``_maintenance_tick`` to drive, and the engine reports it in its own /metrics and log, not
# through the SDK's sink.

# durable-sdk: "offload traffic is visible" is dropped. Offload is the engine's (API.md "Sizes"): no
# blob store is reachable from the SDK, so ``blob.put`` / ``blob.get`` are never the worker's, and
# its 1-byte ``OffloadOptions`` threshold has no engine equivalent (``serve`` has no
# ``--offload-threshold``: F2).


async def test_a_broken_sink_cannot_break_a_run(running: Any) -> None:
    """Observability is never control flow. A sink that throws on every event changes nothing."""
    sink = ExplodingSink()

    async def body(ctx: Ctx, _input: Any) -> Any:
        await ctx.run("step", lambda: "s")
        return "done"

    wf = define_workflow("pymetrics4", body)
    _worker, client = await running([wf], metrics=sink)

    assert await (await client.start(wf, None)).result(RESULT_TIMEOUT_MS) == "done"
    assert sink.calls > 0, "the sink was never called, so this proved nothing"


async def test_health_reports_what_the_worker_is_doing(nc: Any, prefix: str) -> None:
    """A readiness probe has to answer without a server round trip — the outage may BE the server."""
    release = asyncio.Event()

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("hold", release.wait)

    wf = define_workflow("pymetrics5", body)
    worker = create_worker(nc, [wf], prefix=prefix, stream_max_bytes=16 * MB)

    before = worker.health()
    assert before.running is False
    assert before.pumps == []

    await worker.start()
    try:
        client = create_client(nc, prefix=prefix, stream_max_bytes=16 * MB)
        handle = await client.start(wf, None)
        # durable-sdk: until the item is held, rather than 0.5 s: over a WAN round trip
        # (F29) it reaches the worker later than that.
        deadline = asyncio.get_running_loop().time() + existing.scaled(10)
        while worker.health().inflight == 0 and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.05)

        live = worker.health()
        assert live.running is True
        assert live.inflight == 1, "the held work item should be counted in flight"
        assert live.queued_runs == 1
        # durable-sdk: assertions touched — "two pumps per workflow name, on _WORK and _TIMERS"
        # → one poll slot per unit of concurrency (16 by default), each on the engine's poll
        # subject. The consumers behind them are the engine's (the TS twin's network suite made
        # the same change to its pump assertion).
        assert len(live.pumps) == 16
        assert all(p.alive for p in live.pumps)
        assert {p.stream for p in live.pumps} == {f"{prefix}.api.v1.poll"}
        assert all(p.restarts == 0 and p.last_error is None for p in live.pumps)

        release.set()  # let the held step finish, so the drain below has nothing to abandon
        assert await handle.result(RESULT_TIMEOUT_MS) is True
    finally:
        await worker.stop()

    after = worker.health()
    assert after.running is False
    assert after.inflight == 0
