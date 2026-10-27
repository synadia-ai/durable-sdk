"""The engine's floor and the one withdrawal (F45), against a real
nats-server. The twin of ``test/integration/engine-floor.test.ts``.

The floor: ``worker.start()`` reads ``info.capabilities`` and refuses an engine that does not
announce ``withdrawn-remembered`` (the engine's F53, from commit 16cb3da), naming it by
``engine`` and ``commit``, never by its version. A stub responder on a prefix of the test's own
answers ``info`` as an older engine would, and as one at the floor; and, for a start that found no
engine, only after ``start()`` has returned, so the first answered poll checks the floor and stops
the worker.

The one withdrawal: on the pinned engine, ``stop()`` sends one ``cancelPoll`` and returns at once,
never after a poll's wait, also when it follows ``start()`` at once and when the polls reach the
engine after the withdrawal (the engine remembers the worker); and a restart of the same worker
polls as before (its ``waitMs`` 0 poll ends the engine's memory of the stop).
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Any

import pytest

import existing
from engine import create_worker
from synadia_durable import UnsupportedEngineError, define_workflow
from synadia_durable import create_worker as sdk_create_worker

pytestmark = pytest.mark.asyncio


async def _ok(_ctx: Any, _input: Any) -> str:
    return "ok"


WF = define_workflow("pyfloor_wf", _ok)
OLD = {"engine": "0.2.0-dev", "commit": "stub5047d79", "api": "v1"}
AT_FLOOR = {**OLD, "commit": "stub16cb3da", "capabilities": ["withdrawn-remembered"]}


class _Errors:
    """A logger that keeps the error lines."""

    def __init__(self) -> None:
        self.lines: list[str] = []

    def debug(self, message: str, **fields: Any) -> None:
        pass

    def info(self, message: str, **fields: Any) -> None:
        pass

    def warn(self, message: str, **fields: Any) -> None:
        pass

    def error(self, message: str, **fields: Any) -> None:  # noqa: ARG002
        self.lines.append(message)


class _Stub:
    """A stub engine on ``prefix``: ``info`` answers ``info`` (none subscribed while it is None),
    ``poll`` answers ``none`` after ``poll_s`` (a withdrawal at once), ``done`` answers ``{}``."""

    def __init__(self, nc: Any, prefix: str, poll_s: float) -> None:
        self._nc = nc
        self._prefix = prefix
        self._poll_s = poll_s
        self._subs: list[Any] = []
        self._tasks: set[asyncio.Task[None]] = set()

    async def start(self, info: dict[str, Any] | None) -> _Stub:
        async def on_poll(m: Any) -> None:
            async def later() -> None:
                if not json.loads(m.data or b"{}").get("cancelPoll"):
                    await asyncio.sleep(self._poll_s)
                await m.respond(b'{"none":true}')

            t = asyncio.ensure_future(later())
            self._tasks.add(t)
            t.add_done_callback(self._tasks.discard)

        async def on_done(m: Any) -> None:
            await m.respond(b"{}")

        self._subs.append(await self._nc.subscribe(f"{self._prefix}.api.v1.poll", cb=on_poll))
        self._subs.append(await self._nc.subscribe(f"{self._prefix}.api.v1.done", cb=on_done))
        if info is not None:
            await self.answer_info(info)
        await self._nc.flush()
        return self

    async def answer_info(self, info: dict[str, Any]) -> None:
        data = json.dumps(info).encode()

        async def on_info(m: Any) -> None:
            await m.respond(data)

        self._subs.append(await self._nc.subscribe(f"{self._prefix}.api.v1.info", cb=on_info))
        await self._nc.flush()

    async def close(self) -> None:
        for s in self._subs:
            await s.unsubscribe()
        for t in list(self._tasks):
            t.cancel()


def _own() -> str:
    """A prefix of the test's own, never the run's: no engine of the harness serves it."""
    return f"pyfloor{uuid.uuid4().hex[:8]}"


async def test_start_raises_on_an_engine_with_no_capabilities_naming_its_engine_and_commit(
    nc: Any,
) -> None:
    prefix = _own()
    stub = await _Stub(nc, prefix, 0.1).start(OLD)
    try:
        log = _Errors()
        w = sdk_create_worker(nc, [WF], prefix=prefix, logger=log)
        with pytest.raises(UnsupportedEngineError) as caught:
            await w.start()
        e = caught.value
        assert (e.engine, e.commit, e.capabilities) == ("0.2.0-dev", "stub5047d79", None)
        for part in ("stub5047d79", "0.2.0-dev", prefix):
            assert part in str(e)
        assert not w.health().running
        assert len(log.lines) == 1
    finally:
        await stub.close()


async def test_an_engine_that_announces_withdrawn_remembered_is_let_through(nc: Any) -> None:
    prefix = _own()
    stub = await _Stub(nc, prefix, 0.1).start(AT_FLOOR)
    w = sdk_create_worker(nc, [WF], prefix=prefix, concurrency=2)
    try:
        await w.start()
        assert w.health().running
    finally:
        await w.stop()
        await stub.close()


async def test_a_start_that_found_no_engine_checks_the_floor_at_its_first_answered_poll(
    nc: Any,
) -> None:
    prefix = _own()
    stub = await _Stub(nc, prefix, 0.3).start(None)
    log = _Errors()
    w = sdk_create_worker(nc, [WF], prefix=prefix, concurrency=2, logger=log)
    try:
        await w.start()  # no `info` responder: not fatal, the slots poll
        assert w.health().running
        await stub.answer_info(OLD)  # the engine answers info only now
        deadline = time.monotonic() + existing.scaled(5)
        while w.health().running and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        assert not w.health().running
        assert len(log.lines) == 1
        assert "stub5047d79" in log.lines[0]
        assert "this worker stops" in log.lines[0]
    finally:
        await w.stop()
        await stub.close()


class _Lagging:
    """The worker's connection, which can hold its polls back before they are sent: a poll sent
    just before the withdrawal that arrives just after it, as over a slow link."""

    def __init__(self, nc: Any) -> None:
        self._nc = nc
        self.lag_s = 0.0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._nc, name)

    async def request(self, subject: str, payload: bytes = b"", **kwargs: Any) -> Any:
        body = json.loads(payload or b"{}")
        if (
            self.lag_s > 0
            and subject.endswith(".api.v1.poll")
            and not body.get("cancelPoll")
            and body.get("waitMs", 0) > 0
        ):
            await asyncio.sleep(self.lag_s)
        return await self._nc.request(subject, payload, **kwargs)


async def test_one_cancel_poll_per_stop_at_once_and_the_restarted_workers_polls_wait(
    nc: Any, prefix: str
) -> None:
    lagging = _Lagging(nc)
    wf = define_workflow("pywstop_wf", _ok)
    w = create_worker(lagging, [wf], prefix=prefix, concurrency=4)
    # Every poll request on the prefix, as the engine's queue group sees them (this subscription is
    # outside it, so it sees each one): this worker's are those naming its workflow.
    polls: list[dict[str, Any]] = []

    async def on_poll(m: Any) -> None:
        b = json.loads(m.data or b"{}")
        if b.get("cancelPoll") or any(
            x.get("name") == "pywstop_wf" for x in b.get("workflows", [])
        ):
            polls.append({**b, "t": time.monotonic()})

    sub = await nc.subscribe(f"{prefix}.api.v1.poll", cb=on_poll)
    await nc.flush()

    def me() -> str:
        return next(str(p["worker"]) for p in polls if not p.get("cancelPoll"))

    def cancels(since: float) -> int:
        return sum(1 for p in polls if p.get("cancelPoll") and p.get("worker") == me() and p["t"] >= since)  # fmt: skip

    try:
        # Polls out and waiting, then stop(); five stops right after their start(); and one whose
        # polls reach the engine 600 ms after the withdrawal (the engine remembers the worker and
        # answers them `none` at once; the SDK's former stop() withdrew again every 250 ms).
        for settle, lag in [(0.5, 0.0), *[(0.0, 0.0)] * 5, (0.05, 0.6)]:
            lagging.lag_s = lag
            await w.start()
            if settle > 0:
                await asyncio.sleep(settle)
            t0 = time.monotonic()
            await w.stop()
            took = time.monotonic() - t0
            lagging.lag_s = 0.0
            await nc.flush()
            await asyncio.sleep(0.1)
            assert cancels(t0) == 1
            assert took < existing.scaled(5)  # a poll's wait is 30 s
            if lag > 0:
                assert took >= lag - 0.1  # the late polls were out
        # The restart after the stops: its waitMs 0 poll ends the engine's memory of them, so the
        # slots' polls wait as before instead of answering `none` at once in a loop.
        t1 = time.monotonic()
        await w.start()
        await asyncio.sleep(1.0)
        sent = sum(1 for p in polls if not p.get("cancelPoll") and p.get("worker") == me() and p["t"] >= t1)  # fmt: skip
        assert sent <= 1 + 4 + 2  # start()'s and one per slot, a little slack
    finally:
        await w.stop()
        await sub.unsubscribe()
