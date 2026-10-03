"""Ack-progress heartbeats for long steps (PROTOCOL §6.4), against a real nats-server.

A step may legitimately run longer than the consumer's ack wait. Without intervention its work item
is redelivered MID-EXECUTION and a second worker re-runs the frontier step — correct under the
journal CAS, and the single biggest duplicate-effect amplifier in the system. So while a worker
holds an item it resets the AckWait every ~third of it.

The interesting half is the bound. An unbounded extension would let one hung worker keep a run to
itself for as long as its process lives, so ``max_work_ms`` (default 10× the ack wait) is the point
where the worker declares itself stuck and lets go — of the ack extension AND of the run lease,
because releasing one grip while holding the other just moves the deadlock.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from engine import create_client, create_worker, engine_log
from synadia_durable import define_workflow
from synadia_durable.types import Ctx

pytestmark = pytest.mark.asyncio

MB = 1024 * 1024
RESULT_TIMEOUT_MS = 30_000


class _CapturingLogger:
    def __init__(self) -> None:
        self.lines: list[str] = []

    def debug(self, message: str, **_f: Any) -> None: ...
    def info(self, message: str, **_f: Any) -> None: ...

    def warn(self, message: str, **_f: Any) -> None:
        self.lines.append(message)

    def error(self, message: str, **_f: Any) -> None:
        self.lines.append(message)


async def _wait_until(condition: Any, timeout_s: float = 20.0, what: str = "condition") -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        if condition():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"timed out waiting for {what}")


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


async def test_a_step_much_longer_than_the_ack_wait_is_never_redelivered_mid_execution(
    running: Any,
) -> None:
    """The whole point: 4 s of work under a 1.5 s ack wait, and no second worker ever sees it.

    TWO workers and the lease OFF, deliberately. One busy worker cannot fetch its own redelivery, so
    a single-worker version of this test would pass with the heartbeat removed — it proves nothing.
    With a peer waiting and the lease out of the way, the ack extension is the ONLY thing standing
    between a long step and a duplicate execution of it. ``ctx.attempt`` is the delivery count, so
    the workflow reports the redelivery itself; no metrics seam needed.
    """
    attempts: list[int] = []
    executions: list[int] = []

    async def slow() -> str:
        executions.append(1)
        await asyncio.sleep(4.0)  # ~2.7 × the ack wait
        return "done"

    async def body(ctx: Ctx, _input: Any) -> Any:
        attempts.append(ctx.attempt)
        return await ctx.run("slow", slow)

    wf = define_workflow("pywork1", body)
    _worker_a, client = await running([wf], ack_wait_ms=1_500, lease=False)
    await running([wf], ack_wait_ms=1_500, lease=False)  # a peer, free to grab a redelivery

    handle = await client.start(wf, None)
    assert await handle.result(RESULT_TIMEOUT_MS) == "done"
    await asyncio.sleep(2.0)  # give a straggling redelivery time to surface

    assert executions == [1], "the step ran exactly once — no mid-execution redelivery"
    assert attempts == [1], "and the run was driven exactly once, on its first delivery"


async def test_the_cap_releases_the_item_and_a_second_worker_takes_the_hung_run_over(
    running: Any,
) -> None:
    """The escape hatch, in the deployment that matters: two workers, the lease ON.

    Worker A's step hangs. Past ``max_work_ms`` A stops extending the ack AND stops renewing the
    lease, so the item redelivers and B — refused while A's lease was alive — takes the run over.
    Releasing only the ack extension would leave the lease holding every other worker off the item
    that just became redeliverable, which is the interaction this test exists to pin.
    """
    attempts: list[int] = []
    release = asyncio.Event()

    async def hang() -> str:
        await release.wait()
        return "released"

    async def body(ctx: Ctx, _input: Any) -> Any:
        attempts.append(ctx.attempt)
        return await ctx.run("hang", hang)

    wf = define_workflow("pywork2", body)
    opts: dict[str, Any] = {
        "ack_wait_ms": 800,
        "max_work_ms": 1_600,  # extend at most twice, then let go
        "lease_ttl_ms": 1_000,  # …and let the abandoned lease expire quickly
    }
    _worker_a, client = await running([wf], **opts)
    await running([wf], **opts)  # B, idle, waiting for the redelivery

    handle = await client.start(wf, None)
    await _wait_until(
        lambda: len(attempts) >= 2, timeout_s=25.0, what="the hung run to be taken over"
    )
    assert attempts[0] == 1
    assert attempts[1] >= 2, "the takeover was a genuine redelivery, not a fresh start"

    release.set()  # un-hang: both executions finish, one wins the CAS and the other adopts
    assert await handle.result(RESULT_TIMEOUT_MS) == "released"


async def test_the_cap_says_so_when_it_fires(running: Any, prefix: str) -> None:
    """Both releases are announced. A worker that quietly stops holding a run is unexplainable."""
    release = asyncio.Event()
    log = _CapturingLogger()

    async def hang() -> str:
        await release.wait()
        return "released"

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("hang", hang)

    wf = define_workflow("pywork3", body)
    _worker, client = await running(
        [wf], ack_wait_ms=800, max_work_ms=1_200, lease_ttl_ms=1_000, logger=log
    )

    handle = await client.start(wf, None)
    # durable-sdk: both waits re-expressed (docs/findings.md F10). The cap is the engine's now
    # (`serve --max-work`): past it the item's lease ends and the engine announces it in its own
    # log, so the first wait reads the engine's line. The release is the worker's: its next request
    # under that lease answers `expired` and it lets go of the item, loud — the second wait reads
    # that line from the worker's log. The ack extension and the run lease are one lease now.
    engine_lines: list[str] = []

    async def _engine_says_so() -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 15.0
        while loop.time() < deadline:
            engine_lines[:] = await engine_log(prefix)
            if any("exceeded max work" in line for line in engine_lines):
                return
            await asyncio.sleep(0.05)
        raise AssertionError("timed out waiting for the ack-extension cap to be logged")

    await _engine_says_so()
    await _wait_until(
        lambda: any("lease expired; dropping it" in line for line in log.lines),
        timeout_s=15.0,
        what="the lease release to be logged",
    )
    release.set()
    assert await handle.result(RESULT_TIMEOUT_MS) == "released"
