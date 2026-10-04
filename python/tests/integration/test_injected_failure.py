"""durable-sdk: two tests that FAIL ON PURPOSE, run only under ``DE_INJECT_FAILURE=1``, to prove
the existing-server mode's cleanup (docs/findings.md F33): a test that raises with its worker still
running and its runs' blobs written, and one whose ``result()`` times out while its worker is still
putting blobs. The run is expected to report these two as failed and to leave nothing behind on the
prefix: no message in any of its seven, no durable. The twin of
``test/integration/injected-failure.test.ts``.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import pytest

from conftest import over_payload
from engine import create_client, create_worker
from synadia_durable import define_workflow
from synadia_durable.types import Ctx

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        os.environ.get("DE_INJECT_FAILURE") != "1", reason="DE_INJECT_FAILURE=1 only"
    ),
]


async def test_raises_with_its_worker_left_running_and_its_runs_blobs_written(
    nc: Any, prefix: str
) -> None:
    big = "t" * over_payload(nc, 1.5)

    async def body(ctx: Ctx, _input: Any) -> Any:
        value = await ctx.run("big", lambda: big)
        await ctx.wait("never")
        return len(value)

    wf = define_workflow("pyinj_throw", body)
    worker = create_worker(nc, [wf], prefix=prefix)
    await worker.start()
    handle = await create_client(nc, prefix=prefix).start(wf, None)
    for _ in range(100):
        if (await handle.status()).state == "suspended":
            break
        await asyncio.sleep(0.1)
    raise AssertionError("injected: the test fails with its worker running and a blob in the store")


async def test_times_out_while_its_worker_is_still_putting_blobs(nc: Any, prefix: str) -> None:
    size = over_payload(nc, 1.5)

    async def body(ctx: Ctx, _input: Any) -> Any:
        n = 0
        for i in range(40):
            n += len(await ctx.run(f"big-{i}", lambda i=i: str(i) * size))
            await ctx.sleep(f"nap-{i}", 50)
        return n

    wf = define_workflow("pyinj_timeout", body)
    worker = create_worker(nc, [wf], prefix=prefix)
    await worker.start()
    handle = await create_client(nc, prefix=prefix).start(wf, None)
    await handle.result(3_000)  # never within 3 s: DurableTimeoutError, the worker still running
