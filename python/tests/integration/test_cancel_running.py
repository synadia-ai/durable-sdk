"""A cancel sent while a run's step is RUNNING (the fabric test repo's F23). PROTOCOL §11.7 step 2.

The cancel must reach the journal while that execution still holds the run, not wait behind it in
the worker's per-run chain. Waiting behind it meant the execution never saw the cancel — the run
COMPLETED, every later step ran after the cancel was stored, and no ``cancel-requested`` was ever
journaled. The twin of ``test/integration/cancel-running.test.ts``.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from engine import create_client, create_worker
from shell.journal_store import NatsJournal
from shell.names import Names
from synadia_durable import CancelledError, define_workflow
from synadia_durable.types import Ctx

pytestmark = pytest.mark.asyncio

MB = 1024 * 1024
RESULT_TIMEOUT_MS = 20_000


async def _cancel_while_held(nc: Any, prefix: str, worker_count: int) -> None:
    effects = {"before": 0, "hold": 0, "after": 0, "undo": 0}
    holding = asyncio.Event()
    gate = asyncio.Event()

    def bump(key: str) -> None:
        effects[key] += 1

    async def hold() -> str:
        bump("hold")
        holding.set()
        await gate.wait()
        return "held"

    async def body(ctx: Ctx, _input: Any) -> str:
        ctx.on_cancel(lambda: ctx.run("undo", bump, "undo"))
        await ctx.run("before", bump, "before")
        await ctx.run("hold", hold)
        await ctx.run("after", bump, "after")
        return "done"

    wf = define_workflow(f"pycancel_running_{worker_count}", body)
    workers = [
        create_worker(nc, [wf], prefix=prefix, stream_max_bytes=64 * MB)
        for _ in range(worker_count)
    ]
    for w in workers:
        await w.start()
    try:
        client = create_client(nc, prefix=prefix, stream_max_bytes=64 * MB)
        handle = await client.start(wf, {})
        await asyncio.wait_for(holding.wait(), 15)  # the step's effect is running on some worker
        await client.cancel(handle.run_id, reason="operator stop")
        acked_at = time.monotonic()

        journal = NatsJournal(nc, Names(prefix))

        async def requested() -> bool:
            loaded = await journal.load_raw(handle.run_id)
            return any(le.entry["kind"] == "cancel-requested" for le in loaded)

        # The cancel is journaled WHILE the step is still held — not after the execution ends.
        deadline = time.monotonic() + 5
        while not await requested():
            if time.monotonic() > deadline:
                gate.set()  # let the run finish so teardown is clean, then say what it did instead
                try:
                    outcome = f"completed with {await handle.result(RESULT_TIMEOUT_MS)!r}"
                except Exception as e:  # noqa: BLE001 — reported, not handled
                    outcome = f"raised {e!r}"
                raise AssertionError(
                    "cancel-requested was not journaled while the step was held; "
                    f"the run {outcome}, effects {effects}"
                )
            await asyncio.sleep(0.025)
        gate.set()

        with pytest.raises(CancelledError):
            await handle.result(RESULT_TIMEOUT_MS)
        took_ms = round((time.monotonic() - acked_at) * 1000)
        print(f"cancel ack -> cancelled terminal, {worker_count} worker(s): {took_ms} ms")

        loaded = await journal.load_raw(handle.run_id)
        entries = [le.entry for le in loaded]
        kinds = [e["kind"] for e in entries]
        hold_seq = next(
            e["seq"] for e in entries if e["kind"] == "step-begin" and e.get("name") == "hold"
        )
        creq = kinds.index("cancel-requested")
        hold_end = next(
            (i for i, e in enumerate(entries) if e["kind"] == "step-end" and e["seq"] == hold_seq),
            None,
        )
        # No step-end for the held step was ever accepted — or, if one was, only after the request.
        assert hold_end is None or hold_end > creq
        assert effects == {"before": 1, "hold": 1, "after": 0, "undo": 1}
        terminal = entries[-1]
        assert terminal["kind"] == "cancelled"
        assert terminal.get("reason") == "operator stop"
        assert (await handle.status()).state == "cancelled"
    finally:
        gate.set()
        for w in workers:
            await w.stop()


async def test_one_worker_a_cancel_lands_while_a_step_is_held(nc: Any, prefix: str) -> None:
    await _cancel_while_held(nc, prefix, 1)


async def test_two_workers_a_cancel_lands_while_a_step_is_held(nc: Any, prefix: str) -> None:
    await _cancel_while_held(nc, prefix, 2)
