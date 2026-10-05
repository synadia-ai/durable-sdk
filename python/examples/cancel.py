"""Cancellation — ``client.cancel`` stops a run, and ``result()`` tells you so.

A cancel is an uncatchable hard-stop at the run's next COMMAND BOUNDARY: recorded commands still
replay, but the next fresh ``ctx.*`` op halts before executing. A workflow's ``try/except`` cannot
swallow it — cancellation is out-of-band, not an exception thrown through your code. To clean up,
register a compensation with ``ctx.on_cancel`` (see ``saga_cancel.py``).

    nats-server -js &
    cd python && NATS_URL=nats://127.0.0.1:4222 uv run python examples/cancel.py
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import nats

from synadia_durable import (
    CancelledError,
    Ctx,
    create_client,
    create_worker,
    with_reconnect_defaults,
    workflow,
)


@workflow(name="long-job")
async def long_job(ctx: Ctx, _input: Any) -> str:
    await ctx.run("step-1", lambda: print("  [long-job] step 1 done"))
    # In the BODY, not a step — so it prints again when the cancel re-drives the run from the top.
    print("  [long-job] now waiting up to an hour for approval…")
    await ctx.sleep("wait", 60 * 60 * 1000)  # an hour — we cancel long before it fires
    return "finished"


async def main() -> None:
    url = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
    # NATS_CREDS: a creds file, for the fabric (docs/FABRIC.md; needs nkeys, findings F30); absent, a plain local server.
    creds = os.environ.get("NATS_CREDS")
    nc = await nats.connect(
        url, **({"user_credentials": creds} if creds else {}), **with_reconnect_defaults()
    )
    print(f"connected to {url}")
    worker = create_worker(nc, [long_job])
    try:
        await worker.start()
        client = create_client(nc)

        handle = await client.start(long_job, None)
        print(f"started {handle.run_id}")

        await asyncio.sleep(0.5)  # let it park on the sleep
        print("status before cancel:", (await handle.status()).state)
        print("cancelling…")
        await client.cancel(handle.run_id, "changed my mind")

        try:
            await handle.result(15_000)
        except CancelledError:
            # synadia_durable.CancelledError — deliberately NOT asyncio.CancelledError, which asyncio
            # throws into coroutines itself. Conflating the two would make a task cancellation
            # indistinguishable from a durable one.
            print("result() raised: CancelledError")
        print("status after cancel:", (await handle.status()).state)  # "cancelled"
    finally:
        await worker.stop()
        await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
