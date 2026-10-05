"""Durable fan-out: many children at once, and a race against a durable timer.

``ctx.start_call`` returns a future whose child starts immediately; ``asyncio.gather`` waits for all
of them; ``ctx.race`` takes whichever settles first — by JOURNAL order, so a replay picks the same
winner.

    nats-server -js &
    cd python && NATS_URL=nats://127.0.0.1:4222 uv run python examples/fan_out.py

Every child starts in ONE pass, before anything is awaited. The run then suspends once, and each
child's completion re-drives it until every branch has resolved — real parallelism over the work
queue, not concurrency inside one process.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import nats

from synadia_durable import Ctx, create_client, create_worker, with_reconnect_defaults, workflow


@workflow(name="task")
async def task(ctx: Ctx, n: int) -> int:
    return int(await ctx.run("compute", lambda: n * n))


@workflow(name="batch")
async def batch(ctx: Ctx, items: dict[str, Any]) -> int:
    numbers = list(items["numbers"])
    # start_call is SYNC: every child is started here, before the first await. Making these
    # coroutines instead would start the second child only after the first had suspended.
    futures = [ctx.start_call(task, n) for n in numbers]
    squares = await asyncio.gather(*futures)  # suspends until ALL of them have resolved
    total = sum(int(s) for s in squares)
    print(f"  [batch] {numbers} → squares {[int(s) for s in squares]} → total {total}")
    return total


@workflow(name="raced")
async def raced(ctx: Ctx, n: int) -> str:
    """The timeout pattern: a child against a durable timer, whichever settles first."""
    winner = await ctx.race([ctx.start_call(task, n), ctx.start_sleep("timeout", 5_000)])
    return f"task won ({winner})" if winner is not None else "timed out"


async def main() -> None:
    url = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
    # NATS_CREDS: a creds file, for the fabric (docs/FABRIC.md; needs nkeys, findings F30); absent, a plain local server.
    creds = os.environ.get("NATS_CREDS")
    nc = await nats.connect(
        url, **({"user_credentials": creds} if creds else {}), **with_reconnect_defaults()
    )
    print(f"connected to {url}")
    worker = create_worker(nc, [batch, task, raced])
    try:
        await worker.start()
        client = create_client(nc)

        fan = await client.start(batch, {"numbers": [2, 3, 4, 5, 6]})
        print("batch total:", await fan.result(20_000))

        race = await client.start(raced, 9)
        print("race:", await race.result(20_000))
    finally:
        await worker.stop()
        await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
