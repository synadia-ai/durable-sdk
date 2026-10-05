"""The core surface, in one runnable order workflow.

``ctx.run`` (durable steps) · ``ctx.wait`` (an external signal) · ``ctx.sleep`` (a durable timer) ·
``ctx.call`` (a sub-workflow), plus ``client.start`` / ``signal`` / ``result`` / ``status``.

    nats-server -js &
    cd python && NATS_URL=nats://127.0.0.1:4222 uv run python examples/charge.py

**Durability, in one command.** While it is parked at "await payment", kill the process and re-run it
with the SAME runId::

    DE_RUN_ID=order-1 uv run python examples/charge.py

It resumes from the journal: the completed steps replay without their functions running again.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import nats

from synadia_durable import (
    Ctx,
    StartOptions,
    create_client,
    create_worker,
    with_reconnect_defaults,
    workflow,
)


@workflow(name="ship")
async def ship(ctx: Ctx, order: dict[str, Any]) -> dict[str, str]:
    """A sub-workflow — an ordinary durable workflow in its own right, invoked below by ctx.call."""
    label = await ctx.run("print-label", lambda: f"LBL-{order['order_id']}")
    tracking = await ctx.run("dispatch", lambda: f"TRK-{order['order_id'][-4:]}")
    print(f"    [ship] {label} dispatched as {tracking}")
    return {"label": str(label), "tracking": str(tracking)}


@workflow(name="order")
async def order(ctx: Ctx, cart: dict[str, Any]) -> dict[str, Any]:
    # 1. A durable step: its result is journaled, and on replay it is READ rather than re-run.
    def reserve() -> str:
        print(f"  [order] reserved inventory for {cart['cart_id']}")
        return f"ord-{cart['cart_id']}"

    order_id = await ctx.run("reserve-inventory", reserve)

    # 2. Block until an external signal arrives (a payment webhook, say).
    #    This print is in the workflow BODY, not a step, so it re-prints on every replay — which is
    #    exactly why durable side effects belong inside ctx.run and not out here.
    print('  [order] awaiting payment — send client.signal(run_id, "payment", …)')
    payment = await ctx.wait("payment")
    if not payment["approved"]:
        return {"status": "abandoned", "order_id": order_id}

    # 3. A durable timer. The deadline is journaled and armed on the server; it survives restarts.
    await ctx.sleep("settle-delay", 500)

    # 4. A sub-workflow: its own run, its own journal, its result delivered back here.
    shipment = await ctx.call(ship, {"order_id": order_id})

    return {"status": "shipped", "order_id": order_id, "tracking": shipment["tracking"]}


async def main() -> None:
    url = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
    # NATS_CREDS: a creds file, for the fabric (docs/FABRIC.md; needs nkeys, findings F30); absent, a plain local server.
    creds = os.environ.get("NATS_CREDS")
    nc = await nats.connect(
        url, **({"user_credentials": creds} if creds else {}), **with_reconnect_defaults()
    )
    print(f"connected to {url}")

    # One worker process hosts many workflows: register the order and the sub-workflow it calls.
    worker = create_worker(nc, [order, ship])
    try:
        await worker.start()
        client = create_client(nc)

        run_id = os.environ.get("DE_RUN_ID")
        handle = await client.start(
            order,
            {"cart_id": "c-42", "amount": 19.99},
            StartOptions(run_id=run_id) if run_id else None,
        )
        print(f"started order {handle.run_id}")

        # The payment webhook fires a moment later. Over a slow link the signal may land before the
        # workflow reaches its `wait`: the engine journals it and the run takes it on the next
        # replay (durable-engine F51), so the 0.4 s is for the narration, not for correctness.
        await asyncio.sleep(0.4)
        print("approving payment via client.signal(...)")
        await client.signal(handle.run_id, "payment", {"approved": True})

        print("result:", await handle.result(20_000))
        print("status:", await handle.status())
    finally:
        await worker.stop()
        await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
