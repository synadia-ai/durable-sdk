"""Saga / compensation — a distributed transaction with no coordinator.

``book-trip`` runs three forward steps (reserve-flight → charge-card → reserve-hotel). If a later
step cannot complete, the already-committed steps are undone by running their COMPENSATIONS in
reverse order — and because each compensation is itself a durable ``ctx.run``, a crash mid-unwind
resumes the unwind from the journal. The rollback is as crash-safe as the forward path.

    nats-server -js &
    cd python && NATS_URL=nats://127.0.0.1:4222 uv run python examples/saga.py

It seeds two runs: ``py-trip-booked`` (all three steps succeed) and ``py-trip-rolledback`` (no rooms
→ unwind), then stops. Both journals stay in NATS for the dashboard.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
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


@workflow(name="book-trip")
async def book_trip(ctx: Ctx, trip: dict[str, Any]) -> dict[str, str]:
    name = str(trip["trip"])
    rooms = int(trip["rooms"])  # how many rooms are free (0 → the booking cannot complete)

    # A LIFO stack of compensations: each forward step, once committed, pushes how to undo itself.
    # On any failure we pop them in reverse. Each undo is a durable step in its own right.
    undo: list[tuple[str, Callable[[], str]]] = []

    try:

        def reserve_flight() -> str:
            print(f"  ✈  reserve-flight    {name}: seat held → FL-{name}")
            return f"FL-{name}"

        flight = await ctx.run("reserve-flight", reserve_flight)

        def cancel_flight() -> str:
            print(f"     ↩  cancel-flight     released {flight}")
            return f"released {flight}"

        undo.append(("cancel-flight", cancel_flight))

        def charge_card() -> str:
            print(f"  💳  charge-card       {name}: $1,200 captured → CHG-{name}")
            return f"CHG-{name}"

        charge = await ctx.run("charge-card", charge_card)

        def refund_card() -> str:
            print(f"     ↩  refund-card       refunded {charge}")
            return f"refunded {charge}"

        undo.append(("refund-card", refund_card))

        def reserve_hotel() -> dict[str, Any]:
            ok = rooms > 0
            held = f"room held → HT-{name}" if ok else "NO ROOMS AVAILABLE"
            print(f"  🏨  reserve-hotel     {name}: {held}")
            return {"ok": ok, "ref": f"HT-{name}" if ok else None}

        hotel = await ctx.run("reserve-hotel", reserve_hotel)
        if not hotel["ok"]:
            raise RuntimeError("hotel sold out")

        return {"status": "booked", "detail": f"FL-{name} · CHG-{name} · {hotel['ref']}"}

    except Exception as err:  # noqa: BLE001 — a saga unwinds on ANY forward failure, by design
        # Broad, and deliberately so: whatever went wrong past a committed step, the committed work
        # has to come back. It is also SAFE to be broad here — suspension, fencing, retry and cancel
        # are out-of-band in this engine, never raised through your code, so this `except` can only
        # ever see a workflow-logic error. It cannot swallow the engine's control flow.
        reason = str(err)
        print(f"  ✖  {name}: {reason} — unwinding {len(undo)} step(s) in reverse:")
        # Compensate in REVERSE. Each undo is a ctx.run, so the unwind is journaled and crash-safe:
        # a crash halfway through resumes with the already-run compensations replayed, not repeated.
        for step_name, fn in reversed(undo):
            await ctx.run(step_name, fn)
        return {"status": "rolled-back", "detail": f"{reason} → {len(undo)} step(s) compensated"}


async def main() -> None:
    url = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
    nc = await nats.connect(url, **with_reconnect_defaults())
    worker = create_worker(nc, [book_trip])
    await worker.start()
    client = create_client(nc)

    print("\n— py-trip-booked (4 rooms free) —")
    ok = await client.start(
        book_trip, {"trip": "PAR", "rooms": 4}, StartOptions(run_id="py-trip-booked")
    )

    print("\n— py-trip-rolledback (0 rooms free) —")
    bad = await client.start(
        book_trip, {"trip": "TYO", "rooms": 0}, StartOptions(run_id="py-trip-rolledback")
    )

    # Await both BEFORE the header, so the steps' narration stays above it: the two runs execute
    # concurrently on this one worker, and their output interleaves as they go.
    booked, rolled_back = await ok.result(10_000), await bad.result(10_000)
    print("\noutcomes:")
    print("  py-trip-booked     →", booked)
    print("  py-trip-rolledback →", rolled_back)

    await worker.stop()
    await nc.drain()
    print(f"\n✅ seeded the saga demo at {url} — open the dashboard to inspect both journals")


if __name__ == "__main__":
    asyncio.run(main())
