"""Crash and replay — the headline of durable execution, in two commands.

Three steps. Each prints ONLY when its function actually executes, so the console shows what
replayed and what re-ran. Run both phases against the SAME runId::

    nats-server -js &
    cd python
    # phase 1 — a worker runs two steps, then "crashes" before the third:
    CRASH=1 DE_RUN_ID=crash-demo NATS_URL=nats://127.0.0.1:4222 uv run python examples/crash_replay.py
    # phase 2 — a FRESH process picks the run back up and finishes it:
          DE_RUN_ID=crash-demo NATS_URL=nats://127.0.0.1:4222 uv run python examples/crash_replay.py

In phase 2 the two completed steps are **replayed** — their functions never run again, so nothing
prints for them — and only ``attach`` executes. The result stitches together values produced in two
different processes, with each side effect having fired exactly once.
"""

from __future__ import annotations

import asyncio
import os
import sys
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


@workflow(name="provision")
async def provision(ctx: Ctx, vm: dict[str, Any]) -> dict[str, str]:
    # The body runs top to bottom on EVERY incarnation, including each resume — it is just control
    # flow. The durable side effects live inside ctx.run, so they happen once and replay thereafter.
    print(f"→ workflow entered for {vm['name']}")

    def allocate_ip() -> str:
        print("   ⚡ allocate-ip   EXECUTED")
        return "10.0.0.7"

    def create_disk() -> str:
        print("   ⚡ create-disk   EXECUTED")
        return "disk-42"

    def attach() -> str:
        print("   ⚡ attach        EXECUTED")
        return "attached"

    ip = await ctx.run("allocate-ip", allocate_ip)
    disk = await ctx.run("create-disk", create_disk)

    if os.environ.get("CRASH") == "1":
        print("   ✗ CRASH — the worker dies here, before 'attach'")
        # os._exit is the honest simulation: no cleanup, no ack, nothing drained — exactly what a
        # kill -9 leaves behind. It also skips the stdout flush Python does at exit, so this demo
        # has to flush by hand or the lines above vanish when stdout is a pipe.
        sys.stdout.flush()
        os._exit(1)

    status = await ctx.run("attach", attach)
    return {"ip": str(ip), "disk": str(disk), "status": str(status)}


async def main() -> None:
    url = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
    # NATS_CREDS: a creds file, for the fabric (docs/FABRIC.md; needs nkeys); absent, a plain local server.
    creds = os.environ.get("NATS_CREDS")
    nc = await nats.connect(
        url, **({"user_credentials": creds} if creds else {}), **with_reconnect_defaults()
    )
    # A low ack wait so phase 2 re-picks up the item the crashed process was holding in ~2 s rather
    # than the default 30. In production the default is the right trade; here it keeps the demo brisk.
    worker = create_worker(nc, [provision], ack_wait_ms=2_000, lease_ttl_ms=2_000)
    try:
        await worker.start()
        client = create_client(nc)

        run_id = os.environ.get("DE_RUN_ID", "crash-demo")
        handle = await client.start(provision, {"name": "vm-1"}, StartOptions(run_id=run_id))
        print(f"run {handle.run_id}")

        print("result:", await handle.result(30_000))
    finally:
        # Not reached in phase 1: the simulated crash (os._exit above) skips it on purpose.
        await worker.stop()
        await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
