"""synadia-durable — a minimal Python worker, for the cross-language interop test.

The twin of ``conformance/interop/ts-worker.ts``. The TypeScript suite spawns this
(``test/integration/interop-py.test.ts``) and drives it with a TYPESCRIPT client: start, result,
status, signal, cancel. Nothing is shared between the two sides except the engine's API and the
protocol (``durable-engine`` ``docs/protocol/PROTOCOL.md``).

    NATS_URL=… DE_PREFIX=… uv run --project python python python/conformance/worker.py

Prints ``READY`` on stdout once it is polling, so the test can stop guessing. ``DE_PAYLOAD_KEY``
(hex) turns on encryption at rest, as the TypeScript worker's does.
"""

from __future__ import annotations

import asyncio
import os
import signal
from typing import Any

import nats

from synadia_durable import (
    Ctx,
    aes_gcm_payload_codec,
    create_worker,
    define_workflow,
    with_reconnect_defaults,
)


async def _greet(ctx: Ctx, input: dict[str, Any]) -> dict[str, Any]:
    """run + now + a returned object: proves values survive the round trip in both directions."""
    shouted = await ctx.run("shout", lambda: str(input["name"]).upper())
    at = await ctx.now()
    return {"greeting": f"HELLO {shouted}", "at": at, "from": "python"}


async def _echo_signal(ctx: Ctx, _input: Any) -> dict[str, Any]:
    """Parks on a signal and reports what it received."""
    data = str(await ctx.wait("card"))
    return {"len": len(data), "head": data[:24]}


async def _approve(ctx: Ctx, input: dict[str, Any]) -> dict[str, Any]:
    """Parks on a signal, so the TypeScript client can drive `signal` and `cancel` on a live run."""
    decision = await ctx.wait("decision")
    return {"order": input["order"], "approved": decision["ok"]}


async def _boom(_ctx: Ctx, _input: Any) -> None:
    """Fails, so the TypeScript client sees a terminal `fail` surface as WorkflowFailure."""
    raise RuntimeError("the carrier rejected the shipment")


greet = define_workflow("greet", _greet)
echo_signal = define_workflow("echo_signal", _echo_signal)
approve = define_workflow("approve", _approve)
boom = define_workflow("boom", _boom)


async def main() -> None:
    # NATS_CREDS: a creds file, when the server needs one (the harness's existing-server mode).
    creds = os.environ.get("NATS_CREDS")
    nc = await nats.connect(
        os.environ.get("NATS_URL", "nats://127.0.0.1:4222"),
        **with_reconnect_defaults(),
        **({"user_credentials": creds} if creds else {}),
    )
    key_hex = os.environ.get("DE_PAYLOAD_KEY")
    worker = create_worker(
        nc,
        [greet, approve, boom, echo_signal],
        prefix=os.environ.get("DE_PREFIX", "de"),
        payload_codec=aes_gcm_payload_codec(bytes.fromhex(key_hex)) if key_hex else None,
    )
    await worker.start()
    print("READY", flush=True)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    await worker.stop()
    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
