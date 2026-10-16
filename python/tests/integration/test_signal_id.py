"""The signal ``id`` (durable-engine #19: signals journaled at send, deduplicated by ``id``; its
docs/API.md "signal"). The SDK sends an id on every signal, the caller's or a generated UUID, so a
caller that retries after ``EngineUnavailableError`` can pass the id of the first try. The twin of
``test/integration/signal-id.test.ts``.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

import pytest

import existing
from engine import create_client, create_worker
from shell.journal_store import NatsJournal
from shell.names import Names
from synadia_durable import define_workflow
from synadia_durable.errors import DurableTimeoutError
from synadia_durable.types import Ctx

MB = 1024 * 1024
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


async def _until(cond: Any, timeout_s: float, what: str) -> None:
    deadline = asyncio.get_running_loop().time() + existing.scaled(timeout_s)
    while asyncio.get_running_loop().time() < deadline:
        if await cond():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"timed out waiting for {what}")


async def _signal_entries(nc: Any, prefix: str, run_id: str, name: str) -> list[dict[str, Any]]:
    loaded = await NatsJournal(nc, Names(prefix)).load(run_id)
    return [
        dict(le.entry)
        for le in loaded
        if le.entry["kind"] == "signal" and le.entry.get("name") == name
    ]


async def _twice(ctx: Ctx, _input: Any) -> list[Any]:
    """Parks on "gate" while the "go" signals are journaled, then takes one "go" and waits briefly
    for a second: what the run took is its result."""
    await ctx.wait("gate")
    first = await ctx.wait("go")
    try:
        return [first, await ctx.wait("go", timeout_ms=1_500)]
    except DurableTimeoutError:
        return [first]


async def _rolls(ctx: Ctx, state: dict[str, Any]) -> Any:
    """Generation 0 takes one "go", then continues; generation 1 parks."""
    if state["n"] == 0:
        await ctx.wait("go")
        await ctx.continue_as_new({"n": 1})
        raise AssertionError("unreachable")
    return await ctx.wait("never")


async def _suspended(client: Any, run_id: str) -> bool:
    return bool((await client.get(run_id).status()).state == "suspended")


async def test_two_signals_with_the_same_id_journal_one_entry(nc: Any, prefix: str) -> None:
    wf = define_workflow("pysigid_twice", _twice)
    worker = create_worker(nc, [wf], prefix=prefix, stream_max_bytes=64 * MB)
    await worker.start()
    try:
        client = create_client(nc, prefix=prefix)
        h = await client.start(wf, {})
        await _until(lambda: _suspended(client, h.run_id), 10.0, "the run parked")
        await client.signal(h.run_id, "go", "a", id="go-1")
        await client.signal(h.run_id, "go", "a", id="go-1")  # the retry: ok, nothing journaled
        await client.signal(h.run_id, "gate", None)
        assert await h.result(15_000) == ["a"]
        entries = await _signal_entries(nc, prefix, h.run_id, "go")
        assert len(entries) == 1, entries
        assert entries[0]["signalId"] == "go-1"
    finally:
        await worker.stop()


async def test_two_signals_without_an_id_are_two_signals(nc: Any, prefix: str) -> None:
    wf = define_workflow("pysigid_twice", _twice)
    worker = create_worker(nc, [wf], prefix=prefix, stream_max_bytes=64 * MB)
    await worker.start()
    try:
        client = create_client(nc, prefix=prefix)
        h = await client.start(wf, {})
        await _until(lambda: _suspended(client, h.run_id), 10.0, "the run parked")
        await client.signal(h.run_id, "go", "a")
        await client.signal(h.run_id, "go", "b")
        await client.signal(h.run_id, "gate", None)
        assert await h.result(15_000) == ["a", "b"]
        entries = await _signal_entries(nc, prefix, h.run_id, "go")
        assert len(entries) == 2, entries
        ids = [e["signalId"] for e in entries]
        assert len(set(ids)) == 2
        assert all(UUID.fullmatch(i) for i in ids), ids
    finally:
        await worker.stop()


@pytest.mark.parametrize("bad", ["", "has space", "a.b", "a~b", "line\n"])
async def test_an_invalid_id_is_refused_before_any_request(nc: Any, prefix: str, bad: str) -> None:
    client = create_client(nc, prefix=prefix)
    sent: list[str] = []

    async def seen(msg: Any) -> None:
        sent.append(msg.subject)

    # A subscriber of its own on the signal subject sees every request the client would send.
    sub = await nc.subscribe(f"{prefix}.api.v1.signal", cb=seen)
    await nc.flush()
    try:
        with pytest.raises(ValueError, match=r"^signal id must be ASCII alphanumeric"):
            await client.signal("some-run", "go", 1, id=bad)
        await nc.flush()
        assert sent == []
    finally:
        await sub.unsubscribe()


async def test_a_retry_after_a_rollover_is_recognised_in_the_run_named(
    nc: Any, prefix: str
) -> None:
    wf = define_workflow("pysigid_rolls", _rolls)
    worker = create_worker(nc, [wf], prefix=prefix, stream_max_bytes=64 * MB)
    await worker.start()
    try:
        client = create_client(nc, prefix=prefix)
        h = await client.start(wf, {"n": 0})
        await _until(lambda: _suspended(client, h.run_id), 10.0, "generation 0 parked")
        # The first try is journaled in generation 0, the run the retry names too: one of the two
        # journals the engine looks in (the other, the generation current at the retry, is _g1).
        await client.signal(h.run_id, "go", "first", id="roll-1")
        gen1 = f"{h.run_id}_g1"
        await _until(lambda: _suspended(client, gen1), 10.0, "generation 1 parked")
        await client.signal(h.run_id, "go", "first", id="roll-1")  # the retry: answered ok
        assert len(await _signal_entries(nc, prefix, h.run_id, "go")) == 1
        assert await _signal_entries(nc, prefix, gen1, "go") == []
        assert await _suspended(client, gen1)
    finally:
        await worker.stop()
