"""Large-payload offload, WRITE side (PROTOCOL §5.6), against a real nats-server.

A value bigger than the threshold is written to the ``DE_BLOBS`` object store FIRST and the wire
carries a content-addressed ref instead — in the journal, on the work queue, and in the status hint,
because all three have the same size limit and a value that overflows one usually overflows the next.

The tests use a deliberately tiny threshold (1 KiB, the configured floor) so the paths are exercised
in milliseconds rather than by pushing megabytes around. What that changes is only WHEN offload
triggers; every byte of the mechanism is the same one a 1 MB payload would take.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from conftest import over_payload
from engine import create_client, create_worker
from shell.names import Names
from shell.runs import RunsStore
from synadia_durable import (
    PayloadTooLargeError,
    define_workflow,
)
from synadia_durable.journal.blobs import OffloadOptions, entry_ref
from synadia_durable.types import Ctx
from synadia_durable.version import WIRE_VERSION_BLOBS

pytestmark = pytest.mark.asyncio

MB = 1024 * 1024
RESULT_TIMEOUT_MS = 30_000
#: Clamped to MIN_OFFLOAD_THRESHOLD_BYTES; anything with a real payload lands above it.
TINY = OffloadOptions(threshold_bytes=1)
#: Comfortably over the threshold once encoded, small enough to keep the suite quick.
BIG = {"frames": ["x" * 400] * 8}


@pytest.fixture
async def running(nc: Any, prefix: str) -> Any:
    async def _make(workflows: list[Any], **kwargs: Any) -> tuple[Any, Any]:
        kwargs.setdefault("offload", TINY)
        worker = create_worker(nc, workflows, prefix=prefix, stream_max_bytes=64 * MB, **kwargs)
        await worker.start()
        made.append(worker)
        client = create_client(
            nc, prefix=prefix, stream_max_bytes=64 * MB, offload=kwargs["offload"]
        )
        return worker, client

    made: list[Any] = []
    try:
        yield _make
    finally:
        for worker in made:
            await worker.stop()


async def _raw_entries(nc: Any, prefix: str, run_id: str) -> list[dict[str, Any]]:
    """The journal as it sits on the stream — refs NOT resolved, which is the point."""
    from shell.journal_store import NatsJournal

    journal = NatsJournal(nc, Names(prefix))
    return [dict(le.entry) for le in await journal.load_raw(run_id)]


async def _object_names(nc: Any, prefix: str) -> list[str]:
    from conftest import list_object_names

    return await list_object_names(nc, Names(prefix).blobs_bucket)


# durable-sdk: docs/findings.md F1/F2, answered by the engine's F25 (put and get) and F22
# (serve --offload-threshold), with the SDK's chunked transport (_engine/blobs.py).
async def test_a_big_step_value_is_offloaded_and_replays_inline(
    nc: Any, prefix: str, running: Any
) -> None:
    """The core round trip: the stream carries a ref, the engine never sees one."""
    executions: list[int] = []

    def make_big() -> Any:
        executions.append(1)
        return BIG

    async def body(ctx: Ctx, _input: Any) -> Any:
        big = await ctx.run("big", make_big)
        # A second command forces a replay of the offloaded step-end before the run can finish.
        await ctx.sleep("brief", 1)
        return {"frames": len(big["frames"]), "first": big["frames"][0][:3]}

    wf = define_workflow("pyoff1", body)
    _worker, client = await running([wf])

    handle = await client.start(wf, None)
    assert await handle.result(RESULT_TIMEOUT_MS) == {"frames": 8, "first": "xxx"}
    assert executions == [1], "the step replayed from its blob rather than re-running"

    raw = await _raw_entries(nc, prefix, handle.run_id)
    step_end = next(e for e in raw if e["kind"] == "step-end")
    assert step_end["v"] == WIRE_VERSION_BLOBS, "an offloaded entry is stamped v:2"
    assert "value" not in step_end, "the payload slot is gone from the stream"
    ref = entry_ref(step_end)  # type: ignore[arg-type]
    assert ref is not None
    field, blob = ref
    assert field == "valueRef"
    assert blob["name"].startswith(f"{handle.run_id}/sha256:")
    assert blob["size"] > 0


# durable-sdk: docs/findings.md F1/F2, answered by the engine's F25 (put and get) and F22
# (serve --offload-threshold), with the SDK's chunked transport (_engine/blobs.py).
async def test_an_oversized_start_input_and_signal_travel_as_refs(
    nc: Any, prefix: str, running: Any
) -> None:
    """Both client-side write paths: the start entry's input, and a signal's payload on the queue."""

    async def body(ctx: Ctx, order: Any) -> Any:
        got = await ctx.wait("payload")
        return {"input_frames": len(order["frames"]), "signal_frames": len(got["frames"])}

    wf = define_workflow("pyoff2", body)
    _worker, client = await running([wf])

    handle = await client.start(wf, BIG)
    await asyncio.sleep(0.5)
    await client.signal(handle.run_id, "payload", BIG)
    assert await handle.result(RESULT_TIMEOUT_MS) == {"input_frames": 8, "signal_frames": 8}

    raw = await _raw_entries(nc, prefix, handle.run_id)
    start = next(e for e in raw if e["kind"] == "start")
    signal = next(e for e in raw if e["kind"] == "signal")
    assert "inputRef" in start
    assert "input" not in start
    # The signal was shrunk on the QUEUE by the client, and the worker journaled the ref verbatim —
    # it never re-inflates a payload just to write it back out.
    assert "dataRef" in signal
    assert "data" not in signal


# durable-sdk: docs/findings.md F1/F2, answered by the engine's F25 (put and get) and F22
# (serve --offload-threshold), with the SDK's chunked transport (_engine/blobs.py).
async def test_a_big_child_result_reaches_its_parent_as_a_ref(
    nc: Any, prefix: str, running: Any
) -> None:
    """The child-done work item is the third path with the same size limit."""

    async def child_body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("make", lambda: BIG)

    async def parent_body(ctx: Ctx, _input: Any) -> Any:
        got = await ctx.call(child, {})
        return len(got["frames"])

    child = define_workflow("pyoff3c", child_body)
    parent = define_workflow("pyoff3p", parent_body)
    _worker, client = await running([parent, child])

    handle = await client.start(parent, None)
    assert await handle.result(RESULT_TIMEOUT_MS) == 8

    raw = await _raw_entries(nc, prefix, handle.run_id)
    resolve = next(e for e in raw if e["kind"] == "resolve")
    assert "valueRef" in resolve
    assert "value" not in resolve


# durable-sdk: docs/findings.md F1/F2, answered by the engine's F25 (put and get) and F22
# (serve --offload-threshold), with the SDK's chunked transport (_engine/blobs.py).
async def test_a_big_result_does_not_wedge_the_status_hint(
    nc: Any, prefix: str, running: Any
) -> None:
    """The KV record is offloaded for a reason: a failed put NAK-loops the terminal re-drive forever.

    It converges on the SAME object as the journal's terminal — identical value, identical codec,
    identical content-addressed name — so the put dedups instead of storing a second copy.
    """

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("big", lambda: BIG)

    wf = define_workflow("pyoff4", body)
    _worker, client = await running([wf])

    handle = await client.start(wf, None)
    assert await handle.result(RESULT_TIMEOUT_MS) == BIG
    assert (await handle.status()).state == "completed"

    record = await RunsStore(await nc.jetstream().key_value(Names(prefix).runs_kv)).get(
        handle.run_id
    )
    assert record is not None
    assert "resultRef" in record
    assert "result" not in record

    names = await _object_names(nc, prefix)
    assert record["resultRef"]["name"] in names
    # step-end value, terminal result and status record are all the same bytes → ONE object.
    assert len(names) == 1, f"content addressing should have deduped these: {names}"


async def test_offload_disabled_still_fails_loud_on_an_oversized_payload(
    nc: Any, prefix: str
) -> None:
    """The pre-offload contract is preserved, because losing it would be silent data growth.

    With offload off there is nowhere for a big value to go, so it must fail deterministically
    rather than NAK-loop against ``max_payload`` forever.
    """
    huge = "z" * over_payload(nc, 2)

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("huge", lambda: huge)

    wf = define_workflow("pyoff5", body)
    worker = create_worker(nc, [wf], prefix=prefix, stream_max_bytes=64 * MB, offload=False)
    await worker.start()
    try:
        client = create_client(nc, prefix=prefix, stream_max_bytes=64 * MB, offload=False)
        handle = await client.start(wf, None)
        with pytest.raises(Exception) as caught:  # noqa: PT011 — the type is asserted below
            await handle.result(RESULT_TIMEOUT_MS)
        # The run fails terminally with the serialization/size error, rather than hanging.
        assert "PayloadTooLarge" in str(caught.value) or isinstance(
            caught.value, PayloadTooLargeError
        )
    finally:
        await worker.stop()


# durable-sdk: docs/findings.md F1/F2, answered by the engine's F25 (put and get) and F22
# (serve --offload-threshold), with the SDK's chunked transport (_engine/blobs.py).
async def test_the_same_value_in_two_runs_is_stored_per_run(
    nc: Any, prefix: str, running: Any
) -> None:
    """The runId prefix is the GC unit, so identical content in two runs is stored twice on purpose.

    Deleting a run's journal plus its ``{runId}/`` objects has to be complete and has to be safe;
    sharing one object across runs would make it neither.
    """

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("big", lambda: BIG)

    wf = define_workflow("pyoff6", body)
    _worker, client = await running([wf])

    first = await client.start(wf, None)
    second = await client.start(wf, None)
    assert await first.result(RESULT_TIMEOUT_MS) == BIG
    assert await second.result(RESULT_TIMEOUT_MS) == BIG

    names = await _object_names(nc, prefix)
    assert len(names) == 2, names
    assert {n.split("/", 1)[0] for n in names} == {first.run_id, second.run_id}
    assert len({n.split("/", 1)[1] for n in names}) == 1, "same content, same digest"
