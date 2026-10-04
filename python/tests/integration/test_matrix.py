"""The feature matrix crossings that no single-feature suite reaches. Twin of
``test/integration/matrix.test.ts``.

Each feature has its own suite here and each is well covered ON ITS OWN. What none of them cover is
the feature under CONCURRENCY: a versioned run driven by two workers at once, a rollover raced at
the boundary, a cancel arriving while another worker is mid-step, an offloaded payload under a
signal storm. Those are exactly the situations the engine's one primitive exists for, and testing a
feature only in isolation is how a bug survives — the 0.6.1 lost-event bug and the continue-as-new
adopt bug were both invisible until two workers touched the same run.

Every test ends in the same audit rather than in "the result was right": a chaos test that only
checks the return value would miss a duplicated ``step-end`` that happened to carry the same value.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from typing import Any

import pytest

from conftest import over_payload
from engine import count_of, create_client, create_worker, engine_counts
from shell.journal_store import NatsJournal
from shell.names import Names
from synadia_durable import define_workflow
from synadia_durable._internal.json import encode_json
from synadia_durable._nats.protocol import WorkItem
from synadia_durable.journal.entry import TERMINAL_KINDS
from synadia_durable.types import Ctx

pytestmark = pytest.mark.asyncio

MB = 1024 * 1024
SIZED: dict[str, Any] = {"stream_max_bytes": 16 * MB}
RESULT_TIMEOUT_MS = 60_000
#: Command kinds that occupy a command position; each may appear at most once per seq.
_COMMANDS = ("step-begin", "sleep", "await", "call", "now", "random", "uuid")


async def _entries(nc: Any, prefix: str, run_id: str) -> list[dict[str, Any]]:
    """A run's journal as stored — nothing resolved, so the audit sees what is really on the stream."""
    loaded = await NatsJournal(nc, Names(prefix)).load_raw(run_id)
    return [dict(le.entry) for le in loaded]


async def _assert_journal_is_sane(nc: Any, prefix: str, run_id: str) -> None:
    """The invariants that hold whatever raced: one terminal, no command position written twice."""
    entries = await _entries(nc, prefix, run_id)
    terminals = [e["kind"] for e in entries if e["kind"] in TERMINAL_KINDS]
    assert len(terminals) == 1, f"{run_id}: expected exactly one terminal, got {terminals}"
    step_ends = Counter(e["seq"] for e in entries if e["kind"] == "step-end")
    assert not [s for s, n in step_ends.items() if n > 1], f"{run_id}: a step was journaled twice"
    commands = Counter((e["kind"], e["seq"]) for e in entries if e["kind"] in _COMMANDS)
    assert not [k for k, n in commands.items() if n > 1], f"{run_id}: a command position was reused"


class _Drives:
    """Counts how many times each run was DRIVEN, across every worker sharing this sink.

    Without it these tests could pass having never raced: if the storm's items were all consumed
    before the run parked, or one worker took every one, the assertions would hold for a plain
    sequential run and prove nothing about contention. Mirrors the check the chaos suite already
    makes ("the storm never piled up").

    durable-sdk: re-expressed through the engine (``info``), so the four "the storm never piled up"
    assertions read the engine's count (docs/findings.md F10, F11; the TS twin's ``Drives``). The
    library's workers drove every item of a storm, the ones that arrived after the run ended
    included (a terminal re-drive each). The engine hands a run's items to polls one at a time and
    acks an ended run's items itself, so most of a storm never reaches a worker and ``work.done``
    per run stays below 3 however hard the storm piled up. What piles up is the engine's
    ``de_engine_items_pulled_total`` for the workflow: "driven >= 3" per run becomes ">= 3 items
    pulled per run of the workflow". ``per_run`` (the workers' own count) stays, for the message.
    """

    def __init__(self, nc: Any, prefix: str, workflow: str, runs: int = 1) -> None:
        self.per_run: Counter[str] = Counter()
        self._nc = nc
        self._prefix = prefix
        self._workflow = workflow
        self.runs = runs
        self.pulled = 0.0

    def on_event(self, name: str, fields: Any) -> None:
        if name == "work.done":
            self.per_run[str(fields.get("run_id"))] += 1

    def piled_up(self) -> bool:
        return self.pulled >= 3 * self.runs

    def counts(self) -> str:
        return f"pulled={self.pulled} runs={self.runs} workers={dict(self.per_run)}"

    async def settled(self, run_id: str) -> None:
        """Wait for the storm to be PROCESSED. ``result()`` resolves on the status-key hint the
        moment the run completes — before the workers have finished re-driving the rest of the
        storm — so reading the counter right then races the last few items. Bounded: a storm that
        never piles up still fails, with the counts."""
        _ = run_id
        for _ in range(200):
            counts = await engine_counts(self._nc, self._prefix)
            self.pulled = count_of(counts, "de_engine_items_pulled_total", workflow=self._workflow)
            if self.piled_up():
                return
            await asyncio.sleep(0.05)


async def _until(cond: Any, timeout_s: float, what: str) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        result = cond()
        if hasattr(result, "__await__"):
            result = await result
        if result:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"timed out waiting for {what}")


async def test_versioning_x_fence_two_workers_racing_one_pinned_run(nc: Any, prefix: str) -> None:
    """The mixed-fleet test has a v1-only worker PARKING a v2 run — the two never contend.

    Here both workers register both versions and both are woken repeatedly, so version-pinned
    dispatch has to hold under an actual race rather than under an orderly handoff. A v2 run that
    executed v1's body would still produce a plausible result; the journal audit is what catches it.
    """
    executions: list[str] = []

    def _body(tag: str) -> Any:
        async def body(ctx: Ctx, _input: Any) -> str:
            await ctx.wait("go")

            def step() -> str:
                executions.append(tag)
                return tag

            return str(await ctx.run("s", step))

        return body

    v1 = define_workflow("pymx_ver", _body("v1"), version=1)
    v2 = define_workflow("pymx_ver", _body("v2"), version=2)
    drives = _Drives(nc, prefix, "pymx_ver", 2)  # durable-sdk: the engine's count, two runs
    options: dict[str, Any] = {"prefix": prefix, "lease": False, "metrics": drives, **SIZED}
    workers = [create_worker(nc, [v1, v2], **options) for _ in range(2)]
    for worker in workers:
        await worker.start()
    try:
        client = create_client(nc, prefix=prefix, **SIZED)
        h1 = await client.start(v1, {})
        h2 = await client.start(v2, {})
        await asyncio.sleep(0.4)  # let both park on the wait, so the storm lands on parked runs
        await asyncio.gather(
            *(client.signal(h.run_id, "go", {}) for h in (h1, h2) for _ in range(4))
        )

        assert await h1.result(RESULT_TIMEOUT_MS) == "v1"
        assert await h2.result(RESULT_TIMEOUT_MS) == "v2"
        assert (await h1.status()).version == 1
        assert (await h2.status()).version == 2
        # Neither run was ever executed by the other version's code, however the wakes interleaved.
        assert "v1" in executions
        assert "v2" in executions
        for handle in (h1, h2):
            await _assert_journal_is_sane(nc, prefix, handle.run_id)
            await drives.settled(handle.run_id)
            # durable-sdk: assertion re-expressed through the engine's count (see _Drives; F10).
            assert drives.piled_up(), f"the storm never piled up: {drives.counts()}"
    finally:
        for worker in workers:
            await worker.stop()


async def test_continue_as_new_x_crash_a_rollover_interrupted_before_the_next_generation(
    nc: Any, prefix: str
) -> None:
    """The crash window peculiar to a rollover.

    The ``continued`` terminal is journaled and then the process dies before generation n+1 is
    started. Nothing is left to drive the chain — there is no work item for a run that was never
    started — so the repair has to come from re-driving the TERMINAL generation, and it has to be
    idempotent. Unrepaired, the chain simply stops, with a healthy-looking ``continued`` at the end
    and no error anywhere.
    """
    seen: list[int] = []

    async def body(ctx: Ctx, input: dict[str, Any]) -> str:
        n = int(input["n"])
        seen.append(n)
        await ctx.run(f"work{n}", lambda: n)
        if n < 1:
            await ctx.continue_as_new({"n": n + 1})
        return f"done:{n}"

    wf = define_workflow("pymx_can_crash", body)
    worker = create_worker(nc, [wf], prefix=prefix, **SIZED)
    await worker.start()
    client = create_client(nc, prefix=prefix, **SIZED)
    handle = await client.start(wf, {"n": 0})
    assert await handle.result(RESULT_TIMEOUT_MS) == "done:1"
    await worker.stop()

    before = len(seen)

    # durable-sdk: the re-drive is the engine's now; the baseline of its count, before the item.
    async def redrives() -> float:
        counts = await engine_counts(nc, prefix)
        return count_of(counts, "de_engine_acks_total", outcome="terminal-redrive")

    redrives_before = await redrives()
    item: WorkItem = {"runId": handle.run_id, "kind": "resume"}
    await nc.jetstream().publish(
        Names(prefix).work_subject("pymx_can_crash"),
        encode_json(item),
        headers={"Nats-Msg-Id": f"{handle.run_id}:pymx-redrive"},
    )

    outcomes: list[str] = []

    class _Sink:
        def on_event(self, name: str, fields: Any) -> None:
            if name == "work.done" and fields.get("run_id") == handle.run_id:
                outcomes.append(str(fields["outcome"]))

    second = create_worker(nc, [wf], prefix=prefix, metrics=_Sink(), **SIZED)
    await second.start()
    try:
        # durable-sdk: re-expressed through the engine (``info``). The library's worker reported
        # the re-drive as its own ``work.done`` outcome ``terminal-redrive``; the engine re-drives a
        # terminal run itself and acks the item without a poll, so it never reaches ``second``
        # (``outcomes`` stays empty). Waiting on the engine's
        # ``de_engine_acks_total{outcome="terminal-redrive"}`` instead (F10).
        async def redriven() -> bool:
            return (await redrives()) > redrives_before or "terminal-redrive" in outcomes

        await _until(redriven, 25.0, "the re-drive")
        assert (await handle.status()).state == "continued"
        assert (await client.get(f"{handle.run_id}_g1").status()).state == "completed"
        assert len(seen) == before, "the repair re-executed a generation"
        await _assert_journal_is_sane(nc, prefix, handle.run_id)
        await _assert_journal_is_sane(nc, prefix, f"{handle.run_id}_g1")
    finally:
        await second.stop()


async def test_continue_as_new_x_multi_worker_a_raced_rollover_starts_one_generation(
    nc: Any, prefix: str
) -> None:
    """The integration twin of the adopt unit test.

    Two workers drive one run through a rollover under a wake storm; whichever loses the terminal
    CAS must adopt the JOURNALED spec, so exactly one ``continued`` exists and exactly one
    generation 1 is started. Before the adopt fix the loser handed its own spec to the worker, and
    the next generation's input was decided by whoever won a second, unordered race.
    """

    async def body(ctx: Ctx, input: dict[str, Any]) -> str:
        n = int(input["n"])
        await ctx.wait("go")
        if n < 1:
            await ctx.continue_as_new({"n": n + 1})
        return f"end:{n}"

    wf = define_workflow("pymx_can_race", body)
    drives = _Drives(nc, prefix, "pymx_can_race")  # durable-sdk: the engine's count
    options: dict[str, Any] = {"prefix": prefix, "lease": False, "metrics": drives, **SIZED}
    workers = [create_worker(nc, [wf], **options) for _ in range(2)]
    for worker in workers:
        await worker.start()
    try:
        client = create_client(nc, prefix=prefix, **SIZED)
        handle = await client.start(wf, {"n": 0})
        gen1 = f"{handle.run_id}_g1"
        await asyncio.sleep(0.4)
        await asyncio.gather(*(client.signal(handle.run_id, "go", {}) for _ in range(6)))

        await _until(lambda: _state_is_not(client, gen1, "pending"), 40.0, "generation 1 to exist")
        # Generation 1 parks on its own wait, so nudge it and let the chain finish.
        await asyncio.gather(*(client.signal(gen1, "go", {}) for _ in range(3)))
        assert await client.get(gen1).result(RESULT_TIMEOUT_MS) == "end:1"

        gen0 = await _entries(nc, prefix, handle.run_id)
        assert len([e for e in gen0 if e["kind"] == "continued"]) == 1
        assert (await client.get(f"{handle.run_id}_g2").status()).state == "pending", "a third gen"
        await _assert_journal_is_sane(nc, prefix, handle.run_id)
        await _assert_journal_is_sane(nc, prefix, gen1)
        await drives.settled(handle.run_id)
        # durable-sdk: assertion re-expressed through the engine's count (see _Drives; F10).
        assert drives.piled_up(), f"the storm never piled up: {drives.counts()}"
    finally:
        for worker in workers:
            await worker.stop()


async def _state_is_not(client: Any, run_id: str, state: str) -> bool:
    return str((await client.get(run_id).status()).state) != state


# durable-sdk: docs/findings.md F1/F2, answered by the engine's F25 (put and get) and F22
# (serve --offload-threshold), with the SDK's chunked transport (_engine/blobs.py).
async def test_offload_x_multi_worker_a_storm_over_blob_backed_payloads(
    nc: Any, prefix: str
) -> None:
    """Offload is proven against crash, fence and redelivery, but never under contention.

    What is specific here: two workers re-running the same step both WRITE a blob, and the names are
    content-addressed — so identical values converge on one object and the loser's write is
    harmless. A design that deleted a fenced writer's blob would corrupt the winner's entry.
    """

    big_len = over_payload(nc, 2)  # twice this server's max_payload: it cannot ride inline

    async def body(ctx: Ctx, _input: Any) -> int:
        await ctx.wait("go")
        big = await ctx.run("render", lambda: "z" * big_len)
        return len(str(big))

    wf = define_workflow("pymx_blob", body)
    drives = _Drives(nc, prefix, "pymx_blob")  # durable-sdk: the engine's count
    options: dict[str, Any] = {
        "prefix": prefix,
        "lease": False,
        "metrics": drives,
        "stream_max_bytes": 64 * MB,
    }
    workers = [create_worker(nc, [wf], **options) for _ in range(2)]
    for worker in workers:
        await worker.start()
    try:
        client = create_client(nc, prefix=prefix, stream_max_bytes=64 * MB)
        handle = await client.start(wf, {})
        await asyncio.sleep(0.4)
        await asyncio.gather(*(client.signal(handle.run_id, "go", {}) for _ in range(5)))

        assert await handle.result(RESULT_TIMEOUT_MS) == big_len
        await _assert_journal_is_sane(nc, prefix, handle.run_id)
        # The value really did ride as a ref — otherwise this proved nothing about offload.
        entries = await _entries(nc, prefix, handle.run_id)
        step_end = next(e for e in entries if e["kind"] == "step-end")
        assert str(step_end["valueRef"]["name"]).startswith(f"{handle.run_id}/sha256:")
        await drives.settled(handle.run_id)
        # durable-sdk: assertion re-expressed through the engine's count (see _Drives; F10).
        assert drives.piled_up(), f"the storm never piled up: {drives.counts()}"
    finally:
        for worker in workers:
            await worker.stop()


async def test_cancellation_x_multi_worker_a_cancel_racing_two_drivers(
    nc: Any, prefix: str
) -> None:
    """A cancel is deliberately UNGATED by the lease so the kill switch always works.

    Which means it races execution BY DESIGN. Under two workers the run can be mid-step on one while
    the other records the cancel, and the only acceptable outcomes are a single ``cancelled``
    terminal or a single ``result`` — a cancel arriving after completion is a no-op. What must never
    happen is two terminals, or a run that ends with neither.
    """

    async def body(ctx: Ctx, _input: Any) -> str:
        await ctx.wait("go")

        def slow() -> str:
            import time

            time.sleep(0.4)
            return "a"

        await ctx.run("slow", slow)
        await ctx.sleep("gap", 300)
        await ctx.run("after", lambda: "b")
        return "finished"

    wf = define_workflow("pymx_cancel", body)
    drives = _Drives(nc, prefix, "pymx_cancel")  # durable-sdk: the engine's count
    options: dict[str, Any] = {"prefix": prefix, "lease": False, "metrics": drives, **SIZED}
    workers = [create_worker(nc, [wf], **options) for _ in range(2)]
    for worker in workers:
        await worker.start()
    try:
        client = create_client(nc, prefix=prefix, **SIZED)
        handle = await client.start(wf, {})
        await asyncio.sleep(0.4)
        # Wake it from both sides and cancel in the middle of the storm.
        await asyncio.gather(
            *(client.signal(handle.run_id, "go", {}) for _ in range(3)),
            client.cancel(handle.run_id, reason="raced"),
            client.cancel(handle.run_id, reason="raced again"),
        )

        await _until(
            lambda: _state_in(handle, ("cancelled", "completed")), 40.0, "a terminal state"
        )
        await _assert_journal_is_sane(nc, prefix, handle.run_id)  # one terminal, whichever it is
        entries = await _entries(nc, prefix, handle.run_id)
        kinds = [e["kind"] for e in entries]
        assert kinds.count("cancelled") <= 1
        assert kinds.count("cancel-requested") == 1, "a double cancel must collapse"
        await drives.settled(handle.run_id)
        # durable-sdk: assertion re-expressed through the engine's count (see _Drives; F10).
        assert drives.piled_up(), f"the storm never piled up: {drives.counts()}"
    finally:
        for worker in workers:
            await worker.stop()


async def _state_in(handle: Any, states: tuple[str, ...]) -> bool:
    return str((await handle.status()).state) in states
