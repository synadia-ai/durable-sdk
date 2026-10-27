"""Multi-worker chaos: a fleet under a signal storm, and a fleet being killed mid-run.

The last gap in the operational tranche, and the one the TypeScript suite says found real bugs. Every
other test in this suite arranges one situation and checks it. These arrange a mess and check the
INVARIANTS — the properties that have to hold no matter which worker won which race:

* **exactly-once journaling** — one terminal per run, one ``step-end`` per command seq, whatever
  happened above the journal;
* **at-least-once execution** — every step ran, possibly more than once (PROTOCOL §14: that is the
  contract, not a defect);
* **no stranded runs** — every run reaches a terminal, and the status hint agrees with the journal.

The audit is deliberately mechanical: it reads the journal back and counts. A chaos test that only
asserts "the result was right" would miss a duplicated step-end that happened to carry the same
value.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Mapping
from typing import Any

import pytest

from engine import count_of, create_client, create_worker, engine_counts
from shell.journal_store import NatsJournal
from shell.names import Names
from synadia_durable import define_workflow
from synadia_durable.journal.entry import TERMINAL_KINDS
from synadia_durable.types import Ctx

pytestmark = pytest.mark.asyncio

MB = 1024 * 1024
RESULT_TIMEOUT_MS = 60_000


class _Recorder:
    """A metrics sink shared by every worker in a test, so the chaos can be counted afterwards."""

    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def on_event(self, name: str, fields: Mapping[str, Any]) -> None:
        self.events.append((name, dict(fields)))

    def named(self, name: str) -> list[dict[str, Any]]:
        return [fields for event, fields in self.events if event == name]


async def _audit(nc: Any, prefix: str, run_id: str) -> dict[str, Any]:
    """Read one run's journal back and count what must be countable.

    ``load_raw``: this is an audit of what is ON the stream, so nothing is resolved or inflated.
    """
    loaded = await NatsJournal(nc, Names(prefix)).load_raw(run_id)
    kinds = [str(le.entry["kind"]) for le in loaded]
    step_ends = Counter(int(le.entry["seq"]) for le in loaded if le.entry["kind"] == "step-end")
    commands = Counter(
        (str(le.entry["kind"]), int(le.entry["seq"]))
        for le in loaded
        if le.entry["kind"] in ("step-begin", "sleep", "await", "call", "now", "random", "uuid")
    )
    return {
        "terminals": [k for k in kinds if k in TERMINAL_KINDS],
        "duplicate_step_ends": {seq: n for seq, n in step_ends.items() if n > 1},
        "duplicate_commands": {key: n for key, n in commands.items() if n > 1},
        "entries": len(loaded),
    }


def _assert_journal_is_sane(audit: dict[str, Any], run_id: str) -> None:
    assert audit["terminals"] == audit["terminals"][:1], f"{run_id}: more than one terminal"
    assert len(audit["terminals"]) == 1, f"{run_id}: no terminal — the run was stranded"
    assert not audit["duplicate_step_ends"], f"{run_id}: a step was journaled twice"
    assert not audit["duplicate_commands"], f"{run_id}: a command position was written twice"


async def test_a_signal_storm_across_four_workers_journals_every_step_exactly_once(
    nc: Any, prefix: str
) -> None:
    """The lease-LESS contract, which is the one the engine has to hold on its own.

    Four workers share the durables and every run gets a burst of signals, so the same run is driven
    concurrently by several of them: exactly the overlap the journal CAS exists for. The gating lease
    is OFF on purpose — its job is to make this rare, not to make it safe, and the twin with it on
    lives in ``test_lease.py``.
    """
    runs = 6
    executions: list[str] = []

    async def body(ctx: Ctx, _input: Any) -> str:
        await ctx.wait("go")

        def step(name: str) -> str:
            executions.append(name)
            return name

        a = await ctx.run("s1", step, "s1")
        b = await ctx.run("s2", step, "s2")
        c = await ctx.run("s3", step, "s3")
        return f"{a}-{b}-{c}"

    wf = define_workflow("pychaos1", body)
    # One sink shared by all four workers: it is how this test proves the storm actually piled up,
    # rather than passing because the extra signals arrived after the run had already finished.
    sink = _Recorder()
    workers = [
        create_worker(nc, [wf], prefix=prefix, stream_max_bytes=64 * MB, lease=False, metrics=sink)
        for _ in range(4)
    ]
    for worker in workers:  # sequentially: a consumer-provisioning race is not what is under test
        await worker.start()
    try:
        client = create_client(nc, prefix=prefix, stream_max_bytes=64 * MB)
        handles = [await client.start(wf, {}) for _ in range(runs)]
        await asyncio.sleep(0.5)  # let them park on the wait, so the storm lands on parked runs
        await asyncio.gather(
            *(client.signal(h.run_id, "go", {}) for h in handles for _ in range(5))
        )

        results = await asyncio.gather(*(h.result(RESULT_TIMEOUT_MS) for h in handles))
        assert results == ["s1-s2-s3"] * runs, "a result is built from JOURNALED values only"

        for handle in handles:
            _assert_journal_is_sane(await _audit(nc, prefix, handle.run_id), handle.run_id)
        # At-least-once EXECUTION is the contract; every step ran, and some may have run twice.
        assert set(executions) == {"s1", "s2", "s3"}
        assert len(executions) >= 3 * runs
        # And the storm was real: each run was driven several times (start + the surviving signals),
        # across more than one worker.
        per_run = Counter(str(f["run_id"]) for f in sink.named("work.done"))
        # durable-sdk: assertion re-expressed through the engine (`info`), as the TypeScript matrix
        # suite's `drives.piledUp` (F10, F11). The engine hands a run's items to
        # polls one at a time and acks an ended run's items itself, so most of a storm never
        # reaches a worker and `work.done` per run stays below 3 however hard it piled up. What
        # piles up is the engine's `de_engine_items_pulled_total` for the workflow: "driven >= 3
        # per run" becomes ">= 3 items pulled per run of the workflow"; `per_run` is for the message.
        pulled = 0.0
        for _ in range(200):  # the storm's tail is still being absorbed after the last result
            pulled = count_of(
                await engine_counts(nc, prefix), "de_engine_items_pulled_total", workflow="pychaos1"
            )
            if pulled >= 3 * runs:
                break
            await asyncio.sleep(0.05)
        assert pulled >= 3 * runs, f"the storm never piled up: pulled {pulled}, workers {per_run}"
    finally:
        for worker in workers:
            await worker.stop()


async def test_workers_killed_mid_run_leave_no_run_stranded(nc: Any, prefix: str) -> None:
    """Crash chaos: kill the fleet while it is holding work, and start a fresh one over the wreckage.

    ``stop(drain_timeout_s=0)`` is the closest thing to SIGKILL this process can do to itself — the
    in-flight items are abandoned mid-execution, neither acked nor NAK'd, exactly as they would be
    if the machine went away. Their runs are then invisible until AckWait expires, which is why the
    ack wait and the lease TTL are short here: a crashed holder must not keep a run for 30 s.

    What is asserted is not "it was fast" but "nothing was lost": every run reaches a terminal, its
    journal has exactly one, and no step was journaled twice.
    """
    runs = 4
    started: list[str] = []

    async def body(ctx: Ctx, input: dict[str, Any]) -> str:
        started.append(str(input["id"]))

        def slow(tag: str) -> str:
            import time

            time.sleep(0.4)  # long enough to be holding the item when the worker is killed
            return tag

        first = await ctx.run("one", slow, "one")
        second = await ctx.run("two", slow, "two")
        return f"{input['id']}:{first}+{second}"

    wf = define_workflow("pychaos2", body)
    options: dict[str, Any] = {
        "prefix": prefix,
        "stream_max_bytes": 64 * MB,
        "ack_wait_ms": 2_000,
        "lease_ttl_ms": 2_000,
    }
    doomed = [create_worker(nc, [wf], **options) for _ in range(2)]
    for worker in doomed:
        await worker.start()

    survivors: list[Any] = []
    try:
        client = create_client(nc, prefix=prefix, stream_max_bytes=64 * MB)
        handles = [await client.start(wf, {"id": f"r{i}"}) for i in range(runs)]
        await asyncio.sleep(0.6)  # everyone is mid-step

        # The crash. No drain, no ack, no NAK — the items simply stop being processed.
        await asyncio.gather(*(w.stop(drain_timeout_s=0) for w in doomed))

        # A fresh fleet starts over the wreckage, as a rolling deploy would.
        survivors = [create_worker(nc, [wf], **options) for _ in range(2)]
        for worker in survivors:
            await worker.start()

        results = await asyncio.gather(*(h.result(RESULT_TIMEOUT_MS) for h in handles))
        assert sorted(results) == sorted(f"r{i}:one+two" for i in range(runs))

        for handle in handles:
            _assert_journal_is_sane(await _audit(nc, prefix, handle.run_id), handle.run_id)
            status = await handle.status()
            assert status.state == "completed", f"{handle.run_id}: hint disagrees with the journal"
        # The crash is real, not simulated: those runs were driven more than once.
        assert len(started) > runs, "no run was ever redelivered — the kill did nothing"
    finally:
        for worker in [*doomed, *survivors]:
            await worker.stop()


async def test_a_fan_out_survives_the_same_treatment(nc: Any, prefix: str) -> None:
    """The same chaos over the part with the most moving pieces: children, timers and signals at once.

    A parent that fans out to children, sleeps, and waits for a signal has four independent wake
    paths racing three workers. If any of them lost an entry — a duplicated child start, a resolve
    that fenced and was dropped, a timer wake that arrived twice — this is where it would show.
    """
    child_runs: list[str] = []

    def do_child_work(n: int) -> str:
        child_runs.append(str(n))
        return "c"

    async def child_body(ctx: Ctx, input: dict[str, Any]) -> str:
        return str(await ctx.run("work", do_child_work, int(input["n"])))

    async def parent_body(ctx: Ctx, _input: Any) -> str:
        kids = await asyncio.gather(*(ctx.start_call(child, {"n": n}) for n in range(3)))
        await ctx.sleep("settle", 300)
        approval = await ctx.wait("approve")
        return f"{''.join(str(k) for k in kids)}:{approval}"

    child = define_workflow("pychaos3c", child_body)
    parent = define_workflow("pychaos3p", parent_body)
    options: dict[str, Any] = {
        "prefix": prefix,
        "stream_max_bytes": 64 * MB,
        "ack_wait_ms": 2_000,
        "lease_ttl_ms": 2_000,
    }
    workers = [create_worker(nc, [parent, child], **options) for _ in range(3)]
    for worker in workers:
        await worker.start()
    extra: list[Any] = []
    try:
        client = create_client(nc, prefix=prefix, stream_max_bytes=64 * MB)
        handle = await client.start(parent, {})
        await asyncio.sleep(0.5)

        await workers[0].stop(drain_timeout_s=0)  # one worker dies mid-fan-out
        extra = [create_worker(nc, [parent, child], **options)]
        await extra[0].start()
        await asyncio.sleep(0.5)
        await asyncio.gather(*(client.signal(handle.run_id, "approve", "yes") for _ in range(4)))

        assert await handle.result(RESULT_TIMEOUT_MS) == "ccc:yes"
        _assert_journal_is_sane(await _audit(nc, prefix, handle.run_id), handle.run_id)
        for seq in range(3):  # each child is its own run, and each must be sane too
            child_id = f"{handle.run_id}_{seq}"
            _assert_journal_is_sane(await _audit(nc, prefix, child_id), child_id)
    finally:
        for worker in [*workers, *extra]:
            await worker.stop()


# durable-sdk: "a fenced inbound event is retried, not lost" is dropped. It injected the fence by
# patching the worker's own journal (`worker._journal.append`); recording an inbound event is the
# engine's now (it journals the event before the poll hands the item out, retried past a fence:
# the engine's own tests prove it), and no SDK request carries it. The TypeScript
# twin dropped the same test (test/integration/e2e.test.ts).


async def test_a_signal_storm_with_encryption_on_journals_every_step_exactly_once(
    nc: Any, prefix: str
) -> None:
    """The same storm, with a payload codec configured — the combination neither SDK had run.

    The interaction worth proving is specific. The transform is deliberately NON-deterministic (a
    fresh nonce per call, so equal payloads never produce equal bytes), while exactly-once
    journaling rests on ``seq`` + CAS + msgID and never on comparing bytes. Two workers racing the
    same step therefore write DIFFERENT ciphertext for the same value, and the loser has to adopt
    what was recorded rather than see a mismatch. A design that compared bytes would report a false
    non-determinism under any overlap at all.

    It also checks the thing a chaos test is uniquely placed to check: that no entry slipped through
    in the clear because the worker writing it happened to be racing.
    """
    from synadia_durable.journal.payload_codec import aes_gcm_payload_codec

    key = bytes([42]) * 32
    secret = "pan-9999888877776666"
    runs = 3
    executions: list[str] = []

    async def body(ctx: Ctx, input: dict[str, Any]) -> str:
        await ctx.wait("go")

        def step(name: str) -> str:
            executions.append(name)
            return f"{name}:{input['card']}"

        first = await ctx.run("s1", step, "s1")
        second = await ctx.run("s2", step, "s2")
        return f"{first}/{second}"

    wf = define_workflow("pychaos_enc", body)
    options: dict[str, Any] = {
        "prefix": prefix,
        "stream_max_bytes": 64 * MB,
        "lease": False,  # pins the contract the engine holds on its own
        "payload_codec": aes_gcm_payload_codec(key),
    }
    workers = [create_worker(nc, [wf], **options) for _ in range(2)]
    for worker in workers:
        await worker.start()
    try:
        client = create_client(
            nc,
            prefix=prefix,
            stream_max_bytes=64 * MB,
            payload_codec=aes_gcm_payload_codec(key),
        )
        handles = [await client.start(wf, {"card": secret}) for _ in range(runs)]
        await asyncio.sleep(0.5)  # let them park, so the storm lands on parked runs
        await asyncio.gather(
            *(client.signal(h.run_id, "go", {}) for h in handles for _ in range(5))
        )

        results = await asyncio.gather(*(h.result(RESULT_TIMEOUT_MS) for h in handles))
        assert results == [f"s1:{secret}/s2:{secret}"] * runs
        assert set(executions) == {"s1", "s2"}  # at-least-once EXECUTION is the contract

        for handle in handles:
            audit = await _audit(nc, prefix, handle.run_id)
            _assert_journal_is_sane(audit, handle.run_id)

            # Nothing leaked, and every payload-bearing entry really was transformed — a raw audit,
            # so a v:1 entry sitting among v:3 ones would be visible rather than resolved away.
            raw = await NatsJournal(nc, Names(prefix)).load_raw(handle.run_id)
            assert secret not in str([dict(le.entry) for le in raw])
            for le in raw:
                if le.entry["kind"] in ("start", "step-end", "result"):
                    assert le.entry["v"] == 3, dict(le.entry)
    finally:
        for worker in workers:
            await worker.stop()


async def test_the_transform_is_non_deterministic_which_is_why_bytes_are_never_compared() -> None:
    """The property the storm above depends on, isolated and stated once."""
    from synadia_durable.journal.payload_codec import aes_gcm_payload_codec

    codec = aes_gcm_payload_codec(bytes([42]) * 32)
    same = b'{"card":"pan-9999888877776666"}'
    first, second = await codec.encode(same), await codec.encode(same)
    assert first != second
    assert await codec.decode(first) == await codec.decode(second) == same
