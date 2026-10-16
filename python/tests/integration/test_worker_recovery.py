"""The worker's recovery paths, against a real nats-server. The twin of
``test/integration/worker-recovery.test.ts``.

``worker.py`` is the least-covered module in this tree, and what is dark in it is not a random
slice — it is almost entirely the code that runs when something has ALREADY gone wrong: a poison
item, a work item for a run that no longer exists, an event append that keeps losing its CAS, a
crash between the journal terminal and the KV finalize, a broken timers stream, a lease taken away
mid-drive. Every one of those is invisible while the system is healthy, which is when a bug in it
costs the least to have and the most to hit.

The assertions are on the ACK DECISION (PROTOCOL §6.4) rather than on a workflow's return value:
ack vs NAK-with-backoff vs term decides whether a run is retried forever, dropped, or quietly
poisons a consumer, and several different ack choices produce the same ``result()``.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from typing import Any

import pytest

import existing
from engine import count_of, create_client, create_worker, engine_counts, engine_log, restart_engine
from shell.names import Names
from synadia_durable import WorkflowFailure, define_workflow
from synadia_durable._internal.json import encode_json
from synadia_durable._nats.protocol import WorkItem
from synadia_durable.types import Ctx

pytestmark = pytest.mark.asyncio

MB = 1024 * 1024
SIZED: dict[str, Any] = {"stream_max_bytes": 16 * MB}


class _Sink:
    """A metrics sink, so the ack decision can be asserted instead of inferred."""

    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def on_event(self, name: str, fields: Mapping[str, Any]) -> None:
        self.events.append((name, dict(fields)))

    def outcomes(self, run_id: str | None = None) -> list[str]:
        return [
            str(f["outcome"])
            for n, f in self.events
            if n == "work.done" and (run_id is None or f.get("run_id") == run_id)
        ]


class _Logs:
    def __init__(self) -> None:
        self.lines: list[str] = []

    def debug(self, message: str, **_fields: Any) -> None:
        pass

    def info(self, message: str, **_fields: Any) -> None:
        pass

    def warn(self, message: str, **_fields: Any) -> None:
        self.lines.append(message)

    def error(self, message: str, **_fields: Any) -> None:
        self.lines.append(message)

    def saw(self, fragment: str) -> bool:
        return any(fragment in line for line in self.lines)


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


async def _engine_outcomes(nc: Any, prefix: str, labels: list[str]) -> list[str]:
    """durable-sdk: the engine's ack decisions on ``prefix`` by outcome label, as a list (each label
    repeated by its count), for the assertions that read the library worker's ``work.done``
    outcomes. A terminal re-drive and a missing journal are decided in the engine now and never
    reach a worker; the engine counts them in ``de_engine_acks_total{outcome, ack}`` (``info``).
    Order is not observable through counts (docs/findings.md F10)."""
    counts = await engine_counts(nc, prefix)
    return [
        label
        for label in labels
        for _ in range(int(count_of(counts, "de_engine_acks_total", outcome=label)))
    ]


async def _engine_saw(prefix: str, fragment: str) -> bool:
    return any(fragment in line for line in await engine_log(prefix))


async def _state(handle: Any) -> str:
    return str((await handle.status()).state)


async def _is_state(handle: Any, state: str) -> bool:
    return await _state(handle) == state


async def test_an_undecodable_work_item_is_termed_not_looped_on(nc: Any, prefix: str) -> None:
    """A poison message must not head-of-line-block a workflow's whole consumer.

    ``DE_WORK`` has ``max_deliver: -1``, so a NAK'd item comes back forever, and bytes that are not
    JSON can never become decodable. ``term`` is the only correct answer — and on a workqueue stream
    it also removes the message, which is what this asserts.
    """

    async def body(ctx: Ctx, _input: Any) -> str:
        return str(await ctx.run("s", lambda: "ok"))

    wf = define_workflow("pyrec_poison", body)
    worker = create_worker(nc, [wf], prefix=prefix, **SIZED)
    await worker.start()
    try:
        n = Names(prefix)
        await nc.jetstream().publish(n.work_subject("pyrec_poison"), b"{ not json")

        async def emptied() -> bool:
            info = await nc.jsm().stream_info(n.work_stream)
            return bool(info.state.messages == 0)

        await _until(emptied, 15.0, "the poison item to be dropped")

        # …and the pump is still serving: the very next run goes through.
        client = create_client(nc, prefix=prefix, **SIZED)
        handle = await client.start(wf, {})
        assert await handle.result(20_000) == "ok"
    finally:
        await worker.stop()


async def test_a_work_item_for_a_run_with_no_journal_retries_then_drops(
    nc: Any, prefix: str
) -> None:
    """A forged item, or one whose journal was GC'd.

    Dropping on the first delivery would strand a run whose journal was merely slow to read — a
    fired timer wake is one-shot and would be gone — so the retries coming FIRST is the property,
    not just the eventual drop.
    """

    async def body(ctx: Ctx, _input: Any) -> int:
        return int(await ctx.run("s", lambda: 1))

    wf = define_workflow("pyrec_ghost", body)
    sink, logs = _Sink(), _Logs()
    worker = create_worker(
        nc,
        [wf],
        prefix=prefix,
        metrics=sink,
        logger=logs,
        no_journal_max_age_ms=3_000,
        **SIZED,
    )
    await worker.start()
    try:
        item: WorkItem = {"runId": "pyrec-ghost-nobody", "kind": "resume"}
        await nc.jetstream().publish(
            Names(prefix).work_subject("pyrec_ghost"),
            encode_json(item),
            headers={"Nats-Msg-Id": "pyghost-1"},
        )
        # durable-sdk: re-expressed through the engine. The no-journal policy is the engine's:
        # nak'd with its backoff (250 ms doubling) while the item is younger than
        # --no-journal-max-age, 3 s here, then termed (by age since its F75, F77, not on the fifth
        # delivery). The item never reaches the worker: the outcomes are the engine's ack counts
        # (built from counters in label order, so the order of the outcomes is not observable) and
        # the log line is the engine's (F10; the TS twin).
        labels = ["retry-no-journal", "dropped-no-journal"]

        async def dropped() -> bool:
            return "dropped-no-journal" in await _engine_outcomes(nc, prefix, labels)

        await _until(dropped, 30.0, "the ghost item to be dropped")
        outcomes = await _engine_outcomes(nc, prefix, labels)
        # At least one retry and the drop: dropping on the first delivery is the bug this guards.
        # The order is not observable through counts. The backoff gives about four retries within
        # the 3 s (250 + 500 + 1000 ms, then 2 s): expect 4, assert at least one.
        assert outcomes.count("retry-no-journal") >= 1, outcomes
        assert outcomes[-1] == "dropped-no-journal"
        assert await _engine_saw(prefix, "no journal for this run after retries")
        # durable-sdk: assertion added, as the TS twin's — the item never reached the worker.
        assert sink.outcomes("pyrec-ghost-nobody") == []
        _ = logs
    finally:
        await worker.stop()


# durable-sdk: "an event append that keeps fencing NAKs rather than losing the event" is dropped.
# It injected the fences by patching the worker's own NatsJournal.append; recording the signal is
# the engine's now (journaled before the poll hands the item out, nak'd when every attempt is
# fenced: durable-engine core.RecordInbound, engine.TestRecordInbound*), and no SDK request carries
# the signal entry. The TS twin dropped the same test.


async def test_a_terminal_redrive_repairs_a_stale_status_hint(nc: Any, prefix: str) -> None:
    """The crash window: the journal terminal landed, the KV finalize did not.

    Left alone, ``status()`` reports running forever while ``result()`` (journal truth) already
    resolves. The re-drive branch repairs it — including the FAILED arm, the one easy to leave out
    because the happy path is what anybody tests by hand.
    """

    async def body(ctx: Ctx, _input: Any) -> str:
        await ctx.run("s", lambda: 1)
        raise RuntimeError("declined")

    wf = define_workflow("pyrec_redrive", body)
    sink = _Sink()
    worker = create_worker(nc, [wf], prefix=prefix, metrics=sink, **SIZED)
    await worker.start()
    try:
        client = create_client(nc, prefix=prefix, **SIZED)
        handle = await client.start(wf, {})
        with pytest.raises(WorkflowFailure):
            await handle.result(20_000)
        assert await _state(handle) == "failed"

        # Stub the hint back to `running` behind the engine's back.
        kv = await nc.jetstream().key_value(Names(prefix).runs_kv)
        await kv.put(
            handle.run_id,
            json.dumps(
                {
                    "status": "running",
                    "wf": "pyrec_redrive",
                    "version": 1,
                    "updatedMs": 1_700_000_000_000,
                }
            ).encode(),
        )
        assert await _state(handle) == "running"  # the stale state a user would see

        # durable-sdk: the re-drive is the engine's now; the baseline of its count, before the item.
        redrives = len(await _engine_outcomes(nc, prefix, ["terminal-redrive"]))
        item: WorkItem = {"runId": handle.run_id, "kind": "resume"}
        await nc.jetstream().publish(
            Names(prefix).work_subject("pyrec_redrive"),
            encode_json(item),
            headers={"Nats-Msg-Id": f"{handle.run_id}:redrive"},
        )

        # durable-sdk: re-expressed through the engine — the re-drive is the engine's, acked
        # without a poll; its count, not the worker's ``work.done``, says it happened (F10).
        async def redriven() -> bool:
            if "terminal-redrive" in sink.outcomes(handle.run_id):
                return True
            return len(await _engine_outcomes(nc, prefix, ["terminal-redrive"])) > redrives

        await _until(redriven, 20.0, "the re-drive")
        await _until(lambda: _is_state(handle, "failed"), 15.0, "the hint to be repaired")
    finally:
        await worker.stop()


# durable-sdk: docs/findings.md F7, answered by the engine's F19 (the work durable is bound on its
# own, so polls keep being served with DE_TIMERS gone).
@existing.skip_in_mode(existing.DELETES_STREAMS)
async def test_the_maintenance_pass_and_pumps_survive_a_deleted_timers_stream(
    nc: Any, prefix: str
) -> None:
    """The LEARNINGS #40 scenario: a server upgraded past the boundary does not bring DE_TIMERS back.

    The worker must keep serving — loudly — rather than dying inside a background task where nobody
    would see it. A broken timers stream degrades timers; it does not stop the worker.
    """

    async def body(ctx: Ctx, _input: Any) -> str:
        return str(await ctx.run("s", lambda: "ok"))

    wf = define_workflow("pyrec_timers", body)
    logs = _Logs()
    worker = create_worker(
        nc, [wf], prefix=prefix, maintenance_interval_ms=1_000, logger=logs, **SIZED
    )
    await worker.start()
    try:
        await nc.jsm().delete_stream(Names(prefix).timers_stream)

        # durable-sdk: re-expressed through the engine — the maintenance pass is the engine's, and
        # so is its log (``maintenance_interval_ms`` is its ``--maintenance-interval``).
        async def reported() -> bool:
            return (
                logs.saw("timer-wake purge failed")
                or await _engine_saw(prefix, "timer-wake purge failed")
                or await _engine_saw(prefix, "timers headroom check failed")
            )

        await _until(reported, 25.0, "the maintenance pass to report the broken stream")

        client = create_client(nc, prefix=prefix, **SIZED)
        handle = await client.start(wf, {})
        assert await handle.result(20_000) == "ok"

        health = worker.health()
        assert health.running
        # durable-sdk: assertion removed — ``any(p.stream == timers_stream for p in health.pumps)``.
        # The worker has no consumer pumps (one poll per slot); the timers durable is the engine's.
    finally:
        await worker.stop()

    # durable-sdk: provisioning is the engine's, at its start: the restart is the engine's.
    await restart_engine(prefix)

    # …and a fresh worker re-provisions the stream, so a restart is a real remedy.
    async def slept_body(ctx: Ctx, _input: Any) -> str:
        await ctx.sleep("nap", 200)
        return "woke"

    slept = define_workflow("pyrec_timers_sleep", slept_body)
    healed = create_worker(nc, [slept], prefix=prefix, **SIZED)
    await healed.start()
    try:
        info = await nc.jsm().stream_info(Names(prefix).timers_stream)
        assert info.config.allow_msg_schedules is True
        client = create_client(nc, prefix=prefix, **SIZED)
        handle = await client.start(slept, {})
        assert await handle.result(20_000) == "woke"  # timers work again
    finally:
        await healed.stop()


class _HoldingPolls:
    """A worker's connection that holds a poll's item until the worker's ``cancelPoll`` has been
    answered, and a little longer, as the wire would; and records every ``done``'s outcome."""

    def __init__(self, nc: Any) -> None:
        self._nc = nc
        self.handed = asyncio.Event()
        self.withdrawn = asyncio.Event()
        self.dones: list[str] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._nc, name)

    async def request(self, subject: str, payload: bytes = b"", **kwargs: Any) -> Any:
        body = json.loads(payload or b"{}")
        reply = await self._nc.request(subject, payload, **kwargs)
        if subject.endswith(".api.v1.poll") and body.get("cancelPoll") is True:
            self.withdrawn.set()
        elif subject.endswith(".api.v1.poll") and "runId" in json.loads(reply.data):
            self.handed.set()
            await self.withdrawn.wait()
            await asyncio.sleep(0.2)
        elif subject.endswith(".api.v1.done"):
            self.dones.append(str(body.get("outcome", {}).get("kind")))
        return reply


async def test_stop_hands_back_an_item_its_poll_was_handed_as_it_stopped(
    nc: Any, nats_url: str, prefix: str
) -> None:
    """The engine's F51 (the SDK's part): a poll answered with an item in the instant before the
    withdrawal reaches the worker after the ``cancelPoll`` was answered. A ``stop()`` that returned
    then, with the process exiting right after, left the item to a lease nobody held, until its ack
    wait (30 s). ``stop()`` now returns only once every poll has returned and that item has gone
    back (``done retry``): another worker has it at once. The worker's connection is closed the
    moment ``stop()`` returns, as an exiting process would."""

    async def body(ctx: Ctx, _input: Any) -> str:
        return str(await ctx.run("s", lambda: "ran"))

    wf = define_workflow("pyrec_stop_handback", body)
    holder = await existing.connect(nats_url)
    held = _HoldingPolls(holder)
    stopping = create_worker(held, [wf], prefix=prefix, concurrency=1, **SIZED)
    other = None
    try:
        await stopping.start()
        client = create_client(nc, prefix=prefix, **SIZED)
        handle = await client.start(wf, {})
        await asyncio.wait_for(held.handed.wait(), existing.scaled(10))
        await stopping.stop()
        assert held.dones == ["retry"], "stop() returned before the item it was handed went back"
        await holder.close()  # the process exits

        other = create_worker(nc, [wf], prefix=prefix, **SIZED)
        await other.start()
        assert await handle.result(15_000) == "ran"  # the ack wait is 30 s
    finally:
        await stopping.stop()
        if other is not None:
            await other.stop()
        if not holder.is_closed:
            await holder.close()
