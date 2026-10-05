"""synadia-durable — the worker: "The worker's loop" of ``durable-engine`` ``docs/API.md``, in
Python. Mirrors ``src/engine/worker.ts``.

One ``poll`` per concurrency slot names every registered workflow with its versions; for each item
the journal is paged in with ``load`` while ``more``, reversed through the payload codec, and
replayed by the library's ``execute_run`` with its three seams as requests under the item's lease
(Journal → load/append, Scheduler → wake, Children → child), with ``extend`` on a third of the ack
wait meanwhile; the ``ExecOutcome`` goes back as ``done``. On ``expired`` from any request the item
is dropped and the slot polls on. A value too large for a request or a reply goes up and comes
down in parts (``blobs.py``). Every poll names this worker, and ``stop()`` withdraws its waiting
polls with one ``poll {cancelPoll}``: the engine remembers the withdrawn worker 5 s, so a poll of
it that arrives later answers ``none`` at once (its F53, ``withdrawn-remembered``, the floor
``start()`` checks in ``info.capabilities``). A stop takes as long as the items in flight, not a
poll's wait; it returns once every slot's loop has ended, so an item handed to a poll in the
instant before the withdrawal has gone back (``done retry``) before a caller can exit (the
engine's F51). A poll refused at a limit (:class:`LimitError`) is logged at error and backs off;
the first one, at ``start()``, makes ``start()`` raise so a process does not run without work
silently.
The worker holds no state across items: the consumers, the
inbound event, the run lease, the ack matrix, the obligations after a terminal, the cascade, the
forward, the parked index and the lost-wake repair are the engine's.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from synadia_durable._engine.blobs import EngineBlobs, Sizes
from synadia_durable._engine.errors import (
    EngineError,
    InvalidRequestError,
    LeaseExpiredError,
    LimitError,
    UnsupportedEngineError,
    check_engine_floor,
)
from synadia_durable._engine.names import DEFAULT_PREFIX, Op
from synadia_durable._engine.payload import (
    WireJournal,
    fit_entry,
    fit_slot,
    reverse_journal,
    wire_entry,
)
from synadia_durable._engine.transport import Transport
from synadia_durable._internal.logger import Logger, SilentLogger
from synadia_durable._internal.metrics import Metrics, emit_safe
from synadia_durable.core.execute import ExecOutcome, execute_run
from synadia_durable.core.registry import build_registry
from synadia_durable.errors import (
    BlobIntegrityError,
    BlobMissingError,
    ConnectionClosedError,
    FencedError,
    PayloadCodecError,
    PayloadTooLargeError,
    SerializationError,
)
from synadia_durable.journal.codec import json_codec
from synadia_durable.journal.store import AppendResult, JournalAppend

if TYPE_CHECKING:
    from collections.abc import Callable

    from nats.aio.client import Client as NatsClient

    from synadia_durable.core.children import ChildStart
    from synadia_durable.journal.blobs import OffloadOptions
    from synadia_durable.journal.entry import LoadedEntry
    from synadia_durable.journal.payload_codec import PayloadCodec
    from synadia_durable.types import Workflow

__all__ = ["PumpHealth", "Worker", "WorkerHealth", "create_worker"]

#: How long one ``poll`` asks the engine to wait (API.md's loop: 30 s; the engine caps it).
DEFAULT_POLL_WAIT_MS = 30_000
#: How long ``stop()`` lets the items in flight finish before it returns anyway (the library's).
DEFAULT_DRAIN_TIMEOUT_S = 10.0
#: Work items this worker executes at once: one ``poll`` per slot.
DEFAULT_CONCURRENCY = 16


@dataclass
class PumpHealth:
    """One poll slot's liveness (the library's consumer pumps are the engine's now).

    ``alive`` means the slot's last poll was answered and it is polling again; a slot that is
    backing off after a failed poll is not alive, and ``restarts`` counts those failures.
    """

    stream: str
    durable: str
    alive: bool = False
    restarts: int = 0
    last_error: str | None = None


@dataclass(frozen=True, slots=True)
class WorkerHealth:
    """A cheap, synchronous, side-effect-free snapshot — for readiness and liveness endpoints."""

    #: ``start()`` called, ``stop()`` not yet, and the connection not closed for good.
    running: bool
    #: Work items held and not yet finished.
    inflight: int
    #: Distinct runs among them.
    queued_runs: int
    #: One entry per poll slot.
    pumps: list[PumpHealth]
    #: The caller's NATS connection has CLOSED for good: this worker will never receive work again.
    connection_closed: bool = False


def _poll_backoff_s(failures: int) -> float:
    """The back-off of a slot whose poll failed: 250 ms doubling to 5 s."""
    return min(5.0, 0.25 * float(2 ** min(max(0, failures - 1), 5)))


def limit_backoff_s(e: LimitError) -> float:
    """The back-off of a slot whose poll was refused at a limit (API.md "Errors"): ``rate`` clears
    within a second; the others when runs end, a worker goes away or an operator acts, so 30 s."""
    return 1.0 if e.limit == "rate" else 30.0


#: How often the worker says that its polls are failing (one line, with the count).
POLL_WARN_EVERY_S = 60.0


class EveryLog:
    """One log line per ``every_s`` for an event that repeats: :meth:`hit` counts one and answers
    how many the line about to be logged stands for (this one and those since the last line), or 0
    to stay quiet. A long idle over a slow link, every poll failing, then fills no log (F41)."""

    def __init__(self, every_s: float, now: Callable[[], float] = time.monotonic) -> None:
        self.every_s = every_s
        self._now = now
        self._last: float | None = None
        self._count = 0

    def hit(self) -> int:
        self._count += 1
        t = self._now()
        if self._last is not None and t - self._last < self.every_s:
            return 0
        self._last = t
        n, self._count = self._count, 0
        return n


class _Dropped:
    """The item's lease came back ``expired``: nothing more is sent under it."""

    def __init__(self) -> None:
        self.error: LeaseExpiredError | None = None


class Worker:
    """Executes workflows. Build one with :func:`create_worker`."""

    def __init__(
        self,
        nc: NatsClient,
        workflows: list[Workflow[Any, Any]],
        *,
        prefix: str = DEFAULT_PREFIX,
        dependencies: dict[str, Any] | None = None,
        payload_codec: PayloadCodec | None = None,
        concurrency: int = DEFAULT_CONCURRENCY,
        poll_wait_ms: int = DEFAULT_POLL_WAIT_MS,
        logger: Logger | None = None,
        metrics: Metrics | None = None,
        offload: bool = True,
    ) -> None:
        self._nc = nc
        self._prefix = prefix
        self._log: Logger = logger or SilentLogger()
        self._metrics = metrics
        self._request = Transport(nc, prefix, self._log)
        self._sizes = Sizes(self._request, lambda: nc.max_payload if nc.is_connected else 0)
        #: ``offload=False`` keeps the library's meaning: nothing goes up in parts.
        self._offload = offload
        self._codec = payload_codec
        # name → version → workflow. Raises on a duplicate (name, version): fail loud at
        # construction.
        self._registry = build_registry(workflows)
        self._workflows = [
            {"name": name, "versions": self._registry.versions(name)}
            for name in self._registry.names
        ]
        self._deps = dependencies or {}
        self._concurrency = max(1, int(concurrency))
        self._poll_wait_ms = max(0, int(poll_wait_ms))

        self._running = False
        self._closed_reported = False
        #: This worker's name on its polls, so that stop() withdraws its polls and no one else's.
        self._worker_id = str(uuid.uuid4())
        self._slots: list[PumpHealth] = []
        self._polls: list[asyncio.Task[None]] = []
        #: run_id of every item held right now, by item.
        self._held: dict[object, str] = {}
        self._inflight: set[asyncio.Task[None]] = set()
        #: The slots' polls awaiting their reply right now.
        self._polling = 0
        #: The engine's floor (``info.capabilities``), checked: at start() when an engine answered
        #: ``info``, else by the first slot whose poll is answered (one ``info`` for all meanwhile).
        self._floor_checked = False
        self._floor_check: asyncio.Future[UnsupportedEngineError | None] | None = None
        #: A stop() the worker began itself (an engine below the floor), kept from the collector.
        self._self_stop: asyncio.Task[None] | None = None
        #: The tail of each run's items executing on this worker (removed when it settles).
        self._run_chains: dict[str, asyncio.Task[None]] = {}
        #: Set by stop(): a slot backing off wakes at once.
        self._stopping: asyncio.Event | None = None
        #: The worker's "poll failed; retrying": one line a minute, however many slots fail.
        self._poll_warn = EveryLog(POLL_WARN_EVERY_S)
        #: A refusal at a limit: one error line per limit per back-off, however many slots.
        self._limit_logs: dict[str, EveryLog] = {}

    # ------------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        if self._running:
            return
        if self._nc.is_closed:
            raise ConnectionClosedError("create_worker: the NATS connection is closed")
        self._floor_checked = False
        self._self_stop = None
        try:
            info = await self._request.request("info", {})
        except ConnectionClosedError:
            raise
        except LimitError as e:
            self._refused_at_start(e, "info")
        except EngineError as e:
            # Not fatal: the polls retry until an engine answers, as a library worker rides out an
            # outage; the first slot whose poll is answered checks the floor then.
            self._log.warn(
                "durable: no engine answered info; polling until one does",
                prefix=self._prefix,
                error=str(e),
            )
        else:
            # The floor: an engine that does not announce what this SDK needs is refused here, by
            # its capabilities, never by its version (every build so far says 0.2.0-dev).
            below = check_engine_floor(self._prefix, info)
            if below is not None:
                self._log_below_floor(below, "start() raises")
                raise below
            self._floor_checked = True
        # The first poll (`waitMs: 0`), sent whether or not `info` answered: it checks once whether
        # the engine serves these workflows to this worker, and it is this worker's start to the
        # engine, which ends the 5 s it remembers a stop() of the same worker (its F53). An item it
        # is handed goes to the first slot.
        first: dict[str, Any] | None = None
        try:
            r = await self._request.request(
                "poll",
                {"workflows": self._workflows, "waitMs": 0, "worker": self._worker_id},
                wait_ms=0,
            )
            if not r.get("none"):
                first = r
        except ConnectionClosedError:
            raise
        except LimitError as e:
            self._refused_at_start(e, "first poll")
        except Exception:  # noqa: BLE001 — anything else the slots meet again and retry
            pass
        self._running = True
        self._stopping = asyncio.Event()
        self._slots = [
            PumpHealth(stream=f"{self._prefix}.api.v1.poll", durable=f"slot-{i}", alive=True)
            for i in range(self._concurrency)
        ]
        self._polls = [
            asyncio.ensure_future(self._poll_loop(s, first if i == 0 else None))
            for i, s in enumerate(self._slots)
        ]

    def _log_below_floor(self, e: UnsupportedEngineError, then: str) -> None:
        self._log.error(
            f"durable: {e}; {then}",
            prefix=self._prefix,
            engine=e.engine,
            commit=e.commit,
            capabilities=e.capabilities,
            missing=e.missing,
        )

    async def _below_floor(self) -> bool:
        """The floor, deferred: start() found no engine, and now a poll was answered. One ``info``
        for every slot asking meanwhile; a failure leaves it to the next answered poll. An engine
        below the floor stops this worker, loud: ``start()`` has returned, so the caller sees
        ``health()`` stopped."""
        if self._floor_checked:
            return False
        if self._floor_check is None:
            self._floor_check = asyncio.ensure_future(self._check_floor())
        check = self._floor_check
        try:
            refused = await asyncio.shield(check)
        finally:
            if self._floor_check is check and check.done():
                self._floor_check = None
        if refused is None:
            return False
        if self._self_stop is None and self._running:
            self._log_below_floor(refused, "this worker stops")
            self._self_stop = asyncio.ensure_future(self.stop())
        return True

    async def _check_floor(self) -> UnsupportedEngineError | None:
        try:
            info = await self._request.request("info", {})
        except Exception as e:  # noqa: BLE001 — the next answered poll asks again
            self._log.debug(
                "durable: info failed; the floor is checked at the next answered poll", error=str(e)
            )
            return None
        self._floor_checked = True
        return check_engine_floor(self._prefix, info)

    def _refused_at_start(self, e: LimitError, where: str) -> None:
        """A refusal at a limit (but ``rate``, which clears within a second) makes ``start()``
        raise: a process must not run without work silently (the engine's F46)."""
        if e.limit == "rate":
            return
        self._log.error(
            f"durable: the engine refused this worker's {where} at a limit ({e.code} {e.limit}); "
            "start() raises",
            code=e.code,
            limit=e.limit,
            workflows=[w["name"] for w in self._workflows],
            error=str(e),
        )
        raise e

    async def stop(self, drain_timeout_s: float = DEFAULT_DRAIN_TIMEOUT_S) -> None:
        """Stop polling, and let the items in flight finish (bounded by ``drain_timeout_s``).

        This worker's polls are withdrawn, once (``poll {cancelPoll}``, API.md "Withdrawing
        polls"): every instance answers those waiting ``none`` at once and never hands them an
        item, and remembers the worker 5 s, so a poll of it that arrives after the withdrawal (sent
        just before it) answers ``none`` at once too (the engine's F53, the floor ``start()``
        checked). An item handed in the instant before goes straight back (``done retry``), and
        ``stop()`` returns only once every slot's loop has ended, that hand-back included, so a
        caller that exits right after loses nothing to a lease nobody holds (the engine's F51).
        Without an engine to answer, the polls end at their own wait.

        Past the timeout the items still in flight, and the slots, are cancelled, as the library's
        ``stop()`` did: a shutdown must complete. They send no ``done``, so their leases lapse and
        the items redeliver at the ack wait, which is correct, just slower.
        """
        was_running = self._running
        self._running = False
        if self._stopping is not None:
            self._stopping.set()
        for s in self._slots:
            s.alive = False
        if was_running:
            await self._withdraw()
        # Every slot's loop ends: its item in flight finishes, and its poll returns (an item it is
        # handed now goes back before this does).
        current = asyncio.current_task()
        pending = {t for t in self._polls if not t.done() and t is not current}
        if pending:
            _done, pending = await asyncio.wait(pending, timeout=max(0.0, drain_timeout_s))
        if pending:
            items = [t for t in self._inflight if not t.done()]
            self._log.warn(
                "durable: stop() cancelled items or polls still in flight; they redeliver at the "
                "ack wait",
                inflight=len(items),
                polling=self._polling,
            )
            for task in (*items, *pending):
                task.cancel()
            for task in (*items, *pending):
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task

    async def _withdraw(self) -> None:
        """``poll {cancelPoll}`` for this worker's polls; failing, they end at their own wait."""
        if self._nc.is_closed:
            return
        try:
            await self._request.request(
                "poll", {"workflows": [], "worker": self._worker_id, "cancelPoll": True}
            )
        except Exception as e:  # noqa: BLE001 — the polls then end at their own wait
            self._log.debug("durable: withdrawing the polls failed; they end at their wait", error=str(e))  # fmt: skip

    def health(self) -> WorkerHealth:
        """A snapshot of what this worker is doing right now. Never touches the network."""
        closed = bool(self._nc.is_closed)
        live = self._running and not closed
        return WorkerHealth(
            running=live,
            inflight=len(self._held),
            queued_runs=len(set(self._held.values())),
            pumps=[replace(p, alive=p.alive and live) for p in self._slots],
            connection_closed=closed,
        )

    async def __aenter__(self) -> Worker:
        await self.start()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.stop()

    # ------------------------------------------------------------------ the loop

    def _report_connection_closed(self) -> None:
        if self._closed_reported:
            return
        self._closed_reported = True
        self._log.error(
            "durable: the NATS connection is CLOSED for good — this worker will never receive work "
            "again and cannot recover on its own (the connection is the caller's). Restart it on a "
            "new connection, and connect with max_reconnect_attempts=-1 (with_reconnect_defaults) "
            "so an outage is ridden out instead of given up on. Runs are unaffected: other "
            "workers, or this one restarted, resume them from the journal."
        )

    async def _nap(self, seconds: float) -> None:
        """A sleep that stop() cuts short."""
        if self._stopping is None:
            await asyncio.sleep(seconds)
            return
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), seconds)

    def _log_limit(self, e: LimitError, where: str, **fields: Any) -> None:
        key = f"{where}|{e.code}|{e.limit}"
        every = self._limit_logs.get(key)
        if every is None:
            every = self._limit_logs[key] = EveryLog(limit_backoff_s(e))
        n = every.hit()
        if n == 0:
            return
        then = (
            f"polling again in {limit_backoff_s(e):g} s"
            if where == "poll"
            else "the item goes back until it clears"
        )
        self._log.error(
            f"durable: the engine refused this worker's {where} at a limit ({e.code} {e.limit}); "
            f"{then}",
            **fields,
            code=e.code,
            limit=e.limit,
            refused=n,
            error=str(e),
        )

    def _is_running(self) -> bool:
        """``_running``, read through a call: stop() flips it while a poll is awaited."""
        return self._running

    async def _poll_loop(self, state: PumpHealth, first: dict[str, Any] | None = None) -> None:
        failures = 0
        while self._running:
            if self._nc.is_closed:
                self._report_connection_closed()
                break
            try:
                if first is not None:
                    # The item start()'s first poll was handed.
                    r, first = first, None
                else:
                    self._polling += 1
                    try:
                        r = await self._request.request(
                            "poll",
                            {
                                "workflows": self._workflows,
                                "waitMs": self._poll_wait_ms,
                                "worker": self._worker_id,
                            },
                            wait_ms=self._poll_wait_ms,
                        )
                    finally:
                        self._polling -= 1
                failures = 0
                state.alive = self._running
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — a failed poll is retried, whatever it was
                if isinstance(e, ConnectionClosedError) or self._nc.is_closed:
                    if self._running:
                        self._report_connection_closed()
                    break
                if isinstance(e, (BlobMissingError, BlobIntegrityError)):
                    # The engine parked the run whose item this poll was to be handed (its journal
                    # met a gone blob at the hand-off, API.md `poll`): the run's, not this worker's.
                    # Loud, and poll again.
                    self._log.error(
                        "durable: the engine parked a run whose blob is gone — restore the blob or "
                        "cancel the run",
                        run_id=e.run_id,
                        blob=e.blob_name,
                        error=str(e),
                    )
                    continue
                failures += 1
                state.alive = False
                state.restarts += 1
                state.last_error = str(e)
                if isinstance(e, LimitError):
                    # Refused at a limit (the engine's F46): never a broker blip. Loud, and polled
                    # again only after the limit could have cleared.
                    self._log_limit(e, "poll", workflows=[w["name"] for w in self._workflows])
                    if self._running:
                        await self._nap(limit_backoff_s(e))
                    continue
                if isinstance(e, InvalidRequestError):
                    self._log.error("durable: the engine refused this worker's poll", error=str(e))
                else:
                    n = self._poll_warn.hit()
                    if n > 0:
                        self._log.warn(
                            "durable: poll failed; retrying",
                            error=str(e),
                            failures=n,
                            every_s=POLL_WARN_EVERY_S,
                        )
                if self._running:
                    await self._nap(_poll_backoff_s(failures))
                continue
            if not self._floor_checked and await self._below_floor():
                if not r.get("none"):
                    await self._release(r)
                break
            if r.get("none"):
                continue
            if not self._is_running():
                await self._release(r)
                break
            run_id = str(r["runId"])
            key = object()
            self._held[key] = run_id
            task = asyncio.ensure_future(self._process_item(r, self._run_chains.get(run_id)))
            self._run_chains[run_id] = task
            self._inflight.add(task)
            try:
                await asyncio.wait({task})
            finally:
                self._inflight.discard(task)
                self._held.pop(key, None)
                if self._run_chains.get(run_id) is task:
                    del self._run_chains[run_id]
        state.alive = False

    def _park_on_blob(self, run_id: str, e: BlobMissingError | BlobIntegrityError) -> str:
        """An offloaded payload that cannot be resolved (gone, or its digest wrong) makes the
        journal unreadable: operational, so hand the item back (the engine naks with backoff and
        parks the run too), loud, and never a terminal. It heals when the blob is restored, or the
        run is cancelled."""
        self._log.error(
            "durable: offloaded payload unresolvable; parking run — restore the blob or cancel "
            "the run",
            run_id=run_id,
            blob=e.blob_name,
            error=str(e),
        )
        return "blob-unresolvable"

    async def _release(self, it: dict[str, Any]) -> None:
        """Hand an item back unexecuted (a poll answered after stop()): ``done retry``, the engine
        naks it."""
        with contextlib.suppress(Exception):  # the lease lapses at its ack wait all the same
            await self._request.request(
                "done", {"runId": it["runId"], "lease": it["lease"], "outcome": {"kind": "retry"}}
            )

    # ------------------------------------------------------------------ one item

    async def _process_item(self, it: dict[str, Any], before: asyncio.Task[None] | None) -> None:
        """One item, from its journal to its ``done``. Never raises.

        ``before`` is the run's previous item on this worker, still executing (a redelivery after
        its lease ended): this one waits for it, its lease kept live meanwhile, so one process never
        executes a run twice at once.
        """
        run_id = str(it["runId"])
        lease = str(it["lease"])
        workflow = str(it.get("workflow", ""))
        version = int(it.get("version", 1))
        attempt = int(it.get("attempt", 1))
        kind = str((it.get("item") or {}).get("kind", ""))
        started = time.monotonic()
        emit_safe(
            self._metrics,
            "work.dispatch",
            {"workflow": workflow, "run_id": run_id, "kind": kind, "inflight": len(self._held)},
        )
        dropped = _Dropped()
        leased = _Leased(self._request, self._log, run_id, lease, dropped)
        transport = self._request

        async def blob_request(
            op: Op, body: dict[str, Any], *, timeout_ms: int | None = None
        ) -> dict[str, Any]:
            # A part of this run's blob carries the lease; another run's (a child's input) does not.
            if "lease" in body:
                return await leased(op, body, timeout_ms=timeout_ms)
            return await transport.request(op, body, timeout_ms=timeout_ms)

        blobs = EngineBlobs(
            blob_request,
            self._sizes,
            lease=(run_id, lease),
            metrics=self._metrics,
            rtt=self._nc.rtt,
        )
        sizes = self._sizes if self._offload else None
        journal = _EngineJournal(leased, self._codec, self._metrics, blobs, sizes)
        scheduler = _EngineScheduler(leased)
        children = _EngineChildren(leased, self._codec, blobs, sizes)

        outcome = ExecOutcome(kind="retry")
        #: The library's `work.done` outcome when it is not the ExecOutcome's kind.
        label: str | None = None
        # Extend from dispatch: the wait behind `before` and the journal's pages count too.
        extender = asyncio.ensure_future(
            _extend_every(leased, max(0.1, int(it.get("ackWaitMs", 30_000)) / 3 / 1000), self._log)
        )
        try:
            if before is not None:
                await asyncio.wait({before})
            loaded = await journal.load_pages(it.get("journal") or [], bool(it.get("more")))
            wf = self._registry.resolve(workflow, version)
            if wf is None:
                # The engine hands an item only to a poll that names its version; reaching this is
                # a bug.
                self._log.error(
                    "durable: an item for a version this worker does not run; handing it back",
                    run_id=run_id,
                    workflow=workflow,
                    version=version,
                )
                outcome = ExecOutcome(
                    kind="retry", cause=RuntimeError(f"{workflow} v{version} not registered")
                )
            else:
                outcome = await execute_run(
                    run_id,
                    workflow=wf,
                    journal=journal,
                    loaded=loaded,
                    scheduler=scheduler,
                    children=children,
                    dependencies=self._deps,
                    attempt=attempt,
                )
        except asyncio.CancelledError:
            raise
        except (BlobMissingError, BlobIntegrityError) as e:
            label = self._park_on_blob(run_id, e)
            outcome = ExecOutcome(kind="retry", cause=e)
        except PayloadCodecError as e:
            label = "payload-codec"
            # The journal is readable but its payloads are not, and every cause (no codec, the
            # wrong key, altered bytes) is operational: hand it back (the engine naks with backoff),
            # loud.
            self._log.error(
                "durable: payload codec could not decode this run's payloads; parking run — check "
                "that this worker has the same payload_codec (and key) as the writer",
                run_id=run_id,
                error=str(e),
            )
            outcome = ExecOutcome(kind="retry", cause=e)
        except Exception as e:  # noqa: BLE001 — everything left is infrastructure: hand it back
            if isinstance(e, LimitError):
                self._log_limit(e, "request", run_id=run_id)
            elif not isinstance(e, LeaseExpiredError):
                self._log.warn("durable: work item failed; will redeliver", run_id=run_id, error=str(e))  # fmt: skip
            outcome = ExecOutcome(kind="retry", cause=e)
        finally:
            extender.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await extender

        # A blob the engine said is gone (`blob_gone`) under a step-end `load` or a ref's `get`
        # inside the execution: the pure core halted with `retry` on it, as on any infrastructure
        # error; park it as the library did, with its label and its loud line.
        if (
            label is None
            and outcome.kind == "retry"
            and isinstance(outcome.cause, (BlobMissingError, BlobIntegrityError))
        ):
            label = self._park_on_blob(run_id, outcome.cause)
        reported = label or outcome.kind
        if (
            outcome.kind == "retry"
            and label is None
            and not isinstance(outcome.cause, LeaseExpiredError)
            and dropped.error is None
        ):
            # Refused at a limit inside the execution (a child at `runs`, an append at `bytes`):
            # the item goes back and the run waits until it clears (API.md "Errors"), loud.
            if isinstance(outcome.cause, LimitError):
                reported = "limit"
                self._log_limit(outcome.cause, "request", run_id=run_id)
            else:
                self._log.warn("durable: infra error during run; will retry", run_id=run_id, error=str(outcome.cause))  # fmt: skip
        if dropped.error is not None:
            reported = "expired"
        else:
            try:
                await self._request.request(
                    "done", {"runId": run_id, "lease": lease, "outcome": _wire_outcome(outcome)}
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — a failed done redelivers at the ack wait
                if isinstance(e, LeaseExpiredError):
                    leased.drop(e)
                else:
                    self._log.warn("durable: done failed; the item redelivers at its ack wait", run_id=run_id, error=str(e))  # fmt: skip
                reported = "expired" if dropped.error is not None else "error"
        emit_safe(
            self._metrics,
            "work.done",
            {
                "workflow": workflow,
                "run_id": run_id,
                "kind": kind,
                "outcome": reported,
                "ms": round((time.monotonic() - started) * 1000),
                "wait_ms": 0,
                "delivery_count": attempt,
            },
        )


class _Leased:
    """Every request for one item carries its lease; after an ``expired`` nothing more is sent."""

    def __init__(
        self, request: Transport, log: Logger, run_id: str, lease: str, dropped: _Dropped
    ) -> None:
        self._request = request
        self._log = log
        self.run_id = run_id
        self._lease = lease
        self._dropped = dropped

    def drop(self, e: LeaseExpiredError) -> None:
        if self._dropped.error is not None:
            return
        self._dropped.error = e
        self._log.warn(
            "durable: the item's lease expired; dropping it (it redelivers to a poll)",
            run_id=self.run_id,
            error=str(e),
        )

    async def __call__(
        self, op: Op, body: dict[str, Any], *, timeout_ms: int | None = None
    ) -> dict[str, Any]:
        if self._dropped.error is not None:
            raise LeaseExpiredError(op, f"run {self.run_id}: the item's lease expired; not sent")
        try:
            return await self._request.request(
                op, {**body, "lease": self._lease}, timeout_ms=timeout_ms
            )
        except LeaseExpiredError as e:
            self.drop(e)
            raise


async def _extend_every(leased: _Leased, every_s: float, log: Logger) -> None:
    while True:
        await asyncio.sleep(every_s)
        try:
            await leased("extend", {"runId": leased.run_id})
        except asyncio.CancelledError:
            raise
        except LeaseExpiredError:
            pass
        except Exception as e:  # noqa: BLE001 — a missed extend is the next one's to make up
            log.debug("durable: extend failed", run_id=leased.run_id, error=str(e))


class _EngineJournal:
    """The Journal seam: ``load`` pages the run's journal in, ``append`` is one ``append``."""

    def __init__(
        self,
        leased: _Leased,
        codec: PayloadCodec | None,
        metrics: Metrics | None,
        blobs: EngineBlobs,
        sizes: Sizes | None,
    ) -> None:
        self._leased = leased
        self._codec = codec
        self._metrics = metrics
        self._blobs = blobs
        self._sizes = sizes

    async def load_pages(self, first: WireJournal, more: bool) -> list[LoadedEntry]:
        """The run's journal: ``first`` (the poll's), then ``load`` pages while ``more``, every ref
        fetched and every ``<slot>Enc`` reversed. One ``journal.load`` event per call: every replay
        starts with one, as the library's, and ``bytes`` is the journal as the engine handed it."""
        run_id = self._leased.run_id
        started = time.monotonic()
        wire = list(first)
        while more:
            after = int(wire[-1]["streamSeq"]) if wire else 0
            r = await self._leased("load", {"runId": run_id, "after": after})
            page = r.get("journal") or []
            wire.extend(page)
            more = bool(r.get("more")) and len(page) > 0
        out = await reverse_journal(run_id, wire, self._codec, self._blobs)
        emit_safe(
            self._metrics,
            "journal.load",
            {
                "run_id": run_id,
                "entries": len(out),
                # The journal as the engine handed it (refs and `<slot>Enc` as they came).
                "bytes": sum(len(json_codec.encode(le["entry"])) for le in wire),
                "ms": round((time.monotonic() - started) * 1000),
                "outcome": "ok",
            },
        )
        return out

    async def load(self, run_id: str) -> list[LoadedEntry]:
        _ = run_id  # the item's run: the seam is per item
        return await self.load_pages([], True)

    async def append(self, run_id: str, append: JournalAppend) -> AppendResult:
        started = time.monotonic()
        size = 0

        def emit(outcome: str) -> None:
            emit_safe(
                self._metrics,
                "journal.append",
                {
                    "run_id": run_id,
                    "kind": str(append.entry["kind"]),
                    "seq": int(append.entry["seq"]),
                    "bytes": size,
                    "ms": round((time.monotonic() - started) * 1000),
                    "outcome": outcome,
                },
            )

        try:
            envelope = {
                "runId": run_id,
                "msgID": append.msg_id,
                "expectLastSeq": append.expect_last_seq,
                "lease": "x" * 64,
            }
            entry = await wire_entry(run_id, append.entry, self._codec)
            entry = await fit_entry(run_id, entry, envelope, self._sizes, self._blobs)
            size = len(json_codec.encode(entry))
            r = await self._leased(
                "append",
                {
                    "runId": run_id,
                    "entry": entry,
                    "msgID": append.msg_id,
                    "expectLastSeq": append.expect_last_seq,
                },
            )
        except Exception as e:
            emit(_outcome_of_append_error(e))
            raise
        duplicate = r.get("duplicate") is True
        emit("duplicate" if duplicate else "ok")
        return AppendResult(stream_seq=int(r["streamSeq"]), duplicate=duplicate)


class _EngineScheduler:
    """The Scheduler seam: ``wake``, under the item's lease."""

    def __init__(self, leased: _Leased) -> None:
        self._leased = leased

    async def schedule_wake(self, run_id: str, workflow: str, seq: int, due_ms: int) -> None:
        await self._leased("wake", {"runId": run_id, "wf": workflow, "seq": seq, "dueMs": due_ms})


class _EngineChildren:
    """The Children seam: ``child``, under the item's lease."""

    def __init__(
        self,
        leased: _Leased,
        codec: PayloadCodec | None,
        blobs: EngineBlobs,
        sizes: Sizes | None,
    ) -> None:
        self._leased = leased
        self._codec = codec
        self._blobs = blobs
        self._sizes = sizes

    async def start(self, child: ChildStart) -> None:
        body: dict[str, Any] = {
            "childRunId": child.child_run_id,
            "workflow": child.workflow,
            "version": child.version,
        }
        parent = {"runId": child.parent.run_id, "seq": child.parent.seq, "wf": child.parent.wf}
        # A child's input too large for the request is `put` under the CHILD's run.
        body |= await fit_slot(
            "input",
            child.input,
            self._codec,
            child.child_run_id,
            {**body, "parent": parent, "lease": "x" * 64},
            self._sizes,
            self._blobs,
        )
        body["parent"] = parent
        await self._leased("child", body)


def _wire_outcome(o: ExecOutcome) -> dict[str, Any]:
    """The ``done`` outcome: its kind (the engine acts on the journal's terminal, never on a
    value)."""
    if o.kind == "retry":
        e = o.cause
        if e is None:
            return {"kind": "retry"}
        return {"kind": "retry", "error": {"name": type(e).__name__, "message": str(e)}}
    return {"kind": o.kind}


def _outcome_of_append_error(e: BaseException) -> str:
    if isinstance(e, FencedError):
        return "fenced"
    if isinstance(e, PayloadTooLargeError):
        return "payload-too-large"
    if isinstance(e, SerializationError):
        return "serialization"
    if isinstance(e, PayloadCodecError):
        return "payload-codec"
    return "error"


def create_worker(
    nc: NatsClient,
    workflows: list[Workflow[Any, Any]],
    *,
    prefix: str = DEFAULT_PREFIX,
    dependencies: dict[str, Any] | None = None,
    ack_wait_ms: int | None = None,
    version_wait_ms: int | None = None,
    stream_max_bytes: int | None = None,
    offload: bool | OffloadOptions = True,
    payload_codec: PayloadCodec | None = None,
    maintenance_interval_ms: int | None = None,
    reconcile_timers: bool = True,
    reconcile_grace_ms: int | None = None,
    lease: bool = True,
    lease_ttl_ms: int | None = None,
    max_work_ms: int | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
    logger: Logger | None = None,
    metrics: Metrics | None = None,
    poll_wait_ms: int = DEFAULT_POLL_WAIT_MS,
) -> Worker:
    """Build a worker on a caller-owned NATS connection (bring your own connection).

    Call :meth:`Worker.start` to begin polling ``{prefix}.api.v1.poll``, or use it as an async
    context manager. ``concurrency`` (default 16) is the number of poll slots, so the number of
    items executing at once; items of one run never overlap (the engine hands a run's items one at
    a time, and a redelivery waits behind the same run's item still executing here).
    ``poll_wait_ms`` (default 30 000) is how long one poll waits; the engine caps it at its
    ``max_poll_wait_ms``. ``metrics`` sees ``journal.append``, ``journal.load``, ``work.dispatch``
    and ``work.done``.

    The library's options that are ``durable-engine serve``'s flags now are accepted, so a library
    worker's configuration still runs, and ignored: ``ack_wait_ms`` (``--ack-wait``),
    ``max_work_ms`` (``--max-work``), ``lease=False`` (``--no-lease``), ``lease_ttl_ms``
    (``--lease-ttl``), ``stream_max_bytes`` (``--stream-max-bytes``), ``maintenance_interval_ms``
    (``--maintenance-interval``), ``reconcile_grace_ms`` (``--reconcile-grace``),
    ``reconcile_timers`` (always on), ``version_wait_ms`` (the engine parks an unpolled version) and
    ``offload``'s tuning (``info``'s ``offload_threshold``, ``serve --offload-threshold``).
    ``offload=False`` keeps the library's meaning: nothing goes up in parts, and a value too large
    for a request fails the run loud (``PayloadTooLargeError``).
    """
    _ = (
        ack_wait_ms,
        version_wait_ms,
        stream_max_bytes,
        maintenance_interval_ms,
        reconcile_timers,
        reconcile_grace_ms,
        lease,
        lease_ttl_ms,
        max_work_ms,
    )
    return Worker(
        nc,
        workflows,
        prefix=prefix,
        dependencies=dependencies,
        payload_codec=payload_codec,
        concurrency=concurrency,
        poll_wait_ms=poll_wait_ms,
        logger=logger,
        metrics=metrics,
        offload=offload is not False,
    )
