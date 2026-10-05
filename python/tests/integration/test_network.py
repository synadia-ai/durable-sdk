"""The network axes: LATENCY and DISCONNECTS (``docs/plans/test-axes.md``, increment 3).

Every other suite talks to a server ~0.1 ms away over a link that never drops, so the engine's
timing constants and its reconnect behaviour had only ever been argued. Here the library's
connections go through ``tests/netproxy.py`` — a byte-level TCP proxy that adds round-trip time,
resets connections, and withholds replies — while the test's OWN observations go straight to the
server. Twin of ``test/integration/network.test.ts``.

The instrument is tested first: a proxy that silently added no delay would turn every latency test
below into a test at 0.1 ms that passes.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from typing import Any
from urllib.parse import urlparse

import pytest

import existing
from conftest import (
    limits_for,
    list_ephemeral_consumers,
    profile_from_env,
    spawn_server,
)
from engine import (
    EngineFlags,
    create_client,
    create_worker,
    engine_log,
    provision,
    set_engine_server,
)
from netproxy import ProxyProcess
from shell.journal_store import NatsJournal
from shell.names import Names
from synadia_durable import (
    ConnectionClosedError,
    DurableTimeoutError,
    define_workflow,
)
from synadia_durable._internal.json import encode_json
from synadia_durable.journal.entry import TERMINAL_KINDS
from synadia_durable.types import Ctx, StartOptions

# durable-sdk: every test here owns the link or the server, which an existing server (DE_NATS_URL)
# does not give it (tests/existing.py).
pytestmark = [pytest.mark.asyncio, existing.skip_in_mode(existing.OWN_LINK)]

MB = 1024 * 1024
SIZED: dict[str, Any] = {"stream_max_bytes": 16 * MB}
COMMAND_KINDS = ("step-begin", "step-end", "now", "random", "uuid", "sleep", "await", "call")


class Recorder:
    """One event log shared by every worker in a test; ``who`` says which worker spoke."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def sink(self, who: str) -> Any:
        recorder = self

        class _Sink:
            def on_event(self, name: str, fields: Mapping[str, Any]) -> None:
                recorder.events.append(
                    {"name": name, "who": who, "at": time.monotonic(), **dict(fields)}
                )

        return _Sink()

    def named(self, name: str) -> list[dict[str, Any]]:
        return [e for e in self.events if e["name"] == name]

    def tally(self, name: str, field: str) -> dict[str, int]:
        out: dict[str, int] = {}
        for e in self.named(name):
            out[str(e.get(field))] = out.get(str(e.get(field)), 0) + 1
        return out


class Link:
    """Counts a connection's disconnects and reconnects (the emergence check of every cut test)."""

    def __init__(self) -> None:
        self.disconnects = 0
        self.reconnects = 0

    async def on_disconnect(self) -> None:
        self.disconnects += 1

    async def on_reconnect(self) -> None:
        self.reconnects += 1


class Scope:
    """Everything a test opened, closed in reverse order whatever happened in the test body."""

    def __init__(self) -> None:
        self._closers: list[Callable[[], Awaitable[Any]]] = []

    def defer(self, fn: Callable[[], Awaitable[Any]]) -> None:
        self._closers.append(fn)

    def proxy(self, target: str, rtt_ms: float = 0) -> ProxyProcess:
        p = ProxyProcess(target, rtt_ms)

        async def stop() -> None:
            p.stop()

        self.defer(stop)
        return p

    async def conn(self, url: str, *, reconnect: bool = True, link: Link | None = None) -> Any:
        """A connection that reconnects fast and forever — the posture a worker should run with."""
        nc = await existing.connect(
            url,
            allow_reconnect=reconnect,
            reconnect_time_wait=0.1,
            max_reconnect_attempts=-1,
            **(
                {"disconnected_cb": link.on_disconnect, "reconnected_cb": link.on_reconnect}
                if link is not None
                else {}
            ),
        )
        self.defer(nc.close)
        return nc

    def worker(self, w: Any) -> Any:
        self.defer(w.stop)
        return w

    async def close(self) -> None:
        for fn in reversed(self._closers):
            # A closer failing must not hide the test's own failure.
            with contextlib.suppress(Exception):
                await fn()
        self._closers.clear()


@pytest.fixture
async def scope() -> AsyncIterator[Scope]:
    s = Scope()
    try:
        yield s
    finally:
        await s.close()


async def _wait_until(
    condition: Callable[[], Awaitable[bool]], timeout_s: float, what: str
) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if await condition():
            return
        await asyncio.sleep(0.025)
    raise AssertionError(f"timed out waiting for {what}")


async def _journal(nc: Any, prefix: str, run_id: str) -> list[dict[str, Any]]:
    return [dict(le.entry) for le in await NatsJournal(nc, Names(prefix)).load_raw(run_id)]


async def _assert_journal_sane(nc: Any, prefix: str, run_id: str) -> None:
    """One terminal, and no command position journaled twice — whatever the network did."""
    entries = await _journal(nc, prefix, run_id)
    terminals = [e for e in entries if e["kind"] in TERMINAL_KINDS]
    assert len(terminals) == 1, f"terminals of {run_id}: {terminals}"
    seen: dict[str, int] = {}
    for e in entries:
        if e["kind"] in COMMAND_KINDS:
            key = f"{e['kind']}:{e['seq']}"
            seen[key] = seen.get(key, 0) + 1
    assert [k for k, n in seen.items() if n > 1] == [], f"duplicated positions in {run_id}"


# --- the proxy itself (the instrument) ---------------------------------------------------------


async def _rtt_ms(nc: Any) -> float:
    samples = []
    for _ in range(5):
        t0 = time.perf_counter()
        await nc.flush()  # PING → PONG: exactly one round trip
        samples.append((time.perf_counter() - t0) * 1000)
    return sorted(samples)[2]  # the median


async def test_the_proxy_adds_the_round_trip_it_says_and_none_when_told_none(
    scope: Scope, nats_direct_url: str
) -> None:
    proxy = scope.proxy(nats_direct_url, 0)
    nc = await scope.conn(proxy.url)
    assert await _rtt_ms(nc) < 20  # the control: no delay configured, none added
    proxy.set_rtt(120)
    slow = await _rtt_ms(nc)
    assert 118 <= slow < 170, slow


async def test_the_proxy_keeps_a_stream_in_order_when_the_delay_drops_mid_flight(
    scope: Scope, nats_direct_url: str
) -> None:
    proxy = scope.proxy(nats_direct_url, 200)
    pub = await scope.conn(proxy.url)
    direct = await scope.conn(nats_direct_url)
    got: list[int] = []

    async def on_msg(msg: Any) -> None:
        got.append(int(msg.data))

    await direct.subscribe("net.order", cb=on_msg)
    await direct.flush()
    for i in range(50):
        await pub.publish("net.order", str(i).encode())
        if i == 25:
            # The first half is IN the delay queue; remove the delay: the second half would
            # overtake it if the queue released by delay rather than by arrival order.
            await asyncio.sleep(0.02)
            proxy.set_rtt(0)

    async def all_in() -> bool:
        return len(got) == 50

    await _wait_until(all_in, 5, "all 50 messages")
    assert got == list(range(50))


async def test_the_proxy_delivers_what_was_written_before_a_close(
    scope: Scope, nats_direct_url: str
) -> None:
    """A final ack must not die in the delay queue: bytes, THEN the FIN, as on a real link."""
    proxy = scope.proxy(nats_direct_url, 150)
    direct = await scope.conn(nats_direct_url)
    got: list[bytes] = []

    async def on_msg(msg: Any) -> None:
        got.append(msg.data)

    await direct.subscribe("net.lastwords", cb=on_msg)
    await direct.flush()
    # A raw socket, so "written, then closed" is exactly that: a client library's close() is free
    # to discard its own outbound buffer, which would test the library and not the proxy.
    port = urlparse(proxy.url).port
    assert port is not None
    with socket.create_connection(("127.0.0.1", port)) as sock:
        sock.sendall(b'CONNECT {"verbose":false}\r\nPUB net.lastwords 3\r\nbye\r\n')
        sock.shutdown(socket.SHUT_WR)  # FIN 75 ms before the bytes are due

        async def arrived() -> bool:
            return got == [b"bye"]

        await _wait_until(arrived, 3, "the message published just before close")


async def test_cut_resets_connections_and_the_client_reconnects_through_the_proxy(
    scope: Scope, nats_direct_url: str
) -> None:
    proxy = scope.proxy(nats_direct_url, 0)
    link = Link()
    nc = await scope.conn(proxy.url, link=link)
    assert proxy.stats()[0] == 1
    assert proxy.cut() == 1

    async def back() -> bool:
        return link.reconnects == 1

    await _wait_until(back, 5, "the reconnect")
    # Had the client learned the server's real port from INFO, it would be back without this.
    assert proxy.stats()[0] == 2
    await nc.flush()


async def test_stall_down_the_request_lands_and_the_reply_is_withheld(
    scope: Scope, nats_direct_url: str
) -> None:
    proxy = scope.proxy(nats_direct_url, 0)
    nc = await scope.conn(proxy.url)
    direct = await scope.conn(nats_direct_url)
    landed = 0

    async def echo(msg: Any) -> None:
        nonlocal landed
        landed += 1
        await msg.respond(b"pong")

    await direct.subscribe("net.echo", cb=echo)
    await direct.flush()
    proxy.set_stall("down")
    req = asyncio.ensure_future(nc.request("net.echo", b"ping", timeout=5))

    async def has_landed() -> bool:
        return landed == 1

    await _wait_until(has_landed, 2, "the request to land at the server")
    await asyncio.sleep(0.3)
    assert not req.done()
    proxy.set_stall("none")
    assert (await req).data == b"pong"


# --- latency -----------------------------------------------------------------------------------


async def test_a_five_step_run_costs_a_pinned_number_of_dependent_round_trips(
    scope: Scope, nats_direct_url: str, prefix: str
) -> None:
    """The cost budget counts REQUESTS. It cannot see a request that became SEQUENTIAL: at 0.1 ms a
    new dependent round trip costs nothing, at 50 ms it is the latency of every run. So measure the
    only thing that shows it — wall time at a known RTT, minus the same run at no RTT, in RTTs."""
    rtt = 100.0
    proxy = scope.proxy(nats_direct_url, 0)
    wnc = await scope.conn(proxy.url)
    cnc = await scope.conn(proxy.url)

    async def body(ctx: Ctx, n: int) -> int:
        acc = 0
        for i in range(n):
            acc += await ctx.run(f"s{i}", lambda i=i: _identity(i))
        return acc

    wf = define_workflow("pynet_steps", body)
    worker = scope.worker(create_worker(wnc, [wf], prefix=prefix, **SIZED))
    await worker.start()
    client = create_client(cnc, prefix=prefix, **SIZED)

    async def once(n: int) -> float:
        t0 = time.perf_counter()
        await (await client.start(wf, n)).result(30_000)
        ms = (time.perf_counter() - t0) * 1000
        await asyncio.sleep(0.25)  # the worker's tail (ack, lease release) must not bleed over
        return ms

    await once(5)  # warm-up: provisioning, the first pull

    async def round_trips(n: int) -> float:
        proxy.set_rtt(0)
        base = min(await once(n), await once(n))
        floor = await _rtt_ms(cnc)
        proxy.set_rtt(rtt)
        await asyncio.sleep(rtt / 1000)  # let the idle pull's in-flight bytes settle
        # Divide by the round trip the proxy ACTUALLY adds, not the one it was asked for: its
        # timers overshoot by a few percent, and over ~19 hops that reads as half a round trip.
        added = await _rtt_ms(cnc) - floor
        slow = min(await once(n), await once(n))
        return (slow - base) / added

    five = await round_trips(5)
    six = await round_trips(6)
    print(f"\ndependent round trips: 5-step run {five:.2f}, 6-step run {six:.2f}")

    # The instrument resolves ONE step: a step is two dependent appends (begin, end). If this did
    # not hold, the pin below could not see a new sequential request either.
    assert 1.5 < six - five < 2.5, (five, six)
    assert five < PINNED_ROUND_TRIPS_5_STEPS + 0.5, five


#: Derived, then measured (19.0 ± 0.1 over repeated runs, the same number as the TypeScript SDK):
#: start() is 3 (journal start, status hint, work item — the item reaches the worker as its ack
#: reaches the client); the worker's load 1, lease 1, post-gate reload 1; ten step appends; the
#: result 1; the status finalize travelling on to the waiting client as its hint 1; the client's
#: terminal read 1.
PINNED_ROUND_TRIPS_5_STEPS = 19.0


async def test_a_worker_start_costs_a_pinned_number_of_dependent_round_trips(
    scope: Scope, nats_direct_url: str, prefix: str
) -> None:
    """The fabric test repo's F25: 5 s per start against NGS from a laptop — every request of
    start() and provision() awaited after the one before it. Measured on resources that already
    exist (a restart: the floor of every cold start), with one workflow name and with four, so a
    per-name sequential request shows up as the difference."""
    rtt = 60.0
    proxy = scope.proxy(nats_direct_url, 0)
    nc = await scope.conn(proxy.url)

    def flow(x: str) -> Any:
        async def body(ctx: Ctx, _input: Any) -> str:
            return str(await ctx.run("s", lambda: x))

        return define_workflow(f"pynet_start_{x}", body)

    flows = [flow(x) for x in "abcd"]

    async def once(k: int) -> float:
        w = create_worker(nc, flows[:k], prefix=prefix, **SIZED)
        t0 = time.perf_counter()
        await w.start()
        ms = (time.perf_counter() - t0) * 1000
        await w.stop()
        return ms

    await once(4)  # provisions, and binds all four names' consumers
    base = {k: min(await once(k), await once(k)) for k in (1, 4)}
    floor = await _rtt_ms(nc)
    proxy.set_rtt(rtt)
    await asyncio.sleep(rtt / 1000)
    added = await _rtt_ms(nc) - floor
    one = (min(await once(1), await once(1)) - base[1]) / added
    four = (min(await once(4), await once(4)) - base[4]) / added
    print(f"\ndependent round trips: worker start, 1 name {one:.2f}, 4 names {four:.2f}")

    # Names cost no round trip of their own: their consumers are read together.
    assert four - one < 1, (one, four)
    assert one < PINNED_ROUND_TRIPS_WORKER_START + 0.5, one


#: Derived, then measured: every resource's STREAM.INFO at once 1, the batched-direct-get
#: preflight 1, the three buckets opened together 1, every consumer's INFO at once 1. (nats-py's
#: manager makes no API check of its own, unlike the TypeScript client's — one fewer there.)
PINNED_ROUND_TRIPS_WORKER_START = 4.0


async def test_a_client_first_start_costs_a_pinned_number_of_dependent_round_trips(
    scope: Scope, nats_direct_url: str, prefix: str
) -> None:
    """A fresh client provisions, then starts a run. Its runs bucket used to be OPENED after the
    provision that had just verified it — one STREAM.INFO more (F25's follow-up). Measured on a
    prefix that exists."""
    rtt = 60.0
    proxy = scope.proxy(nats_direct_url, 0)
    nc = await scope.conn(proxy.url)
    # durable-sdk: provisioning is the engine's, at its start (engine.provision); the sizing that
    # was ProvisionOptions(stream_max_bytes=…) is the engine's --stream-max-bytes.
    await provision(nc, Names(prefix), EngineFlags(stream_max_bytes=16 * MB))

    async def body(ctx: Ctx, _input: Any) -> int:
        return int(await ctx.run("s", lambda: 1))

    wf = define_workflow("pynet_client_first", body)
    counter = iter(range(1_000))

    async def once() -> float:
        client = create_client(nc, prefix=prefix, **SIZED)  # fresh: nothing memoized
        t0 = time.perf_counter()
        await client.start(wf, {}, StartOptions(run_id=f"first-{next(counter)}"))
        return (time.perf_counter() - t0) * 1000

    await once()  # warm-up
    base = min(await once(), await once())
    floor = await _rtt_ms(nc)
    proxy.set_rtt(rtt)
    await asyncio.sleep(rtt / 1000)
    added = await _rtt_ms(nc) - floor
    first = (min(await once(), await once()) - base) / added
    print(f"\ndependent round trips: a client's first start() {first:.2f}")
    assert first < PINNED_ROUND_TRIPS_CLIENT_FIRST_START + 0.5, first


#: Derived, then measured: every resource's STREAM.INFO at once 1, the batched-direct-get
#: preflight 1; then the start itself — the journal start entry 1, the status hint 1, the work
#: item 1. (One fewer than TypeScript: nats-py's manager makes no API check.)
PINNED_ROUND_TRIPS_CLIENT_FIRST_START = 5.0


# durable-sdk: "an offloaded payload costs one dependent round trip per chunk" is dropped. It timed
# the library's blob writer (NatsBlobStore) through the proxy; the blob writes are the engine's now,
# next to the server, and the SDK sends every value inline in one request (docs/findings.md F1).


async def _identity(i: int) -> int:
    return i


async def _lease_under_latency(
    scope: Scope, server_url: str, prefix: str, max_work_ms: int | None
) -> dict[str, Any]:
    """The lease is a logical deadline the holder re-writes every TTL/3 and a contender reads with
    a plain get. At 0.1 ms neither the renewal's flight time nor the staleness of the contender's
    read exists; at a WAN round trip both eat into the TTL. A live holder that is taken over is a
    step running twice at once, so: RTT 500 ms against a 2 s TTL, a step three TTLs long, and a
    second worker asking for the run the whole time."""
    rtt, ttl, step_s = 500.0, 2_000, 5.5
    proxy = scope.proxy(server_url, 0)
    direct = await scope.conn(server_url)
    rec = Recorder()
    executions = 0

    async def slow() -> str:
        nonlocal executions
        executions += 1
        await asyncio.sleep(step_s)
        return "done"

    async def body(ctx: Ctx, _input: Any) -> Any:
        return await ctx.run("slow", slow)

    wf = define_workflow("pynet_lease", body)
    for who in ("A", "B"):
        w = create_worker(
            await scope.conn(proxy.url),
            [wf],
            prefix=prefix,
            lease_ttl_ms=ttl,
            max_work_ms=max_work_ms,
            metrics=rec.sink(who),
            **SIZED,
        )
        scope.worker(w)
        await w.start()  # provisioning at no added latency; the link slows once it is serving
    client = create_client(direct, prefix=prefix, **SIZED)
    proxy.set_rtt(rtt)
    await asyncio.sleep(rtt / 1000)
    handle = await client.start(wf, None, StartOptions(run_id=f"{prefix}-run"))

    # Nudges: extra `start` items for the SAME run. They journal nothing (an inbound event would
    # fence the step's own append and re-run it for a reason that has nothing to do with the
    # lease) — each one just sends whichever worker receives it to the gate.
    async def nudge() -> None:
        js = direct.jetstream()
        for i in range(9):
            await asyncio.sleep(0.45)
            await js.publish(
                Names(prefix).work_subject(wf.name),
                encode_json({"runId": handle.run_id, "kind": "start"}),
                headers={"Nats-Msg-Id": f"nudge-{i}"},
            )

    nudging = asyncio.ensure_future(nudge())
    result = await handle.result(60_000)
    await nudging
    proxy.set_rtt(0)  # the nudges still queued re-drive the terminal: no need for 500 ms there
    gates = rec.named("lease.gate")
    acquired = next((g for g in gates if g["outcome"] == "acquired"), None)
    return {
        "result": result,
        "executions": lambda: executions,
        "gates": gates,
        "acquired": acquired,
        "ttl_s": ttl / 1000,
    }


# durable-sdk: dropped — "LEASE at RTT 500 ms / TTL 2 s: a live holder is never taken over". Every
# assertion read the worker's `lease.gate` events; the gating run lease is the engine's, held next
# to the server (no RTT between them), and a run's items wait in the engine while one is held, so
# the second worker is never even handed the run: no SDK event exists to observe. Kept, skipped,
# for the diff (as the TypeScript twin).
@pytest.mark.skip(
    reason="durable-sdk: dropped — the gating run lease and its lease.gate events are the engine's"
)
async def test_lease_at_rtt_500_ttl_2s_a_live_holder_is_never_taken_over(
    scope: Scope, nats_direct_url: str, prefix: str
) -> None:
    r = await _lease_under_latency(scope, nats_direct_url, prefix, None)
    assert r["result"] == "done"
    acquired = r["acquired"]
    assert acquired is not None
    # Emergence: the OTHER worker asked for the run after the original deadline had passed and was
    # still refused — without that, nothing here ever tested a renewal.
    late_refusals = [
        g
        for g in r["gates"]
        if g["outcome"] == "held"
        and g["who"] != acquired["who"]
        and g["at"] > acquired["at"] + r["ttl_s"] + 0.25
    ]
    assert late_refusals, [(g["who"], g["outcome"]) for g in r["gates"]]
    assert [g for g in r["gates"] if g["outcome"] == "takeover"] == []
    assert r["executions"]() == 1


# durable-sdk: docs/findings.md F9, answered by the engine's F18 (past max_work_ms the lease is
# lapsed, not gone). The takeover is the engine's to see now: its log, `run taken over past max work`
# (or `run lease taken over`, across instances), stands for the `lease.gate` event.
async def test_control_the_same_run_with_renewal_cut_short_is_taken_over(
    scope: Scope, nats_direct_url: str, prefix: str
) -> None:
    """``max_work_ms`` is the public knob that stops a holder's renewal (the hung-step escape
    hatch): at 300 ms the lease is written once and never again, so its 2 s deadline passes."""
    r = await _lease_under_latency(scope, nats_direct_url, prefix, 300)
    assert r["result"] == "done"
    assert any("taken over" in line for line in await engine_log(prefix))
    assert r["executions"]() >= 2


# --- disconnects -------------------------------------------------------------------------------


class ThreeSteps:
    """s1 → a slow s2 that reports when it starts → s3; every effect is counted."""

    def __init__(self, name: str, slow_s: float) -> None:
        self.ran = {"s1": 0, "s2": 0, "s3": 0}
        self.in_s2 = asyncio.Event()
        outer = self

        async def s1() -> None:
            outer.ran["s1"] += 1

        async def s2() -> None:
            outer.ran["s2"] += 1
            outer.in_s2.set()
            await asyncio.sleep(slow_s)

        async def s3() -> None:
            outer.ran["s3"] += 1

        async def body(ctx: Ctx, _input: Any) -> str:
            await ctx.run("s1", s1)
            await ctx.run("s2", s2)
            await ctx.run("s3", s3)
            return "ok"

        self.wf = define_workflow(name, body)


async def test_a_connection_reset_mid_step_completes_after_the_reconnect_exactly_once(
    scope: Scope, nats_direct_url: str, prefix: str
) -> None:
    proxy = scope.proxy(nats_direct_url, 20)
    direct = await scope.conn(nats_direct_url)
    link = Link()
    wnc = await scope.conn(proxy.url, link=link)
    t = ThreeSteps("pynet_cut", 1.5)
    worker = scope.worker(create_worker(wnc, [t.wf], prefix=prefix, **SIZED))
    await worker.start()
    client = create_client(direct, prefix=prefix, **SIZED)
    handle = await client.start(t.wf, None)
    await asyncio.wait_for(t.in_s2.wait(), 10)
    proxy.set_down(True)  # an outage, not a blip: the first reconnect attempts fail too
    assert proxy.cut() == 1
    await asyncio.sleep(0.6)
    proxy.set_down(False)
    assert await handle.result(20_000) == "ok"
    assert (link.disconnects, link.reconnects) == (1, 1)  # it happened, and once
    assert t.ran == {"s1": 1, "s2": 1, "s3": 1}
    await _assert_journal_sane(direct, prefix, handle.run_id)


async def test_control_with_reconnect_off_the_same_cut_strands_the_run(
    scope: Scope, nats_direct_url: str, prefix: str
) -> None:
    proxy = scope.proxy(nats_direct_url, 20)
    direct = await scope.conn(nats_direct_url)
    wnc = await scope.conn(proxy.url, reconnect=False)
    t = ThreeSteps("pynet_cut", 1.0)
    opts: dict[str, Any] = {"prefix": prefix, "ack_wait_ms": 2_000, "lease_ttl_ms": 2_000, **SIZED}
    worker = scope.worker(create_worker(wnc, [t.wf], **opts))
    await worker.start()
    client = create_client(direct, prefix=prefix, **SIZED)
    handle = await client.start(t.wf, None)
    await asyncio.wait_for(t.in_s2.wait(), 10)
    proxy.cut()
    await asyncio.sleep(
        1.8
    )  # s2 has long finished on the dead worker; its step-end has nowhere to go
    assert wnc.is_closed
    stranded = [e["kind"] for e in await _journal(direct, prefix, handle.run_id)]
    assert stranded == ["start", "step-begin", "step-end", "step-begin"]
    # What the reconnect bought above is visible in what recovery costs without it: the item
    # redelivers to a fresh worker after AckWait, and s2 — begun, never ended — runs AGAIN.
    rescue = scope.worker(create_worker(await scope.conn(nats_direct_url), [t.wf], **opts))
    await rescue.start()
    assert await handle.result(30_000) == "ok"
    assert t.ran == {"s1": 1, "s2": 2, "s3": 1}
    await _assert_journal_sane(direct, prefix, handle.run_id)


async def test_a_lost_ack_the_append_landed_its_reply_did_not(
    scope: Scope, nats_direct_url: str, prefix: str
) -> None:
    """The ambiguous failure: the worker cannot know whether its step-end was written. It must not
    assume either way — the redelivery replays from the journal, which is where the answer is."""
    proxy = scope.proxy(nats_direct_url, 0)
    direct = await scope.conn(nats_direct_url)
    rec = Recorder()
    s2_runs = 0

    async def s1() -> int:
        return 1

    async def s2() -> int:
        nonlocal s2_runs
        s2_runs += 1
        if s2_runs == 1:
            proxy.set_stall("down")  # from here on requests land and replies do not
        return 2

    async def s3() -> str:
        return "ok"

    async def body(ctx: Ctx, _input: Any) -> str:
        await ctx.run("s1", s1)
        await ctx.run("s2", s2)
        return str(await ctx.run("s3", s3))

    wf = define_workflow("pynet_ack", body)
    worker = scope.worker(
        create_worker(
            await scope.conn(proxy.url), [wf], prefix=prefix, metrics=rec.sink("W"), **SIZED
        )
    )
    await worker.start()
    client = create_client(direct, prefix=prefix, **SIZED)
    handle = await client.start(wf, None)

    async def landed() -> bool:
        entries = await _journal(direct, prefix, handle.run_id)
        return any(e["kind"] == "step-end" and e["seq"] == 1 for e in entries)

    await _wait_until(landed, 10, "s2's step-end to land while its ack is withheld")
    proxy.cut()  # the withheld ack dies with the connection
    proxy.set_stall("none")
    # durable-sdk: the 30 s bound is the worker's own recovery: its append times out at the request margin
    # (10 s, a timer load does not stretch), its `done retry` goes back, the redelivery replays
    # and finishes (≈10.3 s from the append, measured quiet and beside the engine's race suite).
    # This test's engine has the default ack wait and lease TTL, 30 s, so a run that reaches the
    # bound fell to the engine's lapse instead: the `done` did not land. Its failure says which
    # (docs/findings.md F45).
    t0 = time.monotonic()
    try:
        assert await handle.result(30_000) == "ok"
    except DurableTimeoutError as e:
        try:
            warns = [line for line in await engine_log(prefix) if "level=WARN" in line or "level=ERROR" in line]  # fmt: skip
        except pytest.skip.Exception:  # DE_ENGINE_URL: the engine's log is its operator's
            warns = []
        raise AssertionError(
            f"the run did not finish within 30 s of the cut ({time.monotonic() - t0:.1f} s): "
            f"work.done {rec.tally('work.done', 'outcome')}, journal.append "
            f"{[(a.get('kind'), a.get('seq'), a.get('outcome'), a.get('ms')) for a in rec.named('journal.append')]}, "
            f"the engine's warnings {warns[-10:]}"
        ) from e
    # Emergence: the worker really did see that append FAIL, and really did retry the item.
    lost = [
        e["outcome"]
        for e in rec.named("journal.append")
        if e.get("kind") == "step-end" and e.get("seq") == 1
    ]
    assert lost == ["error"], [
        (e.get("kind"), e.get("seq"), e.get("outcome"), e.get("ms"))
        for e in rec.named("journal.append")
    ]
    assert "retry" in rec.tally("work.done", "outcome"), rec.tally("work.done", "outcome")
    assert s2_runs == 1
    await _assert_journal_sane(direct, prefix, handle.run_id)


async def test_a_result_waiter_rides_out_a_cut_even_though_its_wake_up_call_was_lost(
    scope: Scope, nats_direct_url: str, prefix: str
) -> None:
    proxy = scope.proxy(nats_direct_url, 20)
    direct = await scope.conn(nats_direct_url)
    link = Link()
    cnc = await scope.conn(proxy.url, link=link)

    async def body(ctx: Ctx, _input: Any) -> str:
        await ctx.wait("go")
        return "woken"

    wf = define_workflow("pynet_res", body)
    worker = scope.worker(create_worker(direct, [wf], prefix=prefix, **SIZED))
    await worker.start()
    client = create_client(cnc, prefix=prefix, **SIZED)
    handle = await client.start(wf, None)
    # The waiter's own view of its wake-up subject, on the same connection.
    hints = 0

    async def on_hint(_msg: Any) -> None:
        nonlocal hints
        hints += 1

    await cnc.subscribe(Names(prefix).runs_hint_subject(handle.run_id), cb=on_hint)
    await cnc.flush()
    waiting = asyncio.ensure_future(handle.result(20_000))
    await asyncio.sleep(0.3)
    hints_before = hints
    proxy.set_down(True)
    proxy.cut()
    # The run finishes while the waiter is cut off: the status hint is published to nobody.
    await create_client(direct, prefix=prefix, **SIZED).signal(handle.run_id, "go", {})

    async def finished() -> bool:
        return any(e["kind"] == "result" for e in await _journal(direct, prefix, handle.run_id))

    await _wait_until(finished, 10, "the run to finish during the outage")
    proxy.set_down(False)
    assert await waiting == "woken"
    assert link.reconnects == 1
    assert hints == hints_before  # no hint ever reached this connection: the TICK found it


async def test_a_result_waiter_on_a_connection_that_closes_fails_at_once_by_name(
    scope: Scope, nats_direct_url: str, prefix: str
) -> None:
    """Before the fix the wait loop hot-looped on the dead connection for the whole timeout — a
    failed hint cost nothing, a failed journal read cost nothing — and then reported a timeout the
    run had nothing to do with."""
    proxy = scope.proxy(nats_direct_url, 0)
    cnc = await scope.conn(proxy.url, reconnect=False)

    async def body(ctx: Ctx, _input: Any) -> int:
        await ctx.wait("never")
        return 1

    wf = define_workflow("pynet_res_closed", body)
    client = create_client(cnc, prefix=prefix, **SIZED)
    handle = await client.start(wf, None)
    t0 = time.monotonic()
    waiting = asyncio.ensure_future(handle.result(8_000))
    await asyncio.sleep(0.3)
    proxy.cut()
    with pytest.raises(ConnectionClosedError):
        await waiting
    assert time.monotonic() - t0 < 3.0  # at once (within one tick), not at the timeout


class _CapturingLogger:
    def __init__(self) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def debug(self, message: str, **_f: Any) -> None: ...
    def info(self, message: str, **_f: Any) -> None: ...

    def warn(self, message: str, **_f: Any) -> None:
        self.warnings.append(message)

    def error(self, message: str, **_f: Any) -> None:
        self.errors.append(message)


async def test_a_worker_whose_connection_closes_for_good_says_so_and_stops_pretending(
    scope: Scope, nats_direct_url: str, prefix: str
) -> None:
    """The library does not own the connection and cannot reopen it. Before this, such a worker
    reported ``running=True`` forever and logged "consumer pump failed; rebuilding" every second —
    which it never could — so a readiness probe kept a permanently deaf pod in service."""
    proxy = scope.proxy(nats_direct_url, 0)
    wnc = await scope.conn(proxy.url, reconnect=False)

    async def body(_ctx: Ctx, _input: Any) -> int:
        return 1

    log = _CapturingLogger()
    worker = scope.worker(
        create_worker(
            wnc, [define_workflow("pynet_deaf", body)], prefix=prefix, logger=log, **SIZED
        )
    )
    await worker.start()

    async def serving() -> bool:
        h = worker.health()
        return h.running and not h.connection_closed and all(p.alive for p in h.pumps)

    await _wait_until(serving, 10, "both pumps to be attached")
    proxy.cut()

    async def reported() -> bool:
        return worker.health().connection_closed and len(log.errors) >= 1

    await _wait_until(reported, 10, "the worker to notice its connection closed")
    await asyncio.sleep(2.5)  # long enough for the old loop to have "rebuilt" twice more
    after = worker.health()
    assert after.running is False
    # durable-sdk: assertion touched, `== [False, False]` → every entry False. The worker's
    # `pumps` are its poll slots (16 by default), not a work and a timers consumer per name.
    assert all(not p.alive for p in after.pumps)
    assert [e for e in log.errors if "CLOSED for good" in e] == [log.errors[0]]  # once, by name
    assert len(log.errors) == 1
    assert not [w for w in log.warnings if "rebuilding" in w]  # and it stopped pretending


# --- a broker crash mid-storm ------------------------------------------------------------------


@existing.skip_in_mode(existing.OWN_SERVER)
async def test_sigkill_and_restart_under_a_two_worker_signal_storm(
    scope: Scope, prefix: str
) -> None:
    server = spawn_server(limits_for(profile_from_env()))

    async def stop_server() -> None:
        server.stop()

    scope.defer(stop_server)
    set_engine_server(server.url)  # durable-sdk: this test's engine serves this test's own server
    runs = 6
    ran: dict[str, int] = {}

    async def body(ctx: Ctx, run_key: str) -> str:
        await ctx.wait("go")

        def bump(step: str) -> Callable[[], Awaitable[str]]:
            async def effect() -> str:
                k = f"{run_key}:{step}"
                ran[k] = ran.get(k, 0) + 1
                await asyncio.sleep(0.3)  # long enough that the crash can land INSIDE a step
                return step

            return effect

        await ctx.run("s1", bump("s1"))
        await ctx.sleep("nap", 1_000)  # a native schedule that must survive the restart
        await ctx.run("s2", bump("s2"))
        return str(await ctx.run("s3", bump("s3")))

    wf = define_workflow("pynet_storm", body)
    rec = Recorder()
    links = []
    for who in ("A", "B"):
        link = Link()
        links.append(link)
        w = create_worker(
            await scope.conn(server.url, link=link),
            [wf],
            prefix=prefix,
            metrics=rec.sink(who),
            **SIZED,
        )
        scope.worker(w)
        await w.start()
    cnc = await scope.conn(server.url)
    client = create_client(cnc, prefix=prefix, **SIZED)
    handles = [
        await client.start(wf, f"r{i}", StartOptions(run_id=f"{prefix}-boom-{i}"))
        for i in range(runs)
    ]
    results = [asyncio.ensure_future(h.result(90_000)) for h in handles]
    await asyncio.gather(*(client.signal(h.run_id, "go", {}) for h in handles for _ in range(3)))

    # Crash at a moment worth crashing at: some run is INSIDE a step (begin journaled, end not).
    observer = await scope.conn(server.url)
    open_at_crash = 0

    async def some_step_open() -> bool:
        nonlocal open_at_crash
        open_now = 0
        for h in handles:
            entries = await _journal(observer, prefix, h.run_id)
            open_now += sum(e["kind"] == "step-begin" for e in entries)
            open_now -= sum(e["kind"] == "step-end" for e in entries)
        open_at_crash = open_now
        return open_now > 0

    await _wait_until(some_step_open, 10, "a run mid-step")
    done_before = 0
    for h in handles:
        done_before += any(
            e["kind"] == "result" for e in await _journal(observer, prefix, h.run_id)
        )
    await asyncio.to_thread(server.restart, kill=True, down_s=0.5)

    assert await asyncio.gather(*results) == ["s3"] * runs
    repeats = sum(1 for v in ran.values() if v > 1)
    print(
        f"\nbroker crash: {open_at_crash} step(s) open at the kill · work.done "
        f"{rec.tally('work.done', 'outcome')} · appends {rec.tally('journal.append', 'outcome')} "
        f"· pump restarts {len(rec.named('pump.restart'))} · effects repeated {repeats}"
    )
    # Emergence: the crash interrupted work in progress, and both workers lived through it.
    assert open_at_crash > 0
    assert done_before == 0
    for link in links:
        assert link.reconnects >= 1
    # State is exactly-once whatever the crash interleaved; an EFFECT may repeat across a crash
    # (begin journaled, end not — the documented at-least-once window), never go missing.
    for h in handles:
        await _assert_journal_sane(observer, prefix, h.run_id)
    for i in range(runs):
        for step in ("s1", "s2", "s3"):
            assert ran.get(f"r{i}:{step}", 0) >= 1
    assert await list_ephemeral_consumers(observer) == []
