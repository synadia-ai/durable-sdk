"""durable-sdk: the engine's two limit refusals, end to end (F40; the engine's F46). The twin of
``test/integration/limits.test.ts``. A licence limit on a standalone engine of the test's own (a
test licence with ``-workflows 1 -runs-in-flight 1``), and a tenant limit on an engine in the fabric
mode on a three-account server of the test's own (the tenant's record ``maxWorkflows: 1`` in
``KV_DE_TENANTS``, as an operator sets it). Each is a :class:`LimitError` naming the limit:
``worker.start()`` raises it, ``client.start()`` raises it after one request. Both need a server
and an engine of their own, so neither runs against an existing server.
"""

from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import nats
import pytest

import existing
from conftest import render_tenancy_config, spawn_server
from engine import EngineFlags, EngineProcess, count_of, engine_counts, mint_test_licence
from synadia_durable import LimitError, create_client, create_worker, define_workflow
from synadia_durable.types import StartOptions

pytestmark = pytest.mark.asyncio

OWN = "needs a server and an engine of its own, with a licence or a tenant's limits of its own"
MIB = 1024 * 1024


class Scope:
    """What a test opened, closed in reverse order whatever happened in it."""

    def __init__(self) -> None:
        self._closers: list[Callable[[], Awaitable[Any]]] = []

    def defer(self, fn: Callable[[], Awaitable[Any]]) -> None:
        self._closers.append(fn)

    async def conn(self, url: str, **opts: Any) -> Any:
        nc = await nats.connect(url, **opts)
        self.defer(nc.close)
        return nc

    async def close(self) -> None:
        for fn in reversed(self._closers):
            with contextlib.suppress(Exception):  # a cleanup never hides the test's outcome
                await fn()
        self._closers.clear()


@pytest.fixture
async def scope() -> AsyncIterator[Scope]:
    s = Scope()
    try:
        yield s
    finally:
        await s.close()


async def _parked(ctx: Any, _input: Any) -> Any:
    return await ctx.wait("go")


async def _other(_ctx: Any, _input: Any) -> str:
    return "never"


PARKED = define_workflow("lim_parked", _parked)
OTHER = define_workflow("lim_other", _other)


@existing.skip_in_mode(OWN)
async def test_a_licence_limit_refuses_a_workflow_at_start_and_a_run_past_its_runs_in_flight(
    scope: Scope, engine_bin: str, nats_direct_url: str, nats_url: str
) -> None:
    prefix = "delimpy"
    licence = mint_test_licence(engine_bin, "durable-sdk-limits", workflows=1, runs_in_flight=1)
    eng = EngineProcess(
        engine_bin, nats_direct_url, prefix, EngineFlags(stream_max_bytes=16 * MIB), licence=licence
    )
    await eng.start()
    scope.defer(eng.stop)
    assert any("licensee=durable-sdk-limits" in line and "max_workflows=1" in line for line in eng.log)  # fmt: skip
    nc = await scope.conn(nats_url)

    # The licence's one workflow name binds; a second is refused, loud, and start() says so.
    first = create_worker(nc, [PARKED], prefix=prefix, concurrency=1)
    await first.start()
    scope.defer(first.stop)
    second = create_worker(nc, [OTHER], prefix=prefix, concurrency=1)
    with pytest.raises(LimitError) as caught:
        await second.start()
    e = caught.value
    assert (e.code, e.limit, e.op) == ("licence_limit", "workflows", "poll")
    assert "licence's limit of 1" in str(e)
    assert not second.health().running

    # One run in flight is the licence's; a second start is refused once, never retried.
    client = create_client(nc, prefix=prefix)
    await client.start(PARKED, None, StartOptions(run_id="lim-1"))
    refused = "de_engine_refused_total"
    before = count_of(await engine_counts(nc, prefix), refused, code="licence_limit")
    with pytest.raises(LimitError) as caught:
        await client.start(PARKED, None, StartOptions(run_id="lim-2"))
    r = caught.value
    assert (r.code, r.limit, r.op) == ("licence_limit", "runs", "start")
    after = count_of(await engine_counts(nc, prefix), refused, code="licence_limit")
    assert after - before == 1

    # The run in flight still finishes: a limit refuses what is new, never what runs.
    await client.signal("lim-1", "go", 7)
    assert await client.get("lim-1").result(20_000) == 7


@existing.skip_in_mode(OWN)
async def test_a_tenant_limit_refuses_a_workflow_at_start(scope: Scope, engine_bin: str) -> None:
    prefix = "de"
    server = spawn_server(
        None, config=lambda port, store: render_tenancy_config(port, store, prefix, ["A", "B"])
    )

    async def stop_server() -> None:
        server.stop()

    scope.defer(stop_server)
    eng = EngineProcess(
        engine_bin,
        f"nats://de:de@127.0.0.1:{server.port}",
        prefix,
        EngineFlags(),
        licence=None,  # the fabric mode needs none, and one would bound its tenants
        extra_args=["--tenancy", "--tenant-max-bytes", "0"],
    )
    await eng.start()
    scope.defer(eng.stop)

    # The tenant's record, before its first request (the engine reads it then, and every 10 s).
    admin = await scope.conn(server.url, user="de", password="de")
    kv = await admin.jetstream().key_value(f"{prefix.upper()}_TENANTS")
    await kv.put("A", json.dumps({"maxWorkflows": 1}).encode())

    nc = await scope.conn(server.url, user="a-client", password="a-client")
    first = create_worker(nc, [PARKED], concurrency=1)
    await first.start()
    scope.defer(first.stop)
    second = create_worker(nc, [OTHER], concurrency=1)
    with pytest.raises(LimitError) as caught:
        await second.start()
    e = caught.value
    assert (e.code, e.limit, e.op) == ("tenant_limit", "workflows", "poll")
    assert "at its limit of 1" in str(e)
    assert not second.health().running

    # Tenant B is not affected: its own two names bind.
    bnc = await scope.conn(server.url, user="b-client", password="b-client")
    for wf in (PARKED, OTHER):
        w = create_worker(bnc, [wf], concurrency=1)
        await w.start()
        scope.defer(w.stop)
