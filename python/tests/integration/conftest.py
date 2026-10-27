"""Per-test hygiene on the shared server — the integration directory's autouse companion to the
session fixture in ``tests/conftest.py`` (mirrors ``test/harness/setup-each.ts``).

durable-sdk: the engine first. A session fixture resolves ``durable-engine`` once (``DE_ENGINE``;
``tests/engine.py``, the twin of ``test/harness/global-setup.ts``'s engine half) and every test
here skips without it, saying why.
Each test's engines are started on first use by ``engine.create_worker`` / ``create_client`` and
stopped here, before anything else (a graceful stop naks what they hold).

After EVERY integration test, two things:

1. The leftover check, attributed: any consumer without a durable name fails the test that made
   it, by name. The end-of-run check still exists; this one says WHO.
2. The owner of the test's streams: every stream on the server is deleted. The server runs under an
   account profile whose stream cap is 16 — a prefix is eight, so two — so a test's provisioning must be
   gone before the next test starts. Deleting everything rather than the prefixes a test declared
   means an undeclared prefix cannot accumulate either.

Autouse fixtures are set up first and torn down LAST, so this runs after the test's own ``nc`` is
closed and its workers are stopped. Scoped to this directory: the pure suites never spawn a server.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
from collections.abc import AsyncIterator, Iterator

import pytest

import engine
import existing
from conftest import delete_all_streams, leftover_message, list_ephemeral_consumers


@pytest.fixture(scope="session")
def engine_bin() -> Iterator[str]:
    """The engine binary for the run, resolved once; the integration suites skip without one. With
    it the run's licence (F42): the caller's ``DE_LICENCE``, or one made once for the run with
    ``DE_LICENCE_TOOL`` and removed at the end; they skip without either."""
    if existing.EXISTING is not None and existing.EXISTING.engine_url is not None:
        yield ""  # DE_ENGINE_URL: the engine is not the harness's
        return
    try:
        binary = engine.resolve_engine_binary()
        print(f"\nengine: {binary} (engine.pin {engine.engine_pin()})", file=sys.stderr)
        licence, minted = engine.run_licence()
    except engine.EngineNotAvailableError as e:
        pytest.skip(f"durable-engine unavailable ({e})")
    print(
        f"licence: made for the run, {licence}" if minted else f"licence: DE_LICENCE {licence}",
        file=sys.stderr,
    )
    engine.configure_licence(licence)
    try:
        yield binary
    finally:
        engine.configure_licence(None)
        if minted:
            shutil.rmtree(os.path.dirname(licence), ignore_errors=True)


@pytest.fixture(scope="session")
def run_engine(
    engine_bin: str,
    nats_direct_url: str,  # noqa: ARG001 — the server first
    nats_url: str,
) -> Iterator[None]:
    """The mode for an existing server (existing.py): the ONE engine of the run, started here with
    ``--nats <url> [--creds <file>] --prefix <prefix>`` (it provisions the eight, or fails loud with
    the engine's own message: an account tier too small is the operator's to fix), stopped at the
    end; under ``DE_ENGINE_URL`` an engine already serving the prefix, never started or stopped.
    At the end: the prefix's leftovers and the engine's /metrics, printed and kept in
    ``.de-run/<prefix>/``, and ``DE_CLEANUP=delete`` deletes the eight of the prefix alone."""
    ex = existing.EXISTING
    if ex is None:
        yield
        return
    # The engine runs where the harness runs, so its round trip to the server is the tests': it
    # connects where they do (under DE_TEST_RTT_MS, the delay proxy).
    run = engine.RunEngine(
        engine_bin or None,
        nats_url,
        ex.prefix,
        creds=ex.creds,
        metrics_addr=None if ex.engine_url else f"127.0.0.1:{_free_port()}",
        start_timeout_s=existing.scaled(30),
    )
    if run.external:
        import urllib.request

        with urllib.request.urlopen(f"{ex.engine_url}/healthz", timeout=10) as r:
            if r.status != 200:
                raise RuntimeError(f"DE_ENGINE_URL {ex.engine_url}: /healthz answered {r.status}")
        print(
            f"engine: DE_ENGINE_URL {ex.engine_url} (not started by the harness)", file=sys.stderr
        )
    else:
        asyncio.run(run.ensure(engine.EngineFlags()))
        existing.say(
            f"engine: {engine_bin} (engine.pin {engine.engine_pin()}), one for the run, metrics on "
            f"http://{run.metrics_addr}"
        )
    engine.configure_run(run)
    asyncio.run(_probe(ex, nats_url))
    existing.AT_START = frozenset(asyncio.run(_snapshot(ex)).consumers)
    try:
        yield
    finally:
        metrics = _engine_metrics(ex, run)
        try:
            asyncio.run(run.stop())
            engine.configure_run(None)
            # The engine stopped, nothing writes a blob: what a test that failed or timed out left
            # after its own cleanup goes now (F33). The pair stays: it is the
            # engine's, and goes with the prefix's streams (existing.purge_since).
            blobs = asyncio.run(_sweep(ex))
            # Under DE_ENGINE_URL the engine still runs: a late-stopped worker's step can put a
            # blob after the sweep. Again after a pause, until a round finds nothing (F38).
            for _ in range(3 if run.external else 0):
                import time

                time.sleep(existing.scaled(2.0))
                more_blobs = asyncio.run(_sweep(ex))
                blobs += more_blobs
                if more_blobs == 0:
                    break
            existing.say(
                f"the run's end: {blobs} blobs of its {len(existing.RUN_IDS)} runs deleted"
            )
            asyncio.run(_report(ex, run, metrics))
        finally:
            if ex.delete_at_end:
                gone = asyncio.run(_delete(ex))
                how = os.environ.get("DE_CLEANUP") or "delete (the default for a generated prefix)"
                existing.say(f"DE_CLEANUP={how}: deleted {', '.join(gone) or 'nothing'}")


async def _probe(ex: existing.ExistingServer, url: str) -> None:
    """The throughput the large values meet, through the engine on the tests' link (the proxy's
    under DE_TEST_RTT_MS), once: the large-value tests' waits carry its term (existing.moving)."""
    import secrets

    run_id = f"probe-{secrets.token_hex(4)}"
    existing.RUN_IDS.add(run_id)
    nc = await existing.connect(url, name="test-harness-probe")
    try:
        bps, put_s, get_s = await existing.measure_throughput(nc, ex.prefix, run_id)
        existing.MAX_PAYLOAD = int(nc.max_payload)
    finally:
        await nc.close()
    existing.THROUGHPUT = bps
    admin = await existing.connect_admin()
    try:
        await existing.delete_run_blobs(admin, ex.prefix, {run_id})
    finally:
        await admin.close()
    mib = 1 << 20
    existing.say(
        f"throughput {bps / mib:.2f} MiB/s (a 4.00 MiB put {put_s * 1000:.0f} ms and get "
        f"{get_s * 1000:.0f} ms through the engine), max_payload {existing.MAX_PAYLOAD / mib:.2f} "
        f"MiB -> a large value's wait adds its bytes over the throughput x {existing.HEADROOM}"
    )


def _free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def _snapshot(ex: existing.ExistingServer) -> existing.Snapshot:
    nc = await existing.connect_admin()
    try:
        return await existing.snapshot(nc, ex.prefix)
    finally:
        await nc.close()


async def _sweep(ex: existing.ExistingServer) -> int:
    nc = await existing.connect_admin()
    try:
        return await existing.sweep_run(nc, ex.prefix)
    finally:
        await nc.close()


def _engine_metrics(ex: existing.ExistingServer, run: engine.RunEngine) -> bytes | Exception | None:
    """The engine's /metrics (or why not), read while it still runs."""
    import urllib.request

    base = ex.engine_url or (f"http://{run.metrics_addr}" if run.metrics_addr else None)
    if base is None:
        return None
    try:
        with urllib.request.urlopen(f"{base}/metrics", timeout=10) as r:
            return bytes(r.read())
    except Exception as e:  # noqa: BLE001 — a report line, never the run's failure
        return e


async def _delete(ex: existing.ExistingServer) -> list[str]:
    nc = await existing.connect_admin()
    try:
        return await existing.delete_prefix_streams(nc, ex.prefix)
    finally:
        await nc.close()


async def _report(
    ex: existing.ExistingServer, run: engine.RunEngine, metrics: bytes | Exception | None
) -> None:
    """What the run leaves for the reader: the prefix's leftovers and the engine's /metrics."""
    from pathlib import Path

    nc = await existing.connect_admin()
    lines: list[str] = []
    try:
        jsm = nc.jsm()
        for s in existing.prefix_streams(ex.prefix):
            try:
                si = await jsm.stream_info(s)
                cs = [
                    c.name
                    for c in await jsm.consumers_info(s)
                    if existing.durable_of(ex.prefix, c.name)
                ]
                lines.append(
                    f"{s}: {si.state.messages} messages, {si.state.bytes} bytes, "
                    f"consumers [{', '.join(cs)}]"
                )
            except Exception:  # noqa: BLE001 — a report line, never the run's failure
                lines.append(f"{s}: absent")
    finally:
        await nc.close()
    out = Path(__file__).resolve().parents[3] / ".de-run" / f"{ex.prefix}-py"
    out.mkdir(parents=True, exist_ok=True)
    (out / "leftovers.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    if isinstance(metrics, bytes):
        (out / "metrics.txt").write_bytes(metrics)
    elif metrics is not None:
        lines.append(f"/metrics: {metrics}")
    if not run.external:
        (out / "engine.log").write_text("\n".join(run.log_since(0)) + "\n", encoding="utf-8")
    body = "\n  ".join(lines)
    existing.say(
        f"prefix {ex.prefix} after the run (also in {out}: leftovers.txt, metrics.txt, "
        f"engine.log; the engine started {run.starts}x):\n  {body}"
    )


@pytest.fixture(autouse=True)
async def _server_hygiene(
    engine_bin: str,
    nats_direct_url: str,
    run_engine: None,  # noqa: ARG001 — the run's engine first, in the mode
    request: pytest.FixtureRequest,
) -> AsyncIterator[None]:
    engine.configure(engine_bin, nats_direct_url)
    if existing.EXISTING is not None:
        async for _ in _existing_hygiene(existing.EXISTING.prefix, nats_direct_url, request):
            yield
        return
    yield
    # The engines of the test first (a graceful stop naks what they hold), then the streams.
    await engine.stop_engines()
    engine.set_engine_server(None)
    import nats

    # The server itself, never the delay proxy (``DE_TEST_RTT_MS``): hygiene is the harness's own
    # traffic, and listing every consumer of every stream at WAN latency would dominate the run.
    nc = await nats.connect(nats_direct_url, name="test-harness-hygiene")
    try:
        try:
            left = await list_ephemeral_consumers(nc)
            if left:
                # A consumer mid-teardown (a test's own helper unwinding) clears in well under
                # this; a leak does not.
                await asyncio.sleep(0.5)
                left = await list_ephemeral_consumers(nc)
            if left:
                raise AssertionError(leftover_message(left, f'after "{request.node.name}"'))
        finally:
            await delete_all_streams(nc)
    finally:
        await nc.drain()


async def _existing_hygiene(
    prefix: str, url: str, request: pytest.FixtureRequest
) -> AsyncIterator[None]:
    """Against an existing server (existing.py): the run's one engine stays up, the leftovers check
    looks at the prefix's streams only, and in place of deleting every stream the test's writes
    under the prefix are purged (what changed since the snapshot taken before it)."""
    # The tests' identity for the engine's API (info, a cancelPoll); the admin connection for the
    # streams (DE_ADMIN_URL, the tests' own by default; as a tenant the engine's account).
    nc = await existing.connect(url, name="test-harness-hygiene")
    adm = await existing.connect_admin(name="test-harness-admin")
    try:
        before = await existing.snapshot(adm, prefix)
        await engine.begin_test(nc)
        yield
        engine.set_engine_server(None)
        # A test that failed or timed out is cleaned like any other (F33): its
        # workers are stopped (bounded) and its polls withdrawn first, so neither a worker it left
        # running nor a fetch the engine still holds re-creates what the cleanup deletes.
        try:
            await engine.end_test_workers(nc)
            left = await existing.prefix_ephemeral_consumers(adm, prefix, before.consumers)
            if left:
                await asyncio.sleep(existing.scaled(0.5))
                left = await existing.prefix_ephemeral_consumers(adm, prefix, before.consumers)
            if left:
                raise AssertionError(leftover_message(left, f'after "{request.node.name}"'))
        finally:
            run_ids = await existing.run_ids_since(adm, prefix, before)
            existing.RUN_IDS.update(run_ids)
            await existing.purge_since(adm, prefix, before)
            await existing.delete_run_blobs(adm, prefix, existing.RUN_IDS)
    finally:
        await adm.drain()
        await nc.drain()
