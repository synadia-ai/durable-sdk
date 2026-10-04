"""Per-test hygiene on the shared server — the integration directory's autouse companion to the
session fixture in ``tests/conftest.py`` (mirrors ``test/harness/setup-each.ts``).

durable-sdk: the engine first. A session fixture resolves ``durable-engine`` once (``DE_ENGINE``, or
a build of ``../durable-engine`` at ``engine.pin``; ``tests/engine.py``, the twin of
``test/harness/global-setup.ts``'s engine half) and every test here skips without it, saying why.
Each test's engines are started on first use by ``engine.create_worker`` / ``create_client`` and
stopped here, before anything else (a graceful stop naks what they hold).

After EVERY integration test, two things:

1. The leftover check, attributed: any consumer without a durable name fails the test that made
   it, by name. The end-of-run check still exists; this one says WHO.
2. The owner of the test's streams: every stream on the server is deleted. The server runs under an
   account profile whose stream cap is 16 — a prefix is seven — so a test's provisioning must be
   gone before the next test starts. Deleting everything rather than the prefixes a test declared
   means an undeclared prefix cannot accumulate either.

Autouse fixtures are set up first and torn down LAST, so this runs after the test's own ``nc`` is
closed and its workers are stopped. Scoped to this directory: the pure suites never spawn a server.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncIterator, Iterator

import pytest

import engine
import existing
from conftest import delete_all_streams, leftover_message, list_ephemeral_consumers


@pytest.fixture(scope="session")
def engine_bin() -> str:
    """The engine binary for the run, resolved once; the integration suites skip without one."""
    if existing.EXISTING is not None and existing.EXISTING.engine_url is not None:
        return ""  # DE_ENGINE_URL: the engine is not the harness's
    try:
        binary = engine.resolve_engine_binary()
    except engine.EngineNotAvailableError as e:
        pytest.skip(f"durable-engine unavailable ({e})")
    print(f"\nengine: {binary} (engine.pin {engine.engine_pin()})", file=sys.stderr)
    return binary


@pytest.fixture(scope="session")
def run_engine(
    engine_bin: str,
    nats_direct_url: str,  # noqa: ARG001 — the server first
    nats_url: str,
) -> Iterator[None]:
    """The mode for an existing server (existing.py): the ONE engine of the run, started here with
    ``--nats <url> [--creds <file>] --prefix <prefix>`` (it provisions the seven, or fails loud with
    the engine's own message: an account tier too small is the operator's to fix), stopped at the
    end; under ``DE_ENGINE_URL`` an engine already serving the prefix, never started or stopped.
    At the end: the prefix's leftovers and the engine's /metrics, printed and kept in
    ``.de-run/<prefix>/``, and ``DE_CLEANUP=delete`` deletes the seven of the prefix alone."""
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
    existing.AT_START = frozenset(asyncio.run(_snapshot(ex)).consumers)
    try:
        yield
    finally:
        try:
            asyncio.run(_report(ex, run))
        finally:
            asyncio.run(run.stop())
            engine.configure_run(None)
            if ex.delete_at_end:
                gone = asyncio.run(_delete(ex))
                existing.say(f"DE_CLEANUP=delete: deleted {', '.join(gone) or 'nothing'}")


def _free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def _snapshot(ex: existing.ExistingServer) -> existing.Snapshot:
    nc = await existing.connect(ex.url)
    try:
        return await existing.snapshot(nc, ex.prefix)
    finally:
        await nc.close()


async def _delete(ex: existing.ExistingServer) -> list[str]:
    nc = await existing.connect(ex.url)
    try:
        return await existing.delete_prefix_streams(nc, ex.prefix)
    finally:
        await nc.close()


async def _report(ex: existing.ExistingServer, run: engine.RunEngine) -> None:
    """What the run leaves for the reader: the prefix's leftovers and the engine's /metrics."""
    import urllib.request
    from pathlib import Path

    nc = await existing.connect(ex.url)
    lines: list[str] = []
    try:
        jsm = nc.jsm()
        for s in existing.prefix_streams(ex.prefix):
            try:
                si = await jsm.stream_info(s)
                cs = [c.name for c in await jsm.consumers_info(s)]
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
    base = ex.engine_url or (f"http://{run.metrics_addr}" if run.metrics_addr else None)
    if base is not None:
        try:
            with urllib.request.urlopen(f"{base}/metrics", timeout=10) as r:
                (out / "metrics.txt").write_bytes(r.read())
        except Exception as e:  # noqa: BLE001 — a report line, never the run's failure
            lines.append(f"/metrics: {e}")
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
    nc = await existing.connect(url, name="test-harness-hygiene")
    try:
        before = await existing.snapshot(nc, prefix)
        await engine.begin_test(nc)
        yield
        engine.set_engine_server(None)
        try:
            left = await existing.prefix_ephemeral_consumers(nc, prefix, before.consumers)
            if left:
                await asyncio.sleep(existing.scaled(0.5))
                left = await existing.prefix_ephemeral_consumers(nc, prefix, before.consumers)
            if left:
                raise AssertionError(leftover_message(left, f'after "{request.node.name}"'))
        finally:
            await existing.purge_since(nc, prefix, before)
    finally:
        await nc.drain()
