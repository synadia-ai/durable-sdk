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
from collections.abc import AsyncIterator

import pytest

import engine
from conftest import delete_all_streams, leftover_message, list_ephemeral_consumers


@pytest.fixture(scope="session")
def engine_bin() -> str:
    """The engine binary for the run, resolved once; the integration suites skip without one."""
    try:
        binary = engine.resolve_engine_binary()
    except engine.EngineNotAvailableError as e:
        pytest.skip(f"durable-engine unavailable ({e})")
    print(f"\nengine: {binary} (engine.pin {engine.engine_pin()})", file=sys.stderr)
    return binary


@pytest.fixture(autouse=True)
async def _server_hygiene(
    engine_bin: str, nats_direct_url: str, request: pytest.FixtureRequest
) -> AsyncIterator[None]:
    engine.configure(engine_bin, nats_direct_url)
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
