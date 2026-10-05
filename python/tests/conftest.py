"""Test fixtures: a real ``nats-server`` off PATH, and a unique resource prefix per test.

Mirrors ``test/harness/nats-server.ts``. Integration tests SKIP cleanly when the binary is absent,
exactly like the TypeScript suite, so a checkout without it still runs the pure tests.

The server enforces ACCOUNT LIMITS shaped like the managed service's: consumers capped PER STREAM,
a stream count, storage, ``max_bytes`` required, no memory storage, an 8 MiB max_payload. The
numbers live in ONE file both harnesses read (``test/harness/server-profiles.json``) so they cannot
drift; ``DE_TEST_PROFILE=ngs|tight|none`` selects the profile (default ``ngs``). With
``no_auth_user`` an unauthenticated client lands in the limited account, so the tests are unchanged.
``tests/integration/conftest.py`` is the per-test companion: the owner of a test's streams.

``DE_TEST_RTT_MS=<n>`` (opt-in) puts a delay proxy (``netproxy.py``) between every test and the
server: ``nats_url`` is then the PROXY, so the whole suite runs at that round-trip time without a
test knowing. ``nats_direct_url`` is always the server itself — for the harness's own hygiene, and
for a test that brings its own proxy and must not stack two.
"""

from __future__ import annotations

import contextlib
import json
import math
import os
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

import existing
from netproxy import ProxyProcess
from shell.blob_store import NatsBlobStore

# Native message scheduling (ctx.sleep / wait timeouts) needs nats-server >= 2.14, and provisioning
# fails loud without it — the same preflight the library performs.
SERVER_START_TIMEOUT_S = 10.0


def pytest_configure(config: pytest.Config) -> None:
    """The mode's lines (existing.say) go to the terminal, past pytest's capture."""

    def say(line: str) -> None:
        reporter = config.pluginmanager.get_plugin("terminalreporter")
        if reporter is None:
            print(line)
        else:
            reporter.write_line(line)

    existing.say = say


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _wait_for_tcp(host: str, port: int, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        with contextlib.suppress(OSError), socket.create_connection((host, port), timeout=0.5):
            return
        time.sleep(0.05)
    raise RuntimeError(f"nats-server did not accept connections on {host}:{port}")


# --- server profiles --------------------------------------------------------------------------
# The same table the TypeScript harness reads. `ngs` is the managed service's R1 tier as read from
# the bench account (docs/plans/test-axes.md, increment 1); `tight` is for hunting; `none` is a
# default local server.

REPO = Path(__file__).resolve().parents[2]
PROFILES_FILE = REPO / "test" / "harness" / "server-profiles.json"


@dataclass(frozen=True, slots=True)
class ServerLimits:
    """The account's JetStream limits, in the server's own config vocabulary."""

    max_payload: int
    max_mem: int
    max_file: int
    max_streams: int
    max_consumers: int
    max_bytes_required: bool


def limits_for(profile: str) -> ServerLimits | None:
    """The limits a profile stands for; ``None`` = a default local server."""
    table = json.loads(PROFILES_FILE.read_text(encoding="utf-8"))
    if profile not in table or profile == "$doc":
        raise ValueError(f'unknown server profile "{profile}" ({PROFILES_FILE})')
    row = table[profile]
    if row is None:
        return None
    return ServerLimits(max_payload=int(row["maxPayload"]), **row["account"])


def profile_from_env() -> str:
    """``DE_TEST_PROFILE``, defaulting to ``ngs``; an unknown value fails loud."""
    raw = os.environ.get("DE_TEST_PROFILE", "ngs")
    if raw not in ("ngs", "tight", "none"):
        raise ValueError(f'DE_TEST_PROFILE="{raw}": expected ngs, tight or none')
    return raw


def render_config(port: int, store: str, limits: ServerLimits | None) -> str:
    """The server config for a port, a store dir and (optionally) an account-limits profile.

    A generous SERVER store budget (1 TiB, logical) — under a profile the account's ``max_file`` is
    the one that binds. Every test prefix RESERVES ``max_bytes`` on each of its streams (~5 GiB at
    the 1 GiB default; ~0.2 GiB with the ``stream_max_bytes`` the integration tests pass), so
    overrunning either surfaces as an opaque "insufficient storage resources" at provision time.
    """
    conf = f"host: 127.0.0.1\nport: {port}\n"
    if limits is not None:
        conf += f"max_payload: {limits.max_payload}\n"
    conf += (
        f'jetstream {{ store_dir: "{store}", max_file_store: 1099511627776, '
        f"max_mem_store: 268435456 }}\n"
    )
    if limits is not None:
        # One account carrying the limits; `no_auth_user` maps every unauthenticated connection
        # into it, so a test's plain `nats.connect(url)` lands under the caps.
        conf += (
            "accounts {\n  DE {\n"
            f"    jetstream: {{ max_mem: {limits.max_mem}, max_file: {limits.max_file}, "
            f"max_streams: {limits.max_streams}, max_consumers: {limits.max_consumers}, "
            f"max_bytes_required: {'true' if limits.max_bytes_required else 'false'} }}\n"
            "    users: [ { user: de, password: de } ]\n  }\n}\nno_auth_user: de\n"
        )
    return conf


@dataclass(slots=True)
class LocalServer:
    proc: subprocess.Popen[bytes]
    url: str
    store: str
    limits: ServerLimits | None
    binary: str = ""
    port: int = 0

    def _halt(self, kill: bool) -> None:
        if kill:
            self.proc.kill()
        else:
            self.proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            self.proc.wait(timeout=5)
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()

    def restart(self, *, kill: bool = False, down_s: float = 0.0) -> None:
        """Take the server away and bring it back on the SAME port and store — a broker restart as
        a client sees it: every connection drops, JetStream state comes back from disk. ``kill`` is
        SIGKILL, a crash with no lame-duck goodbye; ``down_s`` is how long the port stays dark.
        Blocking (call it through ``asyncio.to_thread`` from a test that has workers running)."""
        self._halt(kill)
        if down_s > 0:
            time.sleep(down_s)
        self.proc = subprocess.Popen(
            [self.binary, "-c", str(Path(self.store) / "nats.conf")],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        _wait_for_tcp("127.0.0.1", self.port, SERVER_START_TIMEOUT_S)

    def stop(self) -> None:
        self._halt(kill=False)
        shutil.rmtree(self.store, ignore_errors=True)


def render_tenancy_config(port: int, store: str, prefix: str, tenants: list[str]) -> str:
    """A server for the engine's fabric mode, as the engine's tests set one up
    (``internal/testserver.TenancyConfig``): the account ``DE`` (user ``de``) exports
    ``{prefix}.{T}.api.>`` to each tenant ``T``, whose account imports it as ``{prefix}.api.>`` with
    ``share: true``; users ``de``, ``<t>-client`` and ``<t>-worker``, each password its name. The
    twin of ``renderTenancyConfig`` in ``test/harness/nats-server.ts``."""
    exports = "".join(
        f'      {{ service: "{prefix}.{t}.api.>", accounts: [{t}] }}\n' for t in tenants
    )
    accounts = "".join(
        f"  {t} {{\n    users: [ {{ user: {t.lower()}-client, password: {t.lower()}-client }}, "
        f"{{ user: {t.lower()}-worker, password: {t.lower()}-worker }} ]\n"
        f'    imports: [ {{ service: {{ account: DE, subject: "{prefix}.{t}.api.>" }}, '
        f'to: "{prefix}.api.>", share: true }} ]\n  }}\n'
        for t in tenants
    )
    return (
        f"host: 127.0.0.1\nport: {port}\n"
        f'jetstream {{ store_dir: "{store}", max_file_store: 1099511627776, '
        "max_mem_store: 268435456 }\n"
        "accounts {\n  DE {\n    jetstream: enabled\n    users: [ { user: de, password: de } ]\n"
        f"    exports: [\n{exports}    ]\n  }}\n{accounts}}}\n"
    )


def spawn_server(
    limits: ServerLimits | None, *, config: Callable[[int, str], str] | None = None
) -> LocalServer:
    """Start a ``nats-server -js`` on a free port under the given limits, or with a ``config`` of
    the test's own (:func:`render_tenancy_config`); skip if no binary."""
    binary = shutil.which("nats-server")
    if binary is None:
        pytest.skip("nats-server not found on PATH")
    port = _free_port()
    store = tempfile.mkdtemp(prefix="de-nats-py-")
    conf = Path(store) / "nats.conf"
    text = config(port, store) if config is not None else render_config(port, store, limits)
    conf.write_text(text, encoding="utf-8")
    proc = subprocess.Popen(
        [binary, "-c", str(conf)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    server = LocalServer(
        proc=proc,
        url=f"nats://127.0.0.1:{port}",
        store=store,
        limits=limits,
        binary=binary,
        port=port,
    )
    try:
        _wait_for_tcp("127.0.0.1", port, SERVER_START_TIMEOUT_S)
    except BaseException:
        server.stop()
        raise
    return server


def rtt_from_env() -> float:
    """``DE_TEST_RTT_MS``, default 0 (no proxy); a value that is not a number fails loud."""
    raw = os.environ.get("DE_TEST_RTT_MS", "")
    if raw == "":
        return 0.0
    try:
        rtt = float(raw)
    except ValueError:
        rtt = -1.0
    if rtt < 0:
        raise ValueError(f'DE_TEST_RTT_MS="{raw}": expected a number >= 0')
    return rtt


@pytest.fixture(scope="session")
def nats_direct_url() -> Iterator[str]:
    """A session-wide ``nats-server -js`` under the ``DE_TEST_PROFILE`` limits (default ``ngs``).

    Skips the test when the binary is not on PATH. This is the SERVER, whatever ``DE_TEST_RTT_MS``
    says: the harness's own hygiene uses it, and so does a test that brings its own proxy.
    """
    if existing.EXISTING is not None:
        # The mode for an existing server (existing.py): no spawn, no profile; the end-of-run
        # footprint check looks at the run prefix's streams only.
        yield existing.EXISTING.url
        _assert_no_ephemeral_consumers(existing.EXISTING.url)
        return
    server = spawn_server(limits_for(profile_from_env()))
    try:
        yield server.url
        # The footprint check: a suite that leaked an ephemeral consumer anywhere fails here, by
        # name, rather than passing green on a server that never counts them (LEARNINGS #56, #59).
        # tests/integration/conftest.py already checked after every test; this catches anything
        # created after the last one.
        _assert_no_ephemeral_consumers(server.url)
    finally:
        server.stop()


@pytest.fixture(scope="session")
def nats_url(nats_direct_url: str) -> Iterator[str]:
    """Where the tests connect: the server — or, under ``DE_TEST_RTT_MS``, a delay proxy in front
    of it, so every existing test runs at a WAN round trip without knowing."""
    rtt = rtt_from_env()
    if rtt <= 0:
        _measure_in_mode(nats_direct_url, nats_direct_url, rtt)
        yield nats_direct_url
        return
    proxy = ProxyProcess(nats_direct_url, rtt)
    try:
        _measure_in_mode(nats_direct_url, proxy.url, rtt)
        yield proxy.url
    finally:
        proxy.stop()


def _measure_in_mode(server_url: str, url: str, proxy_rtt: float) -> None:
    """In the mode: the round trip to where the tests connect, measured once, and the factor every
    wait is scaled by (existing.py), printed once."""
    import asyncio

    ex = existing.EXISTING
    if ex is None:
        return

    async def measure(u: str) -> float:
        nc = await existing.connect(u, name="test-harness-rtt")
        try:
            return await existing.measure_rtt(nc)
        finally:
            await nc.close()

    server_rtt = asyncio.run(measure(server_url))
    rtt_ms = asyncio.run(measure(url)) if url != server_url else server_rtt
    existing.FACTOR = existing.rtt_factor(rtt_ms)
    via = (
        f" (the server {server_rtt:.1f} ms, the proxy {proxy_rtt:g} ms)"
        if url != server_url
        else ""
    )
    existing.say(
        f"existing server {ex.url}{' (creds)' if ex.creds else ''}, prefix {ex.prefix}, round trip "
        f"{rtt_ms:.1f} ms{via} -> every wait x {existing.FACTOR:g}"
    )


async def list_ephemeral_consumers(nc: Any) -> list[str]:
    """Every consumer on the server that has no durable name — one the library did not mean to
    keep. The worker's pulls are the only consumers the library creates; journal loads, blob reads
    and a waiting ``result()`` are direct gets or core subscriptions."""
    jsm = nc.jsm()
    left: list[str] = []
    for stream in await jsm.streams_info():
        name = stream.config.name
        if name is None:
            continue
        for consumer in await jsm.consumers_info(name):
            if not consumer.config.durable_name:
                left.append(f"{name}/{consumer.name}")
    return left


def leftover_message(left: list[str], where: str) -> str:
    return (
        f"{len(left)} ephemeral consumer(s) left on the test server {where} — the library must "
        f"not leak consumers (LEARNINGS #56/#59). First: {', '.join(left[:12])}"
    )


async def delete_all_streams(nc: Any) -> int:
    """Delete every stream on the server (KV buckets and object stores are streams too).

    The owner of a test prefix's seven streams: the shared server is the suite's own, and under an
    account profile the stream cap is 16 — two prefixes — so what a test provisions must be gone
    before the next test starts, not at the end of the run.
    """
    from nats.js.errors import NotFoundError

    jsm = nc.jsm()
    names = [s.config.name for s in await jsm.streams_info() if s.config.name is not None]
    for name in names:
        with contextlib.suppress(NotFoundError):  # a KV's backing stream went with its bucket
            await jsm.delete_stream(name)
    return len(names)


def _assert_no_ephemeral_consumers(url: str) -> None:
    """The end-of-run footprint check. Runs in its own event loop: the session fixture is sync."""
    import asyncio

    async def scan() -> list[str]:
        # In the mode the streams are read through the admin connection (as a tenant, the engine's
        # account's), elsewhere on the spawned server.
        nc = await (existing.connect_admin() if existing.EXISTING else existing.connect(url))
        try:
            if existing.EXISTING is not None:
                return await existing.prefix_ephemeral_consumers(
                    nc, existing.EXISTING.prefix, existing.AT_START
                )
            return await list_ephemeral_consumers(nc)
        finally:
            await nc.drain()

    left = asyncio.run(scan())
    if left:
        raise AssertionError(leftover_message(left, "after the suite"))


def over_payload(nc: Any, factor: float = 1.5) -> int:
    """``factor`` × THIS server's max_payload — never a literal MiB. The server runs under a
    profile: 8 MiB on ``ngs``, 64 KiB on ``tight``, 1 MiB on ``none``. A test that wrote "2 MiB is
    over the limit" was true on exactly one of them, and under ``ngs`` every offload test quietly
    stopped offloading — the assertions that checked for a ref caught it."""
    return math.ceil(int(nc.max_payload) * factor)


@pytest.fixture
def prefix() -> str:
    """A unique resource prefix per test, so tests never share streams (or each other's state) —
    or, against an existing server, the run's one prefix (existing.py)."""
    return existing.map_prefix(f"t{uuid.uuid4().hex[:10]}")


@pytest.fixture
async def nc(nats_url: str) -> AsyncIterator[object]:
    """A connected NATS client, closed at teardown."""
    conn = await existing.connect(nats_url)
    try:
        yield conn
    finally:
        await conn.close()


# --- test-side blob inspection ---------------------------------------------------------------
# WITHOUT the object-store client's ``list()`` / ``get()``: both stream through an ordered push
# subscription whose consumer is never deleted (LEARNINGS #59), which the session fixture above now
# counts as a leak. Objects are listed from their meta records with a multi-subject last-message
# direct get, and read through the library's own consumer-free reader. Imported by tests as
# ``from conftest import list_object_names, read_blob`` (pytest puts this directory on sys.path).


async def list_object_names(nc: Any, bucket: str) -> list[str]:
    """Every live object in the bucket (deleted ones excluded), from the meta subjects."""
    inbox = nc.new_inbox()
    sub = await nc.subscribe(inbox)
    names: list[str] = []
    try:
        await nc.publish(
            f"$JS.API.DIRECT.GET.OBJ_{bucket}",
            json.dumps({"multi_last": [f"$O.{bucket}.M.>"]}).encode(),
            reply=inbox,
        )
        while True:
            msg = await sub.next_msg(timeout=5.0)
            headers = msg.headers or {}
            status = headers.get("Status")
            if status is not None:
                if status in ("204", "404"):  # end of batch / no objects at all
                    break
                raise RuntimeError(f"direct get: status {status} {headers.get('Description')}")
            info = json.loads(msg.data)
            if not info.get("deleted"):
                names.append(str(info["name"]))
    finally:
        with contextlib.suppress(Exception):
            await sub.unsubscribe()
    return names


async def read_blob(nc: Any, bucket: str, name: str) -> bytes:
    """The object's bytes through the library's reader (one batched direct get)."""
    return await NatsBlobStore(nc, bucket).get(name)
