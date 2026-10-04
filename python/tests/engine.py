"""The one thing this harness adds to the library's: ``durable-engine serve``, next to the spawned
nats-server. The Python twin of ``test/harness/engine.ts`` and ``test/harness/engine-bin.ts``.

The library's suites build a worker or a client on a prefix of their own and expect the prefix to
work; here the prefix needs an engine, so the suites import ``create_worker`` and ``create_client``
from this module, which start one ``serve`` per (server, prefix) on first use and hand the call on
to the SDK's. The library's per-worker options that are the engine's flags now (``ack_wait_ms`` →
``--ack-wait``, ``lease=False`` → ``--no-lease``, …) become the engine's command line.
``tests/integration/conftest.py`` stops every engine after each test, before it deletes the streams.

The binary: ``DE_ENGINE`` (a path), or built with ``go build -o`` from the engine checkout
(``DE_ENGINE_REPO``, default ``../durable-engine``) at the commit ``engine.pin`` names. A checkout
at another commit, or with local changes, is refused unless ``DE_ENGINE_ANY=1``. Without either the
integration suites skip, saying why. Nothing reaches beyond 127.0.0.1 unless ``DE_NATS_URL`` says
so.

Against an existing server (``existing.py``, ``DE_NATS_URL``) there is one prefix and one engine
for the run instead: every prefix a test names is the run's, the engine is the session's
(``RunEngine``), and a test's flags restart it only when they differ from the ones it runs with.
Its counters (``engine_counts``) and its log (``engine_log``) are the test's own: the difference
from the test's start, as a fresh engine per prefix gave them.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import os
import shutil
import subprocess
import sys
import threading
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

import existing
from synadia_durable import Client, RunHandle, Worker, WorkerHealth
from synadia_durable import create_client as sdk_create_client
from synadia_durable import create_worker as sdk_create_worker
from synadia_durable.journal.blobs import MIN_OFFLOAD_THRESHOLD_BYTES

if TYPE_CHECKING:
    from synadia_durable.types import RunStatus, StartOptions, Workflow

REPO = Path(__file__).resolve().parents[2]


class EngineNotAvailableError(Exception):
    """No engine binary, and none can be built: the integration suites skip."""


def engine_pin() -> str:
    """The commit ``engine.pin`` names."""
    return (REPO / "engine.pin").read_text(encoding="utf-8").strip()


def resolve_engine_binary() -> str:
    """The engine binary to run: ``DE_ENGINE``, or a build of the pinned checkout.

    Raises :class:`EngineNotAvailableError` when neither can be had (the suites skip), and a plain
    ``RuntimeError`` when the checkout is at the wrong commit (the run fails: a silent skip would
    hide it).
    """
    given = os.environ.get("DE_ENGINE", "")
    if given:
        if not (os.path.isfile(given) and os.access(given, os.X_OK)):
            raise RuntimeError(f"DE_ENGINE={given}: not an executable file")
        return given
    pin = engine_pin()
    src = Path(os.environ.get("DE_ENGINE_REPO") or REPO.parent / "durable-engine")
    if not (src / "go.mod").exists():
        raise EngineNotAvailableError(
            f"no DE_ENGINE and no engine checkout at {src} (DE_ENGINE_REPO)"
        )
    if shutil.which("go") is None:
        raise EngineNotAvailableError(f"no DE_ENGINE, and Go is not on PATH to build {src}")

    def git(*args: str) -> str:
        return subprocess.check_output(["git", "-C", str(src), *args], text=True).strip()

    head = git("rev-parse", "HEAD")
    any_ = os.environ.get("DE_ENGINE_ANY") == "1"
    if not head.startswith(pin) and not any_:
        raise RuntimeError(
            f"the engine checkout {src} is at {head[:7]}, engine.pin names {pin}: check out {pin} "
            "there, point DE_ENGINE at a binary, or set DE_ENGINE_ANY=1"
        )
    dirty = git("status", "--porcelain", "--untracked-files=no") != ""
    if dirty and not any_:
        raise RuntimeError(
            f"the engine checkout {src} has local changes; it is not {pin} (DE_ENGINE_ANY=1 to "
            "run it anyway)"
        )
    out_dir = REPO / ".engine-bin"
    out_dir.mkdir(exist_ok=True)
    out = out_dir / f"durable-engine-{head[:12]}{'-dirty' if dirty else ''}"
    if not out.exists() or dirty:
        subprocess.run(["go", "build", "-o", str(out), "./cmd/durable-engine"], cwd=src, check=True)
    return str(out)


# --- the engine's command line -------------------------------------------------------------------


@dataclass(frozen=True)
class EngineFlags:
    """The library's worker/client options that are the engine's command line now."""

    stream_max_bytes: int | None = None
    ack_wait_ms: int | None = None
    max_work_ms: int | None = None
    lease: bool | None = None
    lease_ttl_ms: int | None = None
    maintenance_interval_ms: int | None = None
    reconcile_grace_ms: int | None = None
    offload_threshold: int | None = None

    def args(self) -> list[str]:
        out: list[str] = []
        if self.stream_max_bytes is not None:
            out += ["--stream-max-bytes", str(self.stream_max_bytes)]
        if self.ack_wait_ms is not None:
            out += ["--ack-wait", f"{self.ack_wait_ms}ms"]
        if self.max_work_ms is not None:
            out += ["--max-work", f"{self.max_work_ms}ms"]
        if self.lease is False:
            out += ["--no-lease"]
        if self.lease_ttl_ms is not None:
            out += ["--lease-ttl", f"{self.lease_ttl_ms}ms"]
        if self.maintenance_interval_ms is not None:
            out += ["--maintenance-interval", f"{self.maintenance_interval_ms}ms"]
        if self.reconcile_grace_ms is not None:
            out += ["--reconcile-grace", f"{self.reconcile_grace_ms}ms"]
        if self.offload_threshold is not None:
            out += ["--offload-threshold", str(self.offload_threshold)]
        return out


_FLAG_NAMES = (
    "stream_max_bytes",
    "ack_wait_ms",
    "max_work_ms",
    "lease",
    "lease_ttl_ms",
    "maintenance_interval_ms",
    "reconcile_grace_ms",
)


def flags_of(opts: dict[str, Any]) -> EngineFlags:
    flags = {k: opts[k] for k in _FLAG_NAMES if opts.get(k) is not None}
    # `offload=OffloadOptions(threshold_bytes=…)` is `serve --offload-threshold` now (the engine's
    # F22), clamped as the library clamped it, to its 1 KiB floor.
    threshold = getattr(opts.get("offload"), "threshold_bytes", None)
    if threshold is not None:
        flags["offload_threshold"] = max(MIN_OFFLOAD_THRESHOLD_BYTES, int(threshold))
    return EngineFlags(**flags)


_instances = 0


class EngineProcess:
    """One ``durable-engine serve`` process. Its stderr is kept (the last 2 000 lines)."""

    def __init__(
        self,
        binary: str,
        url: str,
        prefix: str,
        flags: EngineFlags,
        *,
        creds: str | None = None,
        metrics_addr: str | None = None,
        start_timeout_s: float = 30.0,
        on_line: Any = None,
    ) -> None:
        global _instances
        _instances += 1
        self.binary = binary
        self.url = url
        self.prefix = prefix
        self.flags = flags
        self.instance = f"t{_instances}"
        self.log: deque[str] = deque(maxlen=2_000)
        self._proc: subprocess.Popen[bytes] | None = None
        self._serving = threading.Event()
        self.creds = creds
        self.metrics_addr = metrics_addr
        self.start_timeout_s = start_timeout_s
        self.on_line = on_line

    @property
    def args(self) -> list[str]:
        extra: list[str] = []
        if self.creds is not None:
            extra += ["--creds", self.creds]
        if self.metrics_addr is not None:
            extra += ["--metrics-addr", self.metrics_addr]
        return [
            "serve",
            "--nats",
            self.url,
            *extra,
            "--prefix",
            self.prefix,
            "--instance",
            self.instance,
            *self.flags.args(),
        ]

    def _read(self, proc: subprocess.Popen[bytes]) -> None:
        assert proc.stderr is not None
        echo = os.environ.get("DE_ENGINE_LOG") == "1"
        for raw in proc.stderr:
            line = raw.decode("utf-8", "replace").rstrip("\n")
            self.log.append(line)
            if self.on_line is not None:
                self.on_line(line)
            if echo:
                print(f"[engine {self.prefix}] {line}", file=sys.stderr)
            if "msg=serving" in line:
                self._serving.set()
        self._serving.set()  # exited: start() looks at the process

    async def start(self) -> None:
        """Spawn and wait for the engine's ``serving`` line (it provisions the prefix first)."""
        proc = subprocess.Popen(
            [self.binary, *self.args], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
        )
        self._proc = proc
        threading.Thread(target=self._read, args=(proc,), daemon=True).start()
        up = await asyncio.to_thread(self._serving.wait, self.start_timeout_s)
        if not up or proc.poll() is not None:
            proc.kill()
            log = "\n".join(self.log)
            raise RuntimeError(f"durable-engine serve did not come up ({proc.poll()}):\n{log}")

    @property
    def running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    async def _signal(self, kill: bool) -> None:
        proc, self._proc = self._proc, None
        if proc is None or proc.poll() is not None:
            return
        if kill:
            proc.kill()
        else:
            proc.terminate()
        try:
            await asyncio.to_thread(proc.wait, 10.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            await asyncio.to_thread(proc.wait)

    async def stop(self) -> None:
        """A graceful stop: the engine naks what it holds, so it redelivers at once."""
        await self._signal(kill=False)

    async def kill(self) -> None:
        """A crash: its leases are lost, its items redeliver at the ack wait."""
        await self._signal(kill=True)


# --- one engine per (server, prefix) -------------------------------------------------------------

_engines: dict[str, asyncio.Task[EngineProcess]] = {}
_binary: str | None = None
_default_server: str | None = None
_server_override: str | None = None


def configure(binary: str | None, server_url: str | None) -> None:
    """The session's engine binary and nats-server (tests/integration/conftest.py, per test)."""
    global _binary, _default_server
    _binary = binary
    _default_server = server_url


def set_engine_server(url: str | None) -> None:
    """The server the engines of the current test connect to: the suite's shared server by default
    (never the delay proxy: the engine sits next to NATS), or the one a suite spawned itself."""
    global _server_override
    _server_override = url


def _server_url() -> str:
    url = _server_override or _default_server
    if not url:
        raise EngineNotAvailableError("no nats-server for the engine")
    return url


async def ensure_engine(prefix: str, flags: EngineFlags | None = None) -> EngineProcess | None:
    """The engine serving ``prefix`` on the current server, started on first use with ``flags``."""
    if existing.in_mode():
        return await _ensure_run_engine(flags or EngineFlags())
    url = _server_url()
    key = f"{url}|{prefix}"
    task = _engines.get(key)
    if task is None:
        if not _binary:
            raise EngineNotAvailableError("no engine binary")
        e = EngineProcess(_binary, url, prefix, flags or EngineFlags())

        async def up() -> EngineProcess:
            await e.start()
            return e

        task = asyncio.ensure_future(up())
        _engines[key] = task
        task.add_done_callback(
            lambda t: _engines.pop(key, None) if t.cancelled() or t.exception() else None
        )
    return await asyncio.shield(task)


async def engine_for(prefix: str) -> EngineProcess | None:
    if existing.in_mode():
        return _run.current if _run is not None else None
    task = _engines.get(f"{_server_url()}|{prefix}")
    return await task if task is not None else None


async def stop_engines() -> None:
    """Stop every engine (after each test)."""
    tasks = list(_engines.values())
    _engines.clear()
    for t in tasks:
        with contextlib.suppress(Exception):
            await (await t).stop()


async def restart_engine(prefix: str, *, kill: bool = False) -> None:
    """Stop the engine serving ``prefix`` (gracefully, or ``kill=True`` for a crash); the next
    worker or client on the prefix starts a fresh one, which provisions what is missing — the
    library's "a restarted worker re-provisions", now the engine's."""
    global _test_flags_fixed, _test_ensure
    if existing.in_mode():
        if _run is None or _run.external:
            pytest.skip(
                "existing-server mode: the engine is DE_ENGINE_URL's, it cannot be restarted"
            )
        await _run.halt(kill)
        _test_flags_fixed, _test_ensure = (
            False,
            None,
        )  # the next worker or client starts it again, with its flags
        return
    task = _engines.pop(f"{_server_url()}|{prefix}", None)
    if task is None:
        return
    e = await task
    await (e.kill() if kill else e.stop())


async def engine_log(prefix: str) -> list[str]:
    """The log lines of the engine serving ``prefix`` (its stderr, as ``serve`` writes it)."""
    if existing.in_mode():
        # The run's one engine: the lines since the test began, as a test's own engine gave them.
        return _run.log_since(_test_log_from) if _run is not None else []
    e = await engine_for(prefix)
    return list(e.log) if e is not None else []


async def engine_counts(nc: Any, prefix: str) -> dict[str, float]:
    """The engine's own counters (``info``'s ``counts``, its /metrics numbers by series name), for
    an assertion that was about what the library's worker did and is now the engine's."""
    msg = await nc.request(
        f"{existing.map_prefix(prefix)}.api.v1.info", b"{}", timeout=existing.scaled(5)
    )
    info = json.loads(msg.data)
    counts: dict[str, float] = info.get("counts") or {}
    # One engine for the run: the test's own counts are the difference from its start
    # (begin_test), unless the engine was restarted since, when they start at zero.
    if not existing.in_mode() or _test_counts is None or _test_counts[0] != info.get("instance"):
        return counts
    base = _test_counts[1]
    return {k: v - base.get(k, 0) for k, v in counts.items()}


def count_of(counts: dict[str, float], name: str, **labels: str) -> float:
    """The sum of every series of ``name`` whose labels include all of ``labels``."""
    n = 0.0
    for k, v in counts.items():
        if k != name and not k.startswith(f"{name}{{"):
            continue
        if all(f'{label}="{value}"' in k for label, value in labels.items()):
            n += v
    return n


async def provision(_nc: Any, names: Any, opts: Any = None) -> None:
    """The library's ``provision(nc, names, opts)``, re-expressed: provisioning is the engine's, at
    its start. A suite that provisioned a prefix to stage a journal by hand starts its engine."""
    smb = getattr(opts, "stream_max_bytes", None) if opts is not None else None
    await ensure_engine(names.prefix, EngineFlags(stream_max_bytes=smb))


# --- the existing-server mode: one engine for the run --------------------------------------------


def run_flags(flags: EngineFlags) -> EngineFlags:
    """Flags as the run's engine takes them: ``stream_max_bytes`` dropped (the prefix's seven are
    sized once, by the operator or the first provisioning, never resized by a test against a server
    the run does not own)."""
    return dataclasses.replace(flags, stream_max_bytes=None)


class RunEngine:
    """The ONE engine of a run against an existing server, the session's (integration/conftest.py).
    ``ensure`` restarts it only when a test's flags differ from the ones it runs with. Under
    ``DE_ENGINE_URL`` it is not the harness's: never started, stopped or restarted, and a test that
    needs other flags or a restart is skipped, saying so."""

    def __init__(
        self,
        binary: str | None,
        url: str,
        prefix: str,
        *,
        creds: str | None,
        metrics_addr: str | None,
        start_timeout_s: float,
    ) -> None:
        self.binary = binary
        self.url = url
        self.prefix = prefix
        self.creds = creds
        self.metrics_addr = metrics_addr
        self.start_timeout_s = start_timeout_s
        self.current: EngineProcess | None = None
        self.flags = EngineFlags()
        self.starts = 0
        self._log: list[str] = []
        self._dropped = 0

    @property
    def external(self) -> bool:
        return self.binary is None

    def _line(self, line: str) -> None:
        self._log.append(line)
        if len(self._log) > 20_000:
            del self._log[:10_000]
            self._dropped += 10_000

    @property
    def log_mark(self) -> int:
        return self._dropped + len(self._log)

    def log_since(self, since: int) -> list[str]:
        return self._log[max(0, since - self._dropped) :]

    async def ensure(self, flags: EngineFlags) -> None:
        want = run_flags(flags)
        if self.external:
            if want != EngineFlags():
                pytest.skip(
                    f"existing-server mode: the engine is DE_ENGINE_URL's, it cannot take {want}"
                )
            return
        cur = self.current
        if cur is not None and cur.running and want == self.flags:
            return
        if cur is not None:
            await cur.stop()
        assert self.binary is not None
        e = EngineProcess(
            self.binary,
            self.url,
            self.prefix,
            want,
            creds=self.creds,
            metrics_addr=self.metrics_addr,
            start_timeout_s=self.start_timeout_s,
            on_line=self._line,
        )
        self.current, self.flags = e, want
        self.starts += 1
        await e.start()

    async def halt(self, kill: bool) -> None:
        cur, self.current = self.current, None
        if cur is not None:
            await (cur.kill() if kill else cur.stop())

    async def stop(self) -> None:
        await self.halt(kill=False)


_run: RunEngine | None = None
# The current test in the mode: its flags are fixed by its first call, as the first worker or
# client on a prefix fixed a spawned engine's.
_test_flags_fixed = False
_test_ensure: asyncio.Task[None] | None = None
_test_counts: tuple[str, dict[str, float]] | None = None
_test_log_from = 0


def configure_run(run: RunEngine | None) -> None:
    global _run
    _run = run


async def begin_test(nc: Any) -> None:
    """Before each test in the mode: the counters and the log line the test's are measured from."""
    global _test_flags_fixed, _test_counts, _test_log_from, _test_ensure
    _test_flags_fixed = False
    _test_ensure = None
    _test_counts = None
    _test_log_from = _run.log_mark if _run is not None else 0
    with contextlib.suppress(Exception):  # no engine running right now: the counts start at zero
        msg = await nc.request(
            f"{existing.map_prefix('de')}.api.v1.info", b"{}", timeout=existing.scaled(5)
        )
        info = json.loads(msg.data)
        _test_counts = (str(info.get("instance")), info.get("counts") or {})


async def _ensure_run_engine(flags: EngineFlags) -> EngineProcess | None:
    global _test_flags_fixed, _test_ensure
    if _run is None:
        raise EngineNotAvailableError("no run engine (integration/conftest.py)")
    if not _test_flags_fixed:
        # The first call fixes the flags; a concurrent second one waits for the same (re)start.
        if _test_ensure is None or (_test_ensure.done() and _test_ensure.exception() is not None):
            _test_ensure = asyncio.ensure_future(_run.ensure(flags))
        await asyncio.shield(_test_ensure)
        _test_flags_fixed = True
    return _run.current


# --- the SDK's create_worker / create_client, with the prefix's engine started first -------------


class _ConsoleLogger:
    """``DE_SDK_LOG=1``: a worker without a logger of its own logs to stderr (debugging a suite)."""

    def _emit(self, level: str, message: str, fields: dict[str, Any]) -> None:
        print(f"[sdk {level}] {message} {fields or ''}", file=sys.stderr)

    def debug(self, message: str, **fields: Any) -> None:
        self._emit("debug", message, fields)

    def info(self, message: str, **fields: Any) -> None:
        self._emit("info", message, fields)

    def warn(self, message: str, **fields: Any) -> None:
        self._emit("warn", message, fields)

    def error(self, message: str, **fields: Any) -> None:
        self._emit("error", message, fields)


#: The workers the current test started and did not stop.
_test_workers: set[EngineWorker] = set()
#: The workflow-name sets of every worker the current test started, stopped or not.
_test_workflow_sets: list[tuple[str, ...]] = []


class EngineWorker:
    """The SDK's worker, with the prefix's engine started before its first poll."""

    def __init__(
        self, worker: Worker, prefix: str, flags: EngineFlags, names: tuple[str, ...] = ()
    ) -> None:
        self._w = worker
        self._prefix = prefix
        self._flags = flags
        self.names = names

    async def start(self) -> None:
        await ensure_engine(self._prefix, self._flags)
        _test_workers.add(self)
        _test_workflow_sets.append(self.names)
        await self._w.start()

    async def stop(self, drain_timeout_s: float = 10.0) -> None:
        _test_workers.discard(self)
        await self._w.stop(drain_timeout_s)

    def health(self) -> WorkerHealth:
        return self._w.health()

    async def __aenter__(self) -> EngineWorker:
        await self.start()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.stop()


def create_worker(nc: Any, workflows: list[Workflow[Any, Any]], **opts: Any) -> EngineWorker:
    if os.environ.get("DE_SDK_LOG") == "1" and opts.get("logger") is None:
        opts = {**opts, "logger": _ConsoleLogger()}
    prefix = existing.map_prefix(opts.get("prefix", "de"))
    if "prefix" in opts or existing.in_mode():
        opts = {**opts, "prefix": prefix}
    names = tuple(sorted({wf.name for wf in workflows}))
    return EngineWorker(sdk_create_worker(nc, workflows, **opts), prefix, flags_of(opts), names)


async def end_test_workers(nc: Any) -> None:
    """The hygiene, after each test in the mode, before its cleanup (docs/findings.md F33): a
    worker the test left running (it failed, or timed out) is stopped, bounded; and every waiting
    poll for each of the test's workflow sets is withdrawn (``poll {cancelPoll}``), so the engine
    holds no pending fetch that would re-create the durables the cleanup deletes."""
    left = list(_test_workers)
    _test_workers.clear()
    sets = {s for s in _test_workflow_sets if s}
    _test_workflow_sets.clear()

    async def bounded(aw: Any) -> None:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(aw, existing.scaled(5))

    await asyncio.gather(*(bounded(w.stop(1.0)) for w in left))
    subject = f"{existing.map_prefix('de')}.api.v1.poll"
    await asyncio.gather(
        *(
            bounded(
                nc.request(
                    subject,
                    json.dumps(
                        {
                            "workflows": [{"name": n, "versions": [1]} for n in names],
                            "cancelPoll": True,
                        }
                    ).encode(),
                    timeout=existing.scaled(5),
                )
            )
            for names in sets
        )
    )


class EngineHandle(RunHandle[Any]):
    """The SDK's handle, with the prefix's engine started before its first request."""

    def __init__(self, client: EngineClient, h: RunHandle[Any]) -> None:
        super().__init__(h._client, h.run_id)
        self._engine_client = client

    async def result(self, timeout_ms: float = 30_000) -> Any:
        await self._engine_client.ready()
        # Against an existing server every wait is scaled by the measured round trip.
        return await super().result(timeout_ms * existing.FACTOR)

    async def status(self) -> RunStatus:
        await self._engine_client.ready()
        return await super().status()


class EngineClient:
    """The SDK's client, with the prefix's engine started before its first request."""

    def __init__(self, client: Client, prefix: str, flags: EngineFlags) -> None:
        self._c = client
        self._prefix = prefix
        self._flags = flags

    async def ready(self) -> None:
        await ensure_engine(self._prefix, self._flags)

    async def start(
        self, workflow: Workflow[Any, Any], input: Any, opts: StartOptions | None = None
    ) -> EngineHandle:
        await self.ready()
        return EngineHandle(self, await self._c.start(workflow, input, opts))

    def get(self, run_id: str) -> EngineHandle:
        return EngineHandle(self, self._c.get(run_id))

    async def signal(self, run_id: str, name: str, data: Any) -> None:
        await self.ready()
        await self._c.signal(run_id, name, data)

    async def cancel(self, run_id: str, reason: str | None = None) -> None:
        await self.ready()
        await self._c.cancel(run_id, reason)


def create_client(nc: Any, **opts: Any) -> EngineClient:
    prefix = existing.map_prefix(opts.get("prefix", "de"))
    if "prefix" in opts or existing.in_mode():
        opts = {**opts, "prefix": prefix}
    return EngineClient(sdk_create_client(nc, **opts), prefix, flags_of(opts))


__all__ = [
    "EngineClient",
    "EngineFlags",
    "EngineNotAvailableError",
    "EngineProcess",
    "EngineWorker",
    "RunEngine",
    "begin_test",
    "configure",
    "configure_run",
    "count_of",
    "create_client",
    "create_worker",
    "end_test_workers",
    "engine_counts",
    "engine_for",
    "engine_log",
    "engine_pin",
    "ensure_engine",
    "provision",
    "resolve_engine_binary",
    "restart_engine",
    "set_engine_server",
    "stop_engines",
]
