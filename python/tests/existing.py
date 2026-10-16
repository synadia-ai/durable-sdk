"""The harness's mode for an EXISTING server (docs/findings.md F28, F29): ``DE_NATS_URL`` names a
server the run does not own, instead of the ``nats-server`` the harness spawns. The twin of
``test/harness/existing.ts``.

    DE_NATS_URL     the server (a URL the client and the engine both reach); switches the mode on
    DE_NATS_CREDS   a creds file for it, when it needs one (read by nats-py and the engine's
                    ``--creds``, never by this harness)
    DE_PREFIX       the run's one prefix (default a generated ``de-<run id>``)
    DE_ENGINE_URL   an engine that already serves the prefix, by its /metrics address
                    (``http://host:port``); without it the harness starts one for the run
    DE_CLEANUP      ``delete`` or ``keep`` the prefix's eight at the run's end; by default a
                    generated prefix is deleted (only this run can have made it) and a DE_PREFIX
                    one kept
    DE_TENANT       the tests run as this tenant of an engine in tenancy mode (``serve --tenancy``,
                    the engine's PR #7): the token the tenant's import places
    DE_ADMIN_URL    where the harness reads the engine's streams and consumers (default
    DE_ADMIN_CREDS  DE_NATS_URL, DE_NATS_CREDS): as a tenant the tests' own connection sees none

No spawn, no account profile (the server's own limits apply), one prefix and one engine for the
run, a cleanup that purges what each test wrote under the prefix and never deletes a stream during
the run, the leftovers check on the prefix's streams only, and every wait scaled by the measured
round trip. A test that failed or timed out is cleaned like any other, and its runs' blobs are
deleted by name, after it and again at the run's end (docs/findings.md F33).

As a tenant (``DE_TENANT``) the eight are the engine account's, shared by every tenant: everything
above is read through the admin connection and counts the tenant's subjects
(``{prefix}.{tenant}.…``, ``$KV.<bucket>.{tenant}.…``), objects (``{tenant}/…``) and durables
(``{prefix}~{tenant}~…``) alone; another tenant's are never purged or deleted, and the eight never
are.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import secrets
from dataclasses import dataclass
from typing import Any

import pytest


@dataclass(frozen=True)
class ExistingServer:
    url: str
    creds: str | None
    prefix: str
    engine_url: str | None
    #: The prefix is the run's own (no DE_PREFIX): generated here, so nobody else's.
    generated: bool
    delete_at_end: bool
    #: DE_TENANT: the tests run as this tenant of an engine in tenancy mode.
    tenant: str | None = None
    #: DE_ADMIN_URL / DE_ADMIN_CREDS: the harness's view of the engine's streams.
    admin_url: str = ""
    admin_creds: str | None = None


def _from_env() -> ExistingServer | None:
    url = os.environ.get("DE_NATS_URL", "")
    if not url:
        return None
    cleanup = os.environ.get("DE_CLEANUP", "")
    if cleanup not in ("", "delete", "keep"):
        raise ValueError(f'DE_CLEANUP="{cleanup}": expected delete or keep')
    given = os.environ.get("DE_PREFIX", "")
    prefix = given or f"de-{secrets.token_hex(4)}"
    if not re.fullmatch(r"[A-Za-z0-9_-]+", prefix):
        raise ValueError(f'DE_PREFIX="{prefix}": one subject token, no dots')
    tenant = os.environ.get("DE_TENANT") or None
    if tenant is not None:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", tenant):
            raise ValueError(f'DE_TENANT="{tenant}": one subject token, no dots')
        # The tenant's import names the prefix and the engine in the other account serves it: the
        # run cannot make it up, and the eight are every tenant's, never the run's to delete.
        if not given:
            raise ValueError("DE_TENANT needs DE_PREFIX: the prefix the tenant's import uses")
        if not os.environ.get("DE_ENGINE_URL"):
            raise ValueError("DE_TENANT needs DE_ENGINE_URL: the engine runs in its own account")
        if cleanup == "delete":
            raise ValueError("DE_CLEANUP=delete with DE_TENANT: the eight are every tenant's")
    admin_url = os.environ.get("DE_ADMIN_URL") or url
    admin_creds = os.environ.get("DE_ADMIN_CREDS") or (
        None if os.environ.get("DE_ADMIN_URL") else os.environ.get("DE_NATS_CREDS") or None
    )
    return ExistingServer(
        url=url,
        creds=os.environ.get("DE_NATS_CREDS") or None,
        prefix=prefix,
        engine_url=os.environ.get("DE_ENGINE_URL") or None,
        generated=not given,
        # A generated prefix is deleted unless DE_CLEANUP=keep; a given one kept unless =delete.
        delete_at_end=cleanup == "delete" or (cleanup == "" and not given),
        tenant=tenant,
        admin_url=admin_url,
        admin_creds=admin_creds,
    )


#: The mode's settings for this run (one pytest process: the prefix is generated once), or None.
EXISTING: ExistingServer | None = _from_env()
if EXISTING is not None and EXISTING.creds is not None:
    # The conformance workers and clients the interop suites spawn read NATS_CREDS.
    os.environ["NATS_CREDS"] = EXISTING.creds

#: The factor every wait is scaled by (set once the round trip is measured, conftest.py).
FACTOR = 1.0
#: The consumers on the prefix's streams when the run began (integration/conftest.py).
AT_START: frozenset[str] = frozenset()
#: The throughput the probe measured at the start, bytes per second up and down together (0: not
#: measured, the spawned server), and the tests' max_payload (integration/conftest.py).
THROUGHPUT = 0.0
MAX_PAYLOAD = 0
#: How many times the probe's time a large value's bytes are given (test/harness/budget.ts).
HEADROOM = 2
#: Every run id the run's tests touched: each test's cleanup deletes all their blobs by name, and
#: so does the run's end (``sweep_run``), so a blob a timed-out test's run wrote late goes too.
RUN_IDS: set[str] = set()


def say(line: str) -> None:
    """A line on pytest's terminal, past its capture (conftest.py points this at the reporter)."""
    print(line)


def in_mode() -> bool:
    return EXISTING is not None


def map_prefix(prefix: str) -> str:
    """The run's prefix in the mode; elsewhere the prefix the test named."""
    return EXISTING.prefix if EXISTING is not None else prefix


def connect_kwargs() -> dict[str, Any]:
    """What ``nats.connect`` needs for the server: the creds, when the mode names any."""
    if EXISTING is not None and EXISTING.creds is not None:
        return {"user_credentials": EXISTING.creds}
    return {}


async def connect(url: str, **kwargs: Any) -> Any:
    """``nats.connect`` with the mode's creds (every test and the harness connect through this)."""
    import nats

    return await nats.connect(url, **connect_kwargs(), **kwargs)


AS_TENANT = (
    "as a tenant (DE_TENANT): reads the engine's streams, buckets or objects itself, and the "
    "tests' connection has none, the engine's account holds them"
)


def tenant() -> str | None:
    """DE_TENANT in the mode, else None."""
    return EXISTING.tenant if EXISTING is not None else None


async def connect_admin(**kwargs: Any) -> Any:
    """The harness's connection for the engine's streams (DE_ADMIN_URL, DE_ADMIN_CREDS; the
    tests' own by default)."""
    import nats

    assert EXISTING is not None
    creds = {"user_credentials": EXISTING.admin_creds} if EXISTING.admin_creds else {}
    return await nats.connect(EXISTING.admin_url, **creds, **kwargs)


def moving(payloads: float) -> int:
    """The bandwidth term, in ms, for a test whose large values move ``payloads`` x max_payload
    through ``put`` and ``get`` in all: those bytes over the throughput measured at the start, x
    HEADROOM (0 where none was measured). For a ``result()`` wait, which the harness scales by the
    round-trip factor itself: ``handle.result(30_000 + existing.moving(6))``."""
    if THROUGHPUT <= 0 or payloads <= 0:
        return 0
    return int(payloads * MAX_PAYLOAD * 1000 / THROUGHPUT * HEADROOM) + 1


async def measure_throughput(
    nc: Any, prefix: str, run_id: str, size: int = 4 << 20
) -> tuple[float, float, float]:
    """The probe: ``size`` random bytes up through the engine's ``put`` and back through its
    ``get`` on the tests' link. Returns (bytes per second up and down together, put s, get s)."""
    import time

    from synadia_durable._engine.blobs import EngineBlobs, Sizes
    from synadia_durable._engine.transport import Transport

    t = Transport(nc, prefix)
    blobs = EngineBlobs(t.request, Sizes(t, lambda: int(nc.max_payload)), rtt=nc.rtt)
    data = os.urandom(size)
    t0 = time.perf_counter()
    ref = await blobs.upload(run_id, data)
    t1 = time.perf_counter()
    back = await blobs.get(ref["name"])
    t2 = time.perf_counter()
    if len(back) != size:
        raise RuntimeError(f"the throughput probe: {ref['name']} came back {len(back)} bytes")
    return 2 * size / (t2 - t0), t1 - t0, t2 - t1


def _filter_of(prefix: str, stream: str) -> str:
    """The scope's subjects of ``stream``: all standalone; as a tenant its token (the three
    streams), its keys (the buckets), or all of the object store's (``_tenant_objects`` sorts them)."""
    t = tenant()
    if t is None or stream.startswith("OBJ_"):
        return ">"
    if stream.startswith("KV_"):
        return f"$KV.{stream[3:]}.{t}.>"
    return f"{prefix}.{t}.>"


def durable_of(prefix: str, name: str) -> bool:
    """A durable is the scope's: standalone the prefix's pair, ``{prefix}~worker`` and
    ``{prefix}~timer`` (the engine's F77); as a tenant one named ``{prefix}~{tenant}~…`` (its pair,
    F69)."""
    t = tenant()
    if t is None:
        return name in (f"{prefix}~worker", f"{prefix}~timer")
    return name.startswith(f"{prefix}~{t}~")


def _ephemeral_of(prefix: str, config: Any) -> bool:
    t = tenant()
    if t is None:
        return True
    filters = [config.filter_subject or "", *(getattr(config, "filter_subjects", None) or [])]
    return any(
        f.startswith(f"{prefix}.{t}.") or re.match(rf"^\$KV\.[^.]+\.{re.escape(t)}\.", f)
        for f in filters
        if f
    )


def rtt_factor(rtt_ms: float) -> float:
    """1 up to 20 ms (a local server), then one more for every 100 ms: 2.5 at the proxy's 150 ms.
    The fabric's test repo used a flat WANFactor of 2 on NGS; this one follows the link it
    measured."""
    if rtt_ms <= 20:
        return 1.0
    return round(1 + rtt_ms / 100, 2)


def scaled(seconds: float) -> float:
    return seconds * FACTOR


async def measure_rtt(nc: Any) -> float:
    """The median of five round trips to the server, in ms."""
    import time

    samples = []
    for _ in range(5):
        t0 = time.perf_counter()
        await nc.flush()
        samples.append((time.perf_counter() - t0) * 1000)
    return sorted(samples)[2]


OWN_SERVER = "kills and restarts a server of its own"
OWN_LINK = (
    "a proxy of its own between it and the server, to add round trips, cut or stall the link (a "
    "server the run does not own may be TLS, which a byte proxy cannot carry)"
)
DELETES_STREAMS = "deletes one of the prefix's streams"


def skip_in_mode(why: str) -> pytest.MarkDecorator:
    """A test the mode cannot run (it needs a server of its own), skipped with the reason."""
    return pytest.mark.skipif(in_mode(), reason=f"existing-server mode (DE_NATS_URL): {why}")


def prefix_streams(prefix: str) -> list[str]:
    """The prefix's eight backing streams (KV buckets and the object store are streams). The
    engine's record of the workflow names, ``KV_{P}_TENANTS`` (provisioned standalone too since its
    F77), is last, so the indexes into the list ([0] the journal, [:3] the three streams, [6] the
    blobs) are what they were."""
    up = prefix.upper()
    return [
        f"{up}_JOURNAL",
        f"{up}_WORK",
        f"{up}_TIMERS",
        f"KV_{up}_RUNS",
        f"KV_{up}_LEASES",
        f"KV_{up}_PARKED",
        f"OBJ_{up}_BLOBS",
        f"KV_{up}_TENANTS",
    ]


def _engine_record(prefix: str) -> str:
    """The engine's own record among the eight, never purged after a test: ``KV_{P}_TENANTS``
    caches the set of workflow names and is written by CAS."""
    return f"KV_{prefix.upper()}_TENANTS"


async def _consumers(jsm: Any, stream: str) -> list[Any]:
    from nats.js.errors import NotFoundError

    try:
        return list(await jsm.consumers_info(stream))
    except NotFoundError:
        return []


async def prefix_ephemeral_consumers(
    nc: Any, prefix: str, known: set[str] | frozenset[str] = frozenset()
) -> list[str]:
    """The consumers without a durable name on the prefix's streams: the leftovers check, scoped.
    Those in ``known`` (``stream/name``, there before the test or the run) are not the run's: on a
    prefix the run shares, another client's watch is not a leak of this one's."""
    jsm = nc.jsm()
    left: list[str] = []
    for stream in prefix_streams(prefix):
        for c in await _consumers(jsm, stream):
            if (
                not c.config.durable_name
                and f"{stream}/{c.name}" not in known
                and _ephemeral_of(prefix, c.config)
            ):
                left.append(f"{stream}/{c.name}")
    return left


@dataclass
class Snapshot:
    """Each of the prefix's streams: subject → message count; and the consumers, ``stream/name``."""

    subjects: dict[str, dict[str, int]]
    consumers: set[str]


async def _subjects(jsm: Any, stream: str, subjects_filter: str = ">") -> dict[str, int]:
    from nats.js.errors import NotFoundError

    try:
        si = await jsm.stream_info(stream, subjects_filter=subjects_filter)
    except NotFoundError:
        return {}
    return dict(si.state.subjects or {})


async def snapshot(nc: Any, prefix: str) -> Snapshot:
    """What the prefix holds before a test: the cleanup purges only what changed after it."""
    import asyncio

    jsm = nc.jsm()
    streams = prefix_streams(prefix)
    subjects = await asyncio.gather(*(_subjects(jsm, s, _filter_of(prefix, s)) for s in streams))
    consumers = await asyncio.gather(*(_consumers(jsm, s) for s in streams[:3]))
    return Snapshot(
        subjects=dict(zip(streams, subjects, strict=True)),
        consumers={
            f"{s}/{c.name}" for s, cs in zip(streams[:3], consumers, strict=True) for c in cs
        },
    )


async def purge_since(nc: Any, prefix: str, before: Snapshot) -> int:
    """The cleanup after a test: every subject of the prefix's streams that is new or grew since
    ``before`` is purged — the journal, work and timer subjects of the test's runs, their KV keys
    and their blobs' chunks and meta. Never a stream, never a subject outside the prefix, never one
    the test did not touch, never the engine's record of the names (``KV_{P}_TENANTS``: it caches
    the set and writes it by CAS).

    **No durable is deleted.** Until the engine's F77 it made two durables per workflow name, and
    the cleanup deleted the test's, which would otherwise have piled up past an account's
    per-stream consumer cap over the suites' ~150 names. Now the prefix has one pair
    (``{prefix}~worker``, ``{prefix}~timer``; a tenant's ``{prefix}~{tenant}~…``), bound once by the
    run's one engine at its first poll (so absent from the run's first snapshots): deleting it
    after a test would make the engine re-bind under its long pull (the 10012 store race and a
    0.5 s backoff, the engine's ``TestDurablesDeletedUnderTheEngine``). It goes only with the
    prefix's streams at the run's end."""
    import asyncio

    jsm = nc.jsm()
    purged = 0
    for stream in prefix_streams(prefix):
        if stream == _engine_record(prefix):
            continue
        was = before.subjects.get(stream, {})
        now = await _subjects(jsm, stream, _filter_of(prefix, stream))
        touched = [s for s, n in now.items() if was.get(s) != n]
        if tenant() is not None and stream.startswith("OBJ_"):
            # The object store's subjects carry no tenant: only the tenant's objects' metas and
            # chunks. A staged upload left half-way is the engine's to drop (--upload-ttl).
            mine = {
                x for _, meta, chunks in await _scope_objects(nc, prefix) for x in (meta, chunks)
            }
            touched = [s for s in touched if s in mine]
        await asyncio.gather(*(jsm.purge_stream(stream, subject=s) for s in touched))
        purged += len(touched)
    return purged


async def run_ids_since(nc: Any, prefix: str, before: Snapshot) -> list[str]:
    """The run ids whose journals a test touched: ``{prefix}.journal.<runId>`` new or grown."""
    stream = prefix_streams(prefix)[0]
    was = before.subjects.get(stream, {})
    t = tenant()
    head = f"{prefix}.journal." if t is None else f"{prefix}.{t}.journal."
    now = await _subjects(nc.jsm(), stream, _filter_of(prefix, stream))
    return [s[len(head) :] for s, n in now.items() if was.get(s) != n and s.startswith(head)]


def _of_runs(name: str, run_ids: set[str]) -> bool:
    """A blob of one of ``run_ids``: its name is ``{runId}/…`` (a staged upload part too); a
    child's or a generation's run id begins with its parent's and ``_``."""
    run = name.split("/", 1)[0] if "/" in name else ""
    if not run:
        return False
    if run in run_ids:
        return True
    return any(run[:i] in run_ids for i, ch in enumerate(run) if ch == "_" and i > 0)


async def _scope_objects(nc: Any, prefix: str) -> list[tuple[str, str, str]]:
    """The scope's objects in the prefix's blob store, from their meta records' last messages:
    (the name as the ref says it, the meta subject, the chunks' subject); every one standalone, as
    a tenant those named ``{tenant}/…`` (the tenant's token taken off the name)."""
    stream = prefix_streams(prefix)[6]
    bucket = stream[len("OBJ_") :]
    head = f"{tenant()}/" if tenant() is not None else ""
    inbox = nc.new_inbox()
    sub = await nc.subscribe(inbox)
    out: list[tuple[str, str, str]] = []
    try:
        await nc.publish(
            f"$JS.API.DIRECT.GET.{stream}",
            json.dumps({"multi_last": [f"$O.{bucket}.M.>"]}).encode(),
            reply=inbox,
        )
        while True:
            msg = await sub.next_msg(timeout=scaled(5.0))
            headers = msg.headers or {}
            if headers.get("Status") is not None:
                break  # 204 end of batch, 404 no objects or no stream
            info = json.loads(msg.data)
            name = info.get("name")
            if isinstance(name, str) and name.startswith(head):
                nuid = info.get("nuid") or ""
                chunks = f"$O.{bucket}.C.{nuid}" if nuid else ""
                out.append((name[len(head) :], headers.get("Nats-Subject") or "", chunks))
    finally:
        with contextlib.suppress(Exception):
            await sub.unsubscribe()
    return out


async def delete_run_blobs(nc: Any, prefix: str, run_ids: set[str] | frozenset[str]) -> int:
    """Delete the blobs of ``run_ids`` from the prefix's blob store by name: each object's meta
    record and its chunks, purged. By name, not by the snapshot's diff: a test that timed out
    leaves its worker and the engine writing after its cleanup (the dev NGS run left 17 blobs so),
    and those land in the next test's snapshot, out of every diff's reach (docs/findings.md F33).
    Returns how many objects were deleted."""
    import asyncio

    if not run_ids:
        return 0
    stream = prefix_streams(prefix)[6]
    doomed = [o for o in await _scope_objects(nc, prefix) if _of_runs(o[0], set(run_ids))]
    jsm = nc.jsm()
    await asyncio.gather(
        *(
            jsm.purge_stream(stream, subject=subject)
            for _, meta, chunks in doomed
            for subject in (meta, chunks)
            if subject
        )
    )
    return len(doomed)


async def sweep_run(nc: Any, prefix: str) -> int:
    """The run's end, after its engine stopped (nothing writes a blob then): the blobs of every run
    the run's tests touched, by name. What a test that failed or timed out left after its own
    cleanup is gone with them. The durables are not touched: the engine's pair is its own
    (``purge_since``). Returns the blobs deleted."""
    return await delete_run_blobs(nc, prefix, RUN_IDS)


async def delete_prefix_streams(nc: Any, prefix: str) -> list[str]:
    """``DE_CLEANUP=delete`` (the default for a generated prefix): the prefix's eight, at the
    run's end (the pair goes with its streams), and nothing else."""
    from nats.js.errors import NotFoundError

    jsm = nc.jsm()
    deleted: list[str] = []
    for name in prefix_streams(prefix):
        try:
            await jsm.delete_stream(name)
            deleted.append(name)
        except NotFoundError:
            pass
    return deleted
