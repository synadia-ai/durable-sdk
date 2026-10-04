"""The harness's mode for an EXISTING server (docs/findings.md F28, F29): ``DE_NATS_URL`` names a
server the run does not own, instead of the ``nats-server`` the harness spawns. The twin of
``test/harness/existing.ts``.

    DE_NATS_URL     the server (a URL the client and the engine both reach); switches the mode on
    DE_NATS_CREDS   a creds file for it, when it needs one (read by nats-py and the engine's
                    ``--creds``, never by this harness)
    DE_PREFIX       the run's one prefix (default a generated ``de-<run id>``)
    DE_ENGINE_URL   an engine that already serves the prefix, by its /metrics address
                    (``http://host:port``); without it the harness starts one for the run
    DE_CLEANUP      ``delete``: delete the prefix's seven at the run's end (default: keep them)

No spawn, no account profile (the server's own limits apply), one prefix and one engine for the
run, a cleanup that purges what each test wrote under the prefix and never deletes a stream, the
leftovers check on the prefix's streams only, and every wait scaled by the measured round trip.
"""

from __future__ import annotations

import contextlib
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
    delete_at_end: bool


def _from_env() -> ExistingServer | None:
    url = os.environ.get("DE_NATS_URL", "")
    if not url:
        return None
    cleanup = os.environ.get("DE_CLEANUP", "")
    if cleanup not in ("", "delete", "keep"):
        raise ValueError(f'DE_CLEANUP="{cleanup}": expected delete or keep')
    prefix = os.environ.get("DE_PREFIX") or f"de-{secrets.token_hex(4)}"
    if not re.fullmatch(r"[A-Za-z0-9_-]+", prefix):
        raise ValueError(f'DE_PREFIX="{prefix}": one subject token, no dots')
    return ExistingServer(
        url=url,
        creds=os.environ.get("DE_NATS_CREDS") or None,
        prefix=prefix,
        engine_url=os.environ.get("DE_ENGINE_URL") or None,
        delete_at_end=cleanup == "delete",
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
    """The prefix's seven backing streams (KV buckets and the object store are streams)."""
    up = prefix.upper()
    return [
        f"{up}_JOURNAL",
        f"{up}_WORK",
        f"{up}_TIMERS",
        f"KV_{up}_RUNS",
        f"KV_{up}_LEASES",
        f"KV_{up}_PARKED",
        f"OBJ_{up}_BLOBS",
    ]


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
            if not c.config.durable_name and f"{stream}/{c.name}" not in known:
                left.append(f"{stream}/{c.name}")
    return left


@dataclass
class Snapshot:
    """Each of the prefix's streams: subject → message count; and the consumers, ``stream/name``."""

    subjects: dict[str, dict[str, int]]
    consumers: set[str]


async def _subjects(jsm: Any, stream: str) -> dict[str, int]:
    from nats.js.errors import NotFoundError

    try:
        si = await jsm.stream_info(stream, subjects_filter=">")
    except NotFoundError:
        return {}
    return dict(si.state.subjects or {})


async def snapshot(nc: Any, prefix: str) -> Snapshot:
    """What the prefix holds before a test: the cleanup purges only what changed after it."""
    import asyncio

    jsm = nc.jsm()
    streams = prefix_streams(prefix)
    subjects = await asyncio.gather(*(_subjects(jsm, s) for s in streams))
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
    the test did not touch. And the durables the engine made for the test's workflow names: they
    would pile up past an account's per-stream consumer cap; one that was there before stays."""
    import asyncio

    from nats.js.errors import NotFoundError

    jsm = nc.jsm()
    purged = 0
    for stream in prefix_streams(prefix):
        was = before.subjects.get(stream, {})
        now = await _subjects(jsm, stream)
        touched = [s for s, n in now.items() if was.get(s) != n]
        await asyncio.gather(*(jsm.purge_stream(stream, subject=s) for s in touched))
        purged += len(touched)
    for stream in prefix_streams(prefix)[:3]:
        fresh = [
            c.name
            for c in await _consumers(jsm, stream)
            if f"{stream}/{c.name}" not in before.consumers
        ]

        async def drop(stream: str, name: str) -> None:
            with contextlib.suppress(NotFoundError):
                await jsm.delete_consumer(stream, name)

        await asyncio.gather(*(drop(stream, n) for n in fresh))
    return purged


async def delete_prefix_streams(nc: Any, prefix: str) -> list[str]:
    """``DE_CLEANUP=delete``: the prefix's seven, at the run's end, and nothing else."""
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
