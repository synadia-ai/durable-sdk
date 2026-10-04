"""Payload encryption at rest, end to end against a real nats-server.

The pure suite proves the transform and its guards. What only a real server proves is the claim an
operator cares about — run a workflow through a real worker and client, then read the journal the way
a principal with ``subscribe de.journal.>`` would, and find no secret in it — plus the behaviour
PROTOCOL §5.7 states as a MUST: a reader without the codec PARKS the run rather than failing it.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

import existing
from conftest import over_payload
from engine import create_client, create_worker
from shell.journal_store import NatsJournal
from shell.names import Names
from synadia_durable import (
    aes_gcm_payload_codec,
    workflow,
)
from synadia_durable.errors import PayloadCodecError
from synadia_durable.types import Ctx
from synadia_durable.version import WIRE_VERSION_BLOBS, WIRE_VERSION_ENCODED

pytestmark = pytest.mark.asyncio

KEY = bytes([42]) * 32
SECRET = "pan-4242424242424242"
# One resource set per CONFIGURATION, not per test: each prefix reserves max_bytes across six
# streams and the harness's JetStream store is a fixed budget shared by every integration prefix.
ENC = "pyenc"
PLAIN = "pyplain"


async def _raw_journal(nc: Any, prefix: str, run_id: str) -> list[dict[str, Any]]:
    """Every entry on a run's journal subject, as a reader of `de.journal.>` would see it — in its
    STORED form (refs and codec output untouched), through the library's direct-get reader so the
    sweep itself creates no consumer. It used to fetch through an ephemeral pull subscription the
    server expired after a few seconds: invisible to the end-of-run leftover check, caught by the
    per-test one (tests/integration/conftest.py)."""
    return [dict(le.entry) for le in await NatsJournal(nc, Names(prefix)).load_raw(run_id)]


class _Recorder:
    """The metrics seam is a protocol with on_event, not a callable."""

    def __init__(self) -> None:
        self.events: list[tuple[str, Any]] = []

    def on_event(self, name: str, fields: Any) -> None:
        self.events.append((name, fields))


@workflow(name="pyenc_charge")
async def pyenc_charge(ctx: Ctx, cart: dict[str, Any]) -> dict[str, Any]:
    auth = await ctx.run("authorize", lambda: f"auth-for-{cart['card']}")
    return {"auth": auth}


async def test_nothing_on_the_journal_contains_the_secret(nats_url: str) -> None:
    nc = await existing.connect(nats_url)
    worker = create_worker(nc, [pyenc_charge], prefix=ENC, payload_codec=aes_gcm_payload_codec(KEY))
    await worker.start()
    try:
        client = create_client(nc, prefix=ENC, payload_codec=aes_gcm_payload_codec(KEY))
        handle = await client.start(pyenc_charge, {"card": SECRET})
        # the workflow SEES plaintext — encryption is at rest, not in the programming model
        assert await handle.result(20_000) == {"auth": f"auth-for-{SECRET}"}

        raw = await _raw_journal(nc, ENC, handle.run_id)
        assert len(raw) > 2
        blob = json.dumps(raw)
        assert SECRET not in blob
        assert "auth-for-" not in blob
        # the envelope stays legible, which is what keeps replay and the dashboard working
        assert any(e["kind"] == "step-begin" for e in raw)
        assert any(e.get("name") == "authorize" for e in raw)
        assert any(e.get("v") == WIRE_VERSION_ENCODED and "inputEnc" in e for e in raw)
        assert any(e.get("v") == WIRE_VERSION_ENCODED and "valueEnc" in e for e in raw)

        # …and the status KV, the weakest read right we hand out, must not hold the value either
        kv = await nc.jetstream().key_value(Names(ENC).runs_kv)
        raw_hint = (await kv.get(handle.run_id)).value
        assert raw_hint is not None
        hint = json.loads(raw_hint)
        assert hint["status"] == "completed"  # the shape of the run is still visible
        assert "result" not in hint  # the value is not
    finally:
        await worker.stop()
        await nc.drain()


async def test_control_without_the_codec_the_same_run_leaks(nats_url: str) -> None:
    """Without this, the test above proves nothing — a typo'd secret would also "not appear"."""
    nc = await existing.connect(nats_url)
    worker = create_worker(nc, [pyenc_charge], prefix=PLAIN)
    await worker.start()
    try:
        client = create_client(nc, prefix=PLAIN)
        handle = await client.start(pyenc_charge, {"card": SECRET})
        await handle.result(20_000)
        blob = json.dumps(await _raw_journal(nc, PLAIN, handle.run_id))
        assert SECRET in blob
        assert "auth-for-" in blob
        kv = await nc.jetstream().key_value(Names(PLAIN).runs_kv)
        raw_hint = (await kv.get(handle.run_id)).value
        assert raw_hint is not None
        # the status KV leaks the result too, without a codec
        assert "auth-for-" in raw_hint.decode()
    finally:
        await worker.stop()
        await nc.drain()


async def test_a_worker_without_the_codec_parks_and_the_run_heals(nats_url: str) -> None:
    """PROTOCOL §5.7 states this as a MUST, and parking rather than failing is the whole point.

    Asserts both halves: nothing terminal is written while the misconfigured worker holds the item,
    and a correctly-configured worker then finishes the very same run.
    """
    nc = await existing.connect(nats_url)
    rec = _Recorder()

    # A client WITH the codec writes a v:3 start entry…
    client = create_client(nc, prefix=ENC, payload_codec=aes_gcm_payload_codec(KEY))
    handle = await client.start(pyenc_charge, {"card": SECRET})

    # …which a worker WITHOUT it cannot read.
    blind = create_worker(
        nc,
        [pyenc_charge],
        prefix=ENC,
        metrics=rec,
    )
    await blind.start()
    try:
        for _ in range(300):
            if any(n == "work.done" and f.get("outcome") == "payload-codec" for n, f in rec.events):
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail(f"the worker never reported a payload-codec park; saw {rec.events[-5:]}")

        kinds = [e["kind"] for e in await _raw_journal(nc, ENC, handle.run_id)]
        assert "start" in kinds
        assert "result" not in kinds
        assert "fail" not in kinds  # parked, NOT failed — the run is still recoverable
        assert (await handle.status()).state != "failed"
    finally:
        await blind.stop()

    # It heals the moment a worker with the codec picks the run up.
    good = create_worker(nc, [pyenc_charge], prefix=ENC, payload_codec=aes_gcm_payload_codec(KEY))
    await good.start()
    try:
        assert await handle.result(20_000) == {"auth": f"auth-for-{SECRET}"}
    finally:
        await good.stop()
        await nc.drain()


async def test_a_client_without_the_codec_fails_loud_instead_of_hanging(nats_url: str) -> None:
    """A client that cannot decode never heals by waiting; timing out would hide a config mistake."""
    nc = await existing.connect(nats_url)
    worker = create_worker(nc, [pyenc_charge], prefix=ENC, payload_codec=aes_gcm_payload_codec(KEY))
    await worker.start()
    try:
        good = create_client(nc, prefix=ENC, payload_codec=aes_gcm_payload_codec(KEY))
        handle = await good.start(pyenc_charge, {"card": SECRET})
        await handle.result(20_000)

        blind = create_client(nc, prefix=ENC)
        with pytest.raises(PayloadCodecError):
            await blind.get(handle.run_id).result(5_000)
    finally:
        await worker.stop()
        await nc.drain()


# ------------------------------------------------------------------ the work-item boundary
#
# A signal's data and a child's outcome ride ``de.work.*`` as work items, and an oversized one is
# offloaded to ``DE_BLOBS`` by ``shrink_work_item`` BEFORE any journal append exists to encrypt it.
# The item itself is plaintext by design — workqueue retention, consumed and gone — but the BLOB is
# not: it sits in the bucket until GC, which is exactly the "at rest" the codec exists for.


def _big_secret(nc: Any) -> str:
    """Twice THIS server's max_payload, so the work item MUST offload to be publishable at all."""
    return f"{SECRET}-{'s' * over_payload(nc, 2)}"


async def _run_blobs(nc: Any, prefix: str, run_id: str) -> list[bytes]:
    """Every blob this run owns — what a reader of the DE_BLOBS bucket would see."""
    from conftest import list_object_names, read_blob

    bucket = Names(prefix).blobs_bucket
    return [
        await read_blob(nc, bucket, name)
        for name in await list_object_names(nc, bucket)
        if name.startswith(f"{run_id}/")
    ]


@workflow(name="pyenc_bigsig")
async def pyenc_bigsig(ctx: Ctx, _input: Any) -> int:
    data = await ctx.wait("card")
    return len(str(data))


@workflow(name="pyplain_bigsig")
async def pyplain_bigsig(ctx: Ctx, _input: Any) -> int:
    data = await ctx.wait("card")
    return len(str(data))


async def _park_then_signal(nc: Any, prefix: str, wf: Any, codec: Any, data: str) -> Any:
    """Start a run, wait for it to park on its ``wait``, then deliver one oversized signal."""
    kwargs = {"payload_codec": codec} if codec is not None else {}
    client = create_client(nc, prefix=prefix, **kwargs)
    handle = await client.start(wf, {})
    for _ in range(300):
        if (await handle.status()).state == "suspended":
            break
        await asyncio.sleep(0.05)
    await client.signal(handle.run_id, "card", data)
    return handle


# durable-sdk: docs/findings.md F1/F2, answered by the engine's F25 (put and get) and F22
# (serve --offload-threshold), with the SDK's chunked transport (_engine/blobs.py).
async def test_a_large_signals_blob_holds_ciphertext_too(nats_url: str) -> None:
    """Encryption cannot depend on payload SIZE — nothing in the model or the journal says it does."""
    nc = await existing.connect(nats_url)
    codec = aes_gcm_payload_codec(KEY)
    worker = create_worker(nc, [pyenc_bigsig], prefix=ENC, payload_codec=codec)
    await worker.start()
    try:
        big = _big_secret(nc)
        handle = await _park_then_signal(nc, ENC, pyenc_bigsig, codec, big)
        # The workflow still sees plaintext: this is encryption at rest, not in the model.
        assert await handle.result(30_000) == len(big)

        blobs = await _run_blobs(nc, ENC, handle.run_id)
        assert len(blobs) == 1
        assert SECRET.encode() not in blobs[0]

        # …and the entry must SAY the blob is transformed (v:3), or a reader hands the workflow
        # ciphertext instead of decrypting it.
        signal = next(
            e for e in await _raw_journal(nc, ENC, handle.run_id) if e["kind"] == "signal"
        )
        assert signal["v"] == WIRE_VERSION_ENCODED
        assert "data" not in signal
        assert signal["dataRef"]["name"].startswith(f"{handle.run_id}/sha256:")
    finally:
        await worker.stop()
        await nc.drain()


# durable-sdk: docs/findings.md F1/F2, answered by the engine's F25 (put and get) and F22
# (serve --offload-threshold), with the SDK's chunked transport (_engine/blobs.py).
async def test_control_without_a_codec_the_same_signal_lands_in_the_clear(nats_url: str) -> None:
    """Proves the assertion above is load-bearing, not a secret that never reached the bucket."""
    nc = await existing.connect(nats_url)
    worker = create_worker(nc, [pyplain_bigsig], prefix=PLAIN)
    await worker.start()
    try:
        big = _big_secret(nc)
        handle = await _park_then_signal(nc, PLAIN, pyplain_bigsig, None, big)
        assert await handle.result(30_000) == len(big)

        blobs = await _run_blobs(nc, PLAIN, handle.run_id)
        assert len(blobs) == 1
        assert SECRET.encode() in blobs[0]

        signal = next(
            e for e in await _raw_journal(nc, PLAIN, handle.run_id) if e["kind"] == "signal"
        )
        # v:2, and the same field shape the TypeScript worker writes for this case.
        assert signal["v"] == WIRE_VERSION_BLOBS
        assert "dataRef" in signal
    finally:
        await worker.stop()
        await nc.drain()
