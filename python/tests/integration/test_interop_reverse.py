"""**The other direction**: a TYPESCRIPT client driving a PYTHON worker.

``test_interop.py`` proved the Python client speaks the protocol. This proves the Python *worker*
does — as judged by the reference implementation, not by its own tests. Between the two, both roles
are verified against the other SDK.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
from pathlib import Path
from typing import Any, cast

import pytest

import existing
from conftest import over_payload
from engine import create_worker
from shell.journal_store import NatsJournal
from shell.names import Names
from synadia_durable import (
    aes_gcm_payload_codec,
    define_workflow,
    workflow,
)
from synadia_durable.journal.blobs import OffloadOptions
from synadia_durable.types import Ctx

pytestmark = pytest.mark.asyncio

REPO = Path(__file__).resolve().parents[3]
CLIENT = REPO / "conformance" / "interop" / "ts-client.ts"
MB = 1024 * 1024
CLIENT_TIMEOUT_S = 90.0


@pytest.fixture(scope="session")
def _node_available() -> None:
    if shutil.which("npx") is None:
        pytest.skip("npx not on PATH — the TypeScript interop client cannot be started")
    if not (REPO / "node_modules").is_dir():
        pytest.skip("node_modules absent — run `bun install` at the repo root for interop tests")


async def run_ts_client(
    nats_url: str,
    prefix: str,
    workflow: str,
    run_id: str,
    payload: Any = None,
    *,
    signal: tuple[str, Any] | None = None,
    signal_pad: int | None = None,
    payload_key: bytes | None = None,
) -> str:
    """Drive a run from the TypeScript client and return the single line it prints."""
    env = {**os.environ, "NATS_URL": nats_url, "DE_PREFIX": existing.map_prefix(prefix)}
    if payload_key is not None:
        env["DE_PAYLOAD_KEY"] = payload_key.hex()
    if signal is not None:
        env["DE_SIGNAL"] = signal[0]
        env["DE_SIGNAL_DATA"] = json.dumps(signal[1])
    if signal_pad is not None:
        # The client pads the payload itself: an environment variable cannot carry the megabytes it
        # takes to push a work item over max_payload, which is the case worth testing.
        env["DE_SIGNAL_PAD"] = str(signal_pad)
    proc = await asyncio.create_subprocess_exec(
        "npx",
        "tsx",
        str(CLIENT),
        workflow,
        run_id,
        json.dumps(payload if payload is not None else {}),
        cwd=str(REPO),
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), CLIENT_TIMEOUT_S)
    except TimeoutError:
        proc.kill()
        pytest.fail("the TypeScript client did not finish in time")
    if proc.returncode != 0:
        pytest.fail(f"TypeScript client failed: {stderr.decode()[:2000]}")
    return stdout.decode().strip().splitlines()[-1]


@pytest.fixture
async def python_worker(_node_available: None, nc: Any, prefix: str) -> Any:
    """A Python worker running the workflows the TypeScript client will address."""

    async def greet(ctx: Ctx, order: dict[str, Any]) -> dict[str, Any]:
        shouted = await ctx.run("shout", lambda: str(order["name"]).upper())
        return {"greeting": f"HELLO {shouted}", "from": "python"}

    async def approve(ctx: Ctx, order: dict[str, Any]) -> dict[str, Any]:
        decision = await ctx.wait("decision")
        return {"order": order["order"], "approved": decision["ok"]}

    async def boom(_ctx: Ctx, _input: Any) -> None:
        raise RuntimeError("the python worker refused it")

    async def bulky(ctx: Ctx, _input: Any) -> dict[str, Any]:
        # Over the tiny threshold below, so its step value and terminal are OFFLOADED: what reaches
        # the TypeScript client is a v:2 entry with a content-addressed ref that only the reference
        # implementation's own resolver can turn back into this dict.
        made = await ctx.run("make", lambda: {"frames": ["y" * 400] * 8, "from": "python"})
        return cast("dict[str, Any]", made)

    worker = create_worker(
        nc,
        [
            define_workflow("rgreet", greet),
            define_workflow("rapprove", approve),
            define_workflow("rboom", boom),
            define_workflow("rbulky", bulky),
        ],
        prefix=prefix,
        stream_max_bytes=64 * MB,
        offload=OffloadOptions(threshold_bytes=1),
    )
    await worker.start()
    try:
        yield worker
    finally:
        with contextlib.suppress(Exception):
            await worker.stop()


async def test_a_typescript_client_runs_a_python_workflow(
    python_worker: Any, nats_url: str, prefix: str
) -> None:
    _ = python_worker
    line = await run_ts_client(nats_url, prefix, "rgreet", "rev-1", {"name": "grace"})
    assert line.startswith("RESULT ")
    result = json.loads(line.removeprefix("RESULT "))
    assert result == {"greeting": "HELLO GRACE", "from": "python"}


async def test_a_typescript_signal_wakes_a_parked_python_run(
    python_worker: Any, nats_url: str, prefix: str
) -> None:
    _ = python_worker
    line = await run_ts_client(
        nats_url,
        prefix,
        "rapprove",
        "rev-2",
        {"order": "ord-9"},
        signal=("decision", {"ok": True}),
    )
    assert line.startswith("RESULT ")
    assert json.loads(line.removeprefix("RESULT ")) == {"order": "ord-9", "approved": True}


async def test_a_python_failure_surfaces_to_the_typescript_client(
    python_worker: Any, nats_url: str, prefix: str
) -> None:
    _ = python_worker
    line = await run_ts_client(nats_url, prefix, "rboom", "rev-3", {})
    # A terminal `fail` written by Python, read and raised by the reference implementation.
    assert line.startswith("FAILED ")
    assert "refused it" in line


# durable-sdk: F1/F2, answered by the engine's F25 (put and get) and F22
# (serve --offload-threshold), with the SDK's chunked transport (_engine/blobs.py).
async def test_typescript_reads_a_payload_python_offloaded(
    python_worker: Any, nc: Any, nats_url: str, prefix: str
) -> None:
    """The wire contract for large payloads, judged by the reference implementation.

    Python writes the blob, the digest and the ``v:2`` entry; TypeScript resolves them with its own
    code. Nothing in this repo's Python tests could catch a divergence in the ref shape, the digest
    format or the object name — only the other SDK reading it can, which is the entire reason the
    two SDKs live in one repo.
    """
    _ = python_worker
    line = await run_ts_client(nats_url, prefix, "rbulky", "rev-bulky-1", {})
    assert line.startswith("RESULT ")
    result = json.loads(line.removeprefix("RESULT "))
    assert result["from"] == "python"
    assert result["frames"] == ["y" * 400] * 8

    # Self-check, because this test would pass vacuously if nothing had been offloaded: the entry
    # TypeScript just resolved has to actually be a v:2 ref on the stream.
    raw = await NatsJournal(nc, Names(prefix)).load_raw("rev-bulky-1")
    assert any("valueRef" in dict(le.entry) for le in raw), "nothing was offloaded here"


@pytest.mark.usefixtures("_node_available")
async def test_typescript_reads_a_payload_python_encrypted(nats_url: str) -> None:
    """The two SDKs' AES-GCM payload codecs are wire-compatible.

    A Python WORKER encrypts every payload slot; the reference TypeScript client, given the same
    key, starts the run and reads the result back. This is the cross-language half of the feature —
    a codec that only round-trips within one SDK would split a mixed fleet in two, and the failure
    would be a permanently parked run rather than anything loud at deploy time.

    Self-checking: it asserts the journal really is v:3 with a `*Enc` slot, so it cannot pass
    vacuously if encryption silently did nothing.
    """
    key = bytes(range(32))
    prefix = "ipenc"
    nc = await existing.connect(nats_url)

    @workflow(name="enc_interop")
    async def enc_interop(ctx: Ctx, cart: dict[str, Any]) -> dict[str, Any]:
        masked = await ctx.run("mask", lambda: f"masked:{str(cart['card'])[-4:]}")
        return {"masked": masked}

    worker = create_worker(
        nc, [enc_interop], prefix=prefix, payload_codec=aes_gcm_payload_codec(key)
    )
    await worker.start()
    try:
        run_id = "enc-interop-1"
        line = await run_ts_client(
            nats_url,
            prefix,
            "enc_interop",
            run_id,
            {"card": "4242424242424242"},
            payload_key=key,
        )
        assert line == 'RESULT {"masked":"masked:4242"}', line

        # …and it really was encrypted: read the raw journal and check for v:3 + no plaintext —
        # through the library's direct-get reader, so the sweep itself creates no consumer (the
        # ephemeral pull subscription this replaced expired server-side before the end-of-run
        # leftover check could see it; the per-test check did).
        raw = [dict(le.entry) for le in await NatsJournal(nc, Names(prefix)).load_raw(run_id)]
        assert raw, "no journal entries were read back"
        assert any(e.get("v") == 3 and "inputEnc" in e for e in raw), raw
        assert "4242424242424242" not in json.dumps(raw)
    finally:
        await worker.stop()
        await nc.drain()


# durable-sdk: F1/F2, answered by the engine's F25 (put and get) and F22
# (serve --offload-threshold), with the SDK's chunked transport (_engine/blobs.py).
@pytest.mark.usefixtures("_node_available")
async def test_a_python_worker_honours_the_payload_enc_flag_typescript_wrote(
    nats_url: str,
) -> None:
    """The cross-language half of the work-item boundary (PROTOCOL §6.1, §5.7).

    A TypeScript CLIENT sends a signal too large to publish inline, so TypeScript writes the blob,
    transforms it with its codec, and marks the item ``payloadEnc``. The Python worker has to read
    that flag and stamp the entry ``v:3`` — if it stamped ``v:2`` instead, it would hand the
    workflow ciphertext, and if it ignored the ref it would hand it nothing.

    Self-checking twice over: the entry must really be ``v:3`` with a ref, and the blob TypeScript
    wrote must not contain the secret. Without both, a run that simply completed would "pass".
    """
    key = bytes(range(32))
    prefix = "ipencbig"
    secret = "pan-4242424242424242"
    nc = await existing.connect(nats_url)

    @workflow(name="enc_bigsig")
    async def enc_bigsig(ctx: Ctx, _input: Any) -> int:
        data = await ctx.wait("card")
        return len(str(data))

    worker = create_worker(
        nc, [enc_bigsig], prefix=prefix, payload_codec=aes_gcm_payload_codec(key)
    )
    await worker.start()
    try:
        run_id = "enc-bigsig-1"
        size = over_payload(nc, 2)
        line = await run_ts_client(
            nats_url,
            prefix,
            "enc_bigsig",
            run_id,
            {},
            signal=("card", secret),
            signal_pad=size,
            payload_key=key,
        )
        assert line == f"RESULT {size}", line

        raw = await NatsJournal(nc, Names(prefix)).load_raw(run_id)
        signal_entry = next(dict(le.entry) for le in raw if le.entry["kind"] == "signal")
        assert signal_entry["v"] == 3, signal_entry  # not v:2 — the blob is transformed
        assert "dataRef" in signal_entry, "nothing was offloaded — this proved nothing"
        assert "data" not in signal_entry

        from conftest import read_blob

        blob = await read_blob(
            nc, Names(prefix).blobs_bucket, cast("dict[str, Any]", signal_entry["dataRef"])["name"]
        )
        assert secret.encode() not in blob
    finally:
        await worker.stop()
        await nc.drain()
