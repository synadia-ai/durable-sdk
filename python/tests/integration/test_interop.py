"""**The cross-language proof**: a PYTHON client driving a TYPESCRIPT worker.

Nothing is shared between the two processes except the NATS streams and ``docs/PROTOCOL.md``. No
generated code, no shared library, no bridge. If the spec is a real contract, this passes; if it is a
description of some TypeScript, it does not.

Skipped unless both a ``nats-server`` and the repo's Node dependencies are present.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import shutil
from pathlib import Path
from typing import Any

import pytest

import engine
from conftest import over_payload, read_blob
from synadia_durable import CancelledError, WorkflowFailure, create_client, define_workflow
from synadia_durable.errors import DurableTimeoutError

pytestmark = pytest.mark.asyncio

REPO = Path(__file__).resolve().parents[3]
WORKER = REPO / "conformance" / "interop" / "ts-worker.ts"
WORKER_READY_TIMEOUT_S = 60.0


async def _runs_in_typescript(_ctx: Any, _input: Any) -> Any:
    """The body lives in the OTHER SDK.

    A client addresses a workflow by ``(name, version)`` — it never needs the code, which is the
    whole reason this test can exist. Raising makes that explicit: if a Python worker ever picked one
    of these up, we would want to know immediately rather than get a confusing run.
    """
    raise AssertionError("this workflow's body is implemented in the TypeScript worker")


# Matching names and versions, NO shared code — exactly how a polyglot deployment addresses a
# workflow that lives in another runtime.
greet = define_workflow("greet", _runs_in_typescript)
approve = define_workflow("approve", _runs_in_typescript)
boom = define_workflow("boom", _runs_in_typescript)


@pytest.fixture(scope="session")
def _node_available() -> None:
    if shutil.which("npx") is None:
        pytest.skip("npx not on PATH — the TypeScript interop worker cannot be started")
    if not (REPO / "node_modules").is_dir():
        pytest.skip("node_modules absent — run `bun install` at the repo root for interop tests")


@pytest.fixture
async def ts_worker(_node_available: None, nats_url: str, prefix: str) -> Any:
    """Spawn the TypeScript worker and wait until it says READY."""
    await engine.ensure_engine(prefix)  # durable-sdk: the prefix's engine, before the worker polls
    env = {**os.environ, "NATS_URL": nats_url, "DE_PREFIX": prefix}
    proc = await asyncio.create_subprocess_exec(
        "npx",
        "tsx",
        str(WORKER),
        cwd=str(REPO),
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        try:
            line = await asyncio.wait_for(proc.stdout.readline(), WORKER_READY_TIMEOUT_S)
        except TimeoutError:
            pytest.fail("the TypeScript worker never printed READY")
        if b"READY" not in line:
            stderr = b""
            if proc.stderr is not None:
                with contextlib.suppress(TimeoutError):
                    stderr = await asyncio.wait_for(proc.stderr.read(4000), 5)
            pytest.fail(f"TypeScript worker failed to start: {line!r} {stderr!r}")
        yield proc
    finally:
        proc.terminate()
        with contextlib.suppress(TimeoutError, ProcessLookupError):
            await asyncio.wait_for(proc.wait(), 10)


@pytest.fixture
async def client(nc: Any, prefix: str, ts_worker: Any) -> Any:
    _ = ts_worker  # the worker must be up before the client provisions
    return create_client(nc, prefix=prefix)


async def test_a_python_client_runs_a_typescript_workflow(client: Any) -> None:
    handle = await client.start(greet, {"name": "ada"})
    result = await handle.result(20_000)

    assert result["greeting"] == "HELLO ADA"
    assert result["from"] == "typescript"
    # ctx.now() is journaled by the worker, so the value crossing back is a recorded one.
    assert isinstance(result["at"], int)


async def test_status_reports_the_terminal_state(client: Any) -> None:
    handle = await client.start(greet, {"name": "grace"})
    await handle.result(20_000)

    status = await handle.status()
    assert status.state == "completed"
    assert status.workflow == "greet"
    assert status.version == 1


async def test_start_is_idempotent_on_run_id(client: Any) -> None:
    from synadia_durable import StartOptions

    opts = StartOptions(run_id="interop-idem-1")
    first = await client.start(greet, {"name": "ada"}, opts)
    # A second start with the same runId must not create a second run, nor corrupt the first: the
    # runId IS the idempotency key, and the start entry is both deduplicated and CAS-fenced.
    second = await client.start(greet, {"name": "SOMEONE ELSE"}, opts)

    assert first.run_id == second.run_id == "interop-idem-1"
    assert (await second.result(20_000))["greeting"] == "HELLO ADA"


async def test_a_python_signal_wakes_a_parked_typescript_run(client: Any) -> None:
    handle = await client.start(approve, {"order": "ord-1"})

    # Wait for the worker to park on ctx.await before signalling — not required for correctness (the
    # signal is journaled either way), but it proves the run really suspended rather than racing.
    await _wait_for_state(handle, "suspended")

    await client.signal(handle.run_id, "decision", {"ok": True})
    assert await handle.result(20_000) == {"order": "ord-1", "approved": True}


async def test_a_typescript_failure_surfaces_as_a_python_exception(client: Any) -> None:
    handle = await client.start(boom, {})
    with pytest.raises(WorkflowFailure, match="carrier rejected"):
        await handle.result(20_000)
    assert (await handle.status()).state == "failed"


async def test_a_python_cancel_hard_stops_a_typescript_run(client: Any) -> None:
    handle = await client.start(approve, {"order": "ord-2"})
    await _wait_for_state(handle, "suspended")

    await client.cancel(handle.run_id, reason="customer withdrew")
    with pytest.raises(CancelledError):
        await handle.result(20_000)
    assert (await handle.status()).state == "cancelled"


async def test_a_handle_can_be_rebuilt_from_nothing_but_the_run_id(client: Any) -> None:
    # The client holds no run state — everything is on the streams. A handle is just an id.
    started = await client.start(greet, {"name": "linus"})
    await started.result(20_000)

    rebuilt = client.get(started.run_id)
    assert (await rebuilt.result(20_000))["greeting"] == "HELLO LINUS"


async def test_result_times_out_rather_than_hanging_forever(client: Any) -> None:
    handle = await client.start(approve, {"order": "ord-3"})
    with pytest.raises(DurableTimeoutError):
        await handle.result(1_500)


async def _wait_for_state(handle: Any, state: str, timeout_s: float = 20.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        if (await handle.status()).state == state:
            return
        await asyncio.sleep(0.1)
    raise AssertionError(f"run {handle.run_id} never reached {state}")


# ------------------------------------------------------- the same, with encryption at rest on
#
# `test_interop_reverse.py` proves a TypeScript CLIENT reads what a Python worker encrypted. This is
# the other half: a TypeScript WORKER encrypting, read by a Python client. Both halves are needed —
# the two SDKs could agree on a format that neither writes the way the spec says.

MB = 1024 * 1024
PAYLOAD_KEY = bytes(range(32))
SECRET = "pan-4242424242424242"

echo_signal = define_workflow("echo_signal", _runs_in_typescript)


@pytest.fixture
async def ts_worker_encrypted(_node_available: None, nats_url: str, prefix: str) -> Any:
    """The same worker, with `DE_PAYLOAD_KEY` set — so it transforms every payload it journals."""
    await engine.ensure_engine(prefix)  # durable-sdk: the prefix's engine, before the worker polls
    env = {
        **os.environ,
        "NATS_URL": nats_url,
        "DE_PREFIX": prefix,
        "DE_PAYLOAD_KEY": PAYLOAD_KEY.hex(),
    }
    proc = await asyncio.create_subprocess_exec(
        "npx",
        "tsx",
        str(WORKER),
        cwd=str(REPO),
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        try:
            line = await asyncio.wait_for(proc.stdout.readline(), WORKER_READY_TIMEOUT_S)
        except TimeoutError:
            pytest.fail("the encrypted TypeScript worker never printed READY")
        assert b"READY" in line, line
        yield proc
    finally:
        proc.terminate()
        with contextlib.suppress(TimeoutError, ProcessLookupError):
            await asyncio.wait_for(proc.wait(), 10)


async def _raw_entries(nc: Any, prefix: str, run_id: str) -> list[dict[str, Any]]:
    from shell.journal_store import NatsJournal  # durable-sdk: the test's own reader (F13)
    from shell.names import Names

    return [dict(le.entry) for le in await NatsJournal(nc, Names(prefix)).load_raw(run_id)]


async def test_a_python_client_reads_what_a_typescript_worker_encrypted(
    nc: Any, prefix: str, ts_worker_encrypted: Any
) -> None:
    """The forward half of the codec's cross-language contract.

    Self-checking: it asserts the journal really is v:3 with a `*Enc` slot and holds no plaintext,
    so it cannot pass by encryption having quietly done nothing.
    """
    from synadia_durable import aes_gcm_payload_codec

    _ = ts_worker_encrypted
    client = create_client(nc, prefix=prefix, payload_codec=aes_gcm_payload_codec(PAYLOAD_KEY))
    handle = await client.start(greet, {"name": SECRET})
    result = await handle.result(20_000)
    assert result["greeting"] == f"HELLO {SECRET.upper()}"

    raw = await _raw_entries(nc, prefix, handle.run_id)
    assert any(e.get("v") == 3 and "inputEnc" in e for e in raw), raw
    assert SECRET not in str(raw)
    assert SECRET.upper() not in str(raw)


async def test_a_python_client_without_the_key_fails_loud_against_a_typescript_worker(
    nc: Any, prefix: str, ts_worker_encrypted: Any
) -> None:
    """A misconfigured reader must not hang until a timeout, in either language."""
    from synadia_durable import aes_gcm_payload_codec
    from synadia_durable.errors import PayloadCodecError

    _ = ts_worker_encrypted
    good = create_client(nc, prefix=prefix, payload_codec=aes_gcm_payload_codec(PAYLOAD_KEY))
    handle = await good.start(greet, {"name": "ada"})
    await handle.result(20_000)

    blind = create_client(nc, prefix=prefix)
    with pytest.raises(PayloadCodecError):
        await blind.get(handle.run_id).result(5_000)


# durable-sdk: BLOCKED by docs/findings.md F1 (API v1 has no chunked upload: a value over
# max_payload − 16 KiB cannot reach the engine, where the library offloaded it). The signal here is
# twice max_payload by construction.
@pytest.mark.skip(
    reason="durable-sdk: BLOCKED by docs/findings.md F1 (no chunked upload in API v1)"
)
async def test_a_typescript_worker_honours_the_payload_enc_flag_python_wrote(
    nc: Any, prefix: str, ts_worker_encrypted: Any
) -> None:
    """The work-item boundary across languages, with Python as the writer (PROTOCOL §6.1, §5.7).

    The Python client sends a signal too large to publish inline: Python writes the blob, transforms
    it, and marks the item ``payloadEnc``. The TypeScript worker must read that flag and stamp the
    entry ``v:3`` — stamping ``v:2`` would hand the workflow ciphertext instead of the value.
    """
    from shell.names import Names
    from synadia_durable import aes_gcm_payload_codec

    _ = ts_worker_encrypted
    client = create_client(nc, prefix=prefix, payload_codec=aes_gcm_payload_codec(PAYLOAD_KEY))
    handle = await client.start(echo_signal, {})
    await _wait_for_state(handle, "suspended")

    big = SECRET + "s" * over_payload(nc, 2)
    await client.signal(handle.run_id, "card", big)
    assert await handle.result(30_000) == {"len": len(big), "head": big[:24]}

    signal = next(e for e in await _raw_entries(nc, prefix, handle.run_id) if e["kind"] == "signal")
    assert signal["v"] == 3, signal  # not v:2 — the blob holds codec output
    assert "dataRef" in signal, "nothing was offloaded — this proved nothing"
    assert "data" not in signal

    # Through the library's consumer-free reader: the object-store client's `get` leaves an
    # ordered consumer behind that the server expires after a few seconds — invisible to the
    # end-of-run leftover check, caught by the per-test one (LEARNINGS #59).
    blob = await read_blob(nc, Names(prefix).blobs_bucket, signal["dataRef"]["name"])
    assert SECRET.encode() not in blob
