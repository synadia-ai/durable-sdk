"""durable-sdk: the transport's reading of a reply and the blob store's ``put`` wait, against a fake
connection (no server). The twin of ``test/unit/engine-transport.test.ts``. What a refusal is (the
engine's F31: an ``error`` member with a string ``code``, never a failed run's error value), what
``blob_gone`` becomes (the engine's F30: the library's BlobMissingError or BlobIntegrityError, this
repo's F31), and how long a ``put`` part is waited for and what its failure becomes (F33).
"""

from __future__ import annotations

import json
import math
from typing import Any, cast

import nats.errors
import pytest

from synadia_durable._engine.blobs import (
    PUT_MS_PER_MIB,
    PUT_RTTS_PER_CHUNK,
    STORE_CHUNK_BYTES,
    EngineBlobs,
    Limits,
    Sizes,
    put_timeout_ms,
)
from synadia_durable._engine.errors import EngineUnavailableError, LeaseExpiredError
from synadia_durable._engine.names import Op
from synadia_durable._engine.transport import REQUEST_MARGIN_MS, Transport
from synadia_durable.errors import (
    BlobIntegrityError,
    BlobMissingError,
    PayloadTooLargeError,
    SerializationError,
)

pytestmark = pytest.mark.asyncio

NAME = f"r1/sha256:{'a' * 64}"
MIB = 1024 * 1024


class _Msg:
    def __init__(self, data: bytes) -> None:
        self.data = data


class FakeNc:
    """A connection whose every request answers ``reply`` (or raises it), recording the timeouts."""

    is_connected = True
    is_closed = False
    max_payload = MIB

    def __init__(self, reply: dict[str, Any] | Exception) -> None:
        self._reply = reply
        self.timeouts: list[float] = []

    async def request(self, _subject: str, _data: bytes, timeout: float) -> _Msg:
        self.timeouts.append(timeout)
        if isinstance(self._reply, Exception):
            raise self._reply
        return _Msg(json.dumps(self._reply).encode())


def _transport(reply: dict[str, Any] | Exception) -> Transport:
    return Transport(FakeNc(reply), "de")  # type: ignore[arg-type]


async def _limits() -> Limits:
    return Limits(MIB, MIB // 2)


#: A ``Sizes`` stand-in: what it answers is all EngineBlobs asks of it.
_sizes = cast("Sizes", _limits)


# --- a refusal is an error member with a string code (the engine's F31) --------------------------


async def test_a_failed_runs_result_and_status_are_answered_not_raised() -> None:
    failed = {"kind": "fail", "error": {"name": "Error", "message": "boom"}}
    assert await _transport(failed).request("result", {"runId": "r1"}) == failed
    status = {"state": "failed", "error": {"name": "Error", "message": "boom"}}
    assert await _transport(status).request("status", {"runId": "r1"}) == status


async def test_an_error_member_with_a_code_is_the_refusal() -> None:
    with pytest.raises(LeaseExpiredError):
        await _transport({"error": {"code": "expired", "message": "gone"}}).request("load", {})


# --- blob_gone (the engine's F30) is the library's missing-blob error ----------------------------


async def test_a_gone_blob_is_blob_missing_naming_the_blob_and_its_run() -> None:
    message = (
        f"run r1 seq -1: blob {NAME} is gone from the blob store; restore it or cancel the run"
    )
    refusal = {"error": {"code": "blob_gone", "message": message, "blob": NAME}}
    with pytest.raises(BlobMissingError) as caught:
        await _transport(refusal).request("get", {"runId": "r1", "name": NAME, "part": 0})
    e = caught.value
    assert (e.run_id, e.blob_name, e.seq, e.reason) == ("r1", NAME, -1, "absent")


@pytest.mark.parametrize(
    "detail",
    [
        f"content is sha256:{'b' * 64}, not the digest its name carries",
        f"digest mismatch: ref has sha256:{'a' * 64}, content is sha256:{'b' * 64}",
    ],
)
async def test_bytes_that_are_not_the_digests_are_blob_integrity(detail: str) -> None:
    refusal = {"error": {"code": "blob_gone", "message": f"run r1 seq 4: blob {NAME}: {detail}", "blob": NAME}}  # fmt: skip
    with pytest.raises(BlobIntegrityError) as caught:
        await _transport(refusal).request("result", {"runId": "r1"})
    assert (caught.value.run_id, caught.value.blob_name) == ("r1", NAME)


async def test_the_blob_store_raises_blob_missing_for_a_gone_blob() -> None:
    message = (
        f"run r1 seq -1: blob {NAME} is gone from the blob store; restore it or cancel the run"
    )
    t = _transport({"error": {"code": "blob_gone", "message": message, "blob": NAME}})
    with pytest.raises(BlobMissingError):
        await EngineBlobs(t.request, _sizes).get(NAME)


async def test_internal_is_no_longer_read_as_gone() -> None:
    t = _transport({"error": {"code": "internal", "message": "codec"}})
    with pytest.raises(EngineUnavailableError):
        await EngineBlobs(t.request, _sizes).get(NAME)


# --- a put part's wait scales with what the engine stores for it (F33) ---------------------------


async def test_the_margin_a_round_trip_per_chunk_and_the_throughput_floor() -> None:
    assert put_timeout_ms(0, 50) == REQUEST_MARGIN_MS
    chunks = math.ceil(MIB / STORE_CHUNK_BYTES)
    assert put_timeout_ms(MIB, 50) == REQUEST_MARGIN_MS + chunks * PUT_RTTS_PER_CHUNK * 50 + PUT_MS_PER_MIB  # fmt: skip
    # The NGS run's case: a 12 MiB value's last part (30 KiB of its own, the value read back and
    # written) at 51 ms, where the flat 10 s gave up.
    assert put_timeout_ms(30_720 + 2 * 12 * MIB, 51) > 70_000


async def test_each_part_asks_for_its_own_timeout_and_the_last_for_the_value_twice() -> None:
    asked: list[int | None] = []

    async def request(_op: Op, body: dict[str, Any], *, timeout_ms: int | None = None) -> dict[str, Any]:  # fmt: skip
        asked.append(timeout_ms)
        return {"ok": True, "ref": {"name": NAME}} if body.get("last") else {"ok": True}

    async def limits() -> Limits:
        return Limits(8 * 1024, 3 * 1024)

    async def rtt() -> float:
        return 0.020

    data = bytes(7 * 1024)  # parts of 3, 3 and 1 KiB
    await EngineBlobs(request, cast("Sizes", limits), rtt=rtt).upload("r1", data)
    assert asked == [
        put_timeout_ms(3 * 1024, 20),
        put_timeout_ms(3 * 1024, 20),
        put_timeout_ms(1024 + 2 * len(data), 20),
    ]


async def test_the_transport_sends_the_timeout_it_is_given() -> None:
    nc = FakeNc({"ok": True})
    t = Transport(nc, "de")  # type: ignore[arg-type]
    await t.request("put", {"runId": "r1"}, timeout_ms=123_456)
    await t.request("info", {})
    assert nc.timeouts == [123.456, REQUEST_MARGIN_MS / 1000]


# --- a put that fails is deterministic for its value, never a retry loop (F33) -------------------


async def test_no_answer_within_the_scaled_wait_is_payload_too_large() -> None:
    t = _transport(nats.errors.TimeoutError())
    with pytest.raises(PayloadTooLargeError) as caught:
        await EngineBlobs(t.request, _sizes).upload("r1", bytes(10))
    assert isinstance(caught.value, SerializationError)
    assert "did not store a 10-byte value in time" in str(caught.value)


async def test_an_invalid_put_is_a_serialization_error() -> None:
    t = _transport({"error": {"code": "invalid", "message": "digest"}})
    with pytest.raises(SerializationError):
        await EngineBlobs(t.request, _sizes).upload("r1", bytes(10))


@pytest.mark.parametrize(
    ("code", "kind"), [("expired", LeaseExpiredError), ("unavailable", EngineUnavailableError)]
)
async def test_an_expired_lease_and_the_engines_unavailable_keep_their_meaning(
    code: str, kind: type[Exception]
) -> None:
    t = _transport({"error": {"code": code, "message": code}})
    with pytest.raises(kind) as caught:
        await EngineBlobs(t.request, _sizes).upload("r1", bytes(10))
    assert not isinstance(caught.value, SerializationError)
