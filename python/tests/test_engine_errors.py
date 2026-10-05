"""durable-sdk: the engine's refusal codes, against a fake connection (no server). The twin of
``test/unit/engine-errors.test.ts``. Every code of the engine's table (``docs/API.md`` "Errors" at
``engine.pin``) has a class here, so a code the engine adds fails this suite rather than reaching a
caller as ``unavailable`` (the engine's F46; F40). The two limit codes are :class:`LimitError`,
retried by no call: a client's call raises it, a worker's ``start()`` raises it when its first poll
is refused, a refused poll is logged at error and backs off, and a worker's failing polls say so
once a minute (F41). An engine below the floor (``info.capabilities`` without
``withdrawn-remembered``) is refused at ``start()``, or at the first answered poll when ``start()``
found none, and ``stop()`` withdraws once (F45).
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any

import nats.errors
import pytest

from synadia_durable import create_client, create_worker, define_workflow
from synadia_durable._engine.errors import (
    ENGINE_ERROR_CODES,
    EngineError,
    EngineUnavailableError,
    LimitError,
    UnsupportedEngineError,
)
from synadia_durable._engine.transport import Transport
from synadia_durable._engine.worker import EveryLog, limit_backoff_s

pytestmark = pytest.mark.asyncio

REPO = Path(__file__).resolve().parents[2]


class _Msg:
    def __init__(self, data: bytes) -> None:
        self.data = data


class FakeNc:
    """A connection that answers each request by its op, recording the ops asked."""

    is_connected = True
    is_closed = False
    max_payload = 1024 * 1024
    stats: dict[str, int] = {"reconnects": 0}  # noqa: RUF012 — read only

    def __init__(self, answer: Any) -> None:
        self._answer = answer
        self.ops: list[str] = []

    async def rtt(self) -> float:
        return 0.001

    async def request(self, subject: str, data: bytes, timeout: float) -> _Msg:  # noqa: ARG002
        op = subject.rsplit(".", 1)[-1]
        self.ops.append(op)
        a = self._answer(op, json.loads(data))
        if asyncio.iscoroutine(a):
            a = await a
        if isinstance(a, Exception):
            raise a
        return _Msg(json.dumps(a).encode())


class Recorder:
    """A logger that keeps what it is told."""

    def __init__(self) -> None:
        self.lines: list[tuple[str, str, dict[str, Any]]] = []

    def debug(self, message: str, **fields: Any) -> None:
        self.lines.append(("debug", message, fields))

    def info(self, message: str, **fields: Any) -> None:
        self.lines.append(("info", message, fields))

    def warn(self, message: str, **fields: Any) -> None:
        self.lines.append(("warn", message, fields))

    def error(self, message: str, **fields: Any) -> None:
        self.lines.append(("error", message, fields))


def _refusal(code: str, **extra: Any) -> dict[str, Any]:
    return {"error": {"code": code, "message": f"{code} said the engine", **extra}}


async def _body(_ctx: Any, _input: Any) -> str:
    return "ok"


WF = define_workflow("lim", _body)

#: An engine at the floor's ``info`` (its #13 and later).
INFO = {"engine": "0.2.0-dev", "commit": "16cb3da", "capabilities": ["withdrawn-remembered"]}


def _engines_codes() -> list[str] | None:
    """The codes of the engine's table at engine.pin, or None without an engine checkout."""
    src = Path(os.environ.get("DE_ENGINE_REPO") or REPO.parent / "durable-engine")
    if not (src / ".git").exists():
        return None
    pin = (REPO / "engine.pin").read_text(encoding="utf-8").strip()
    try:
        doc = subprocess.check_output(
            ["git", "-C", str(src), "show", f"{pin}:docs/API.md"], text=True
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    section = re.split(r"^## Errors$", doc, flags=re.M)[1]
    section = re.split(r"^## ", section, flags=re.M)[0]
    return [c for c in re.findall(r"^\| `([a-z_]+)` \|", section, flags=re.M) if c != "code"]


# --- every code of the engine's table has a class (F40) ------------------------------------------


async def test_the_mirror_is_the_table_at_engine_pin_code_for_code() -> None:
    codes = _engines_codes()
    if codes is None:
        pytest.skip("no engine checkout to read docs/API.md at engine.pin from")
    assert sorted(ENGINE_ERROR_CODES) == sorted(codes)


@pytest.mark.parametrize(("code", "cls"), sorted(ENGINE_ERROR_CODES.items()))
async def test_each_code_is_its_class(code: str, cls: str) -> None:
    extra: dict[str, Any] = {}
    if code == "blob_gone":
        extra = {"blob": f"r1/sha256:{'a' * 64}"}
    elif code.endswith("_limit"):
        extra = {"limit": "runs"}
    t = Transport(FakeNc(lambda *_: _refusal(code, **extra)), "de")  # type: ignore[arg-type]
    with pytest.raises(Exception) as caught:  # noqa: PT011 — the class is the assertion
        await t.request("start", {"runId": "r1"})
    assert cls in [k.__name__ for k in type(caught.value).__mro__]


async def test_a_code_this_sdk_does_not_know_stays_transient_and_is_said() -> None:
    log = Recorder()
    t = Transport(FakeNc(lambda *_: _refusal("brand_new")), "de", log)  # type: ignore[arg-type]
    with pytest.raises(EngineUnavailableError):
        await t.request("start", {})
    assert [level for level, _, _ in log.lines] == ["warn"]


# --- tenant_limit and licence_limit are LimitError (the engine's F46) ----------------------------


@pytest.mark.parametrize(
    ("code", "limit"), [("tenant_limit", "workflows"), ("licence_limit", "runs")]
)
async def test_a_limit_names_its_limit_and_is_not_unavailable(code: str, limit: str) -> None:
    t = Transport(FakeNc(lambda *_: _refusal(code, limit=limit)), "de")  # type: ignore[arg-type]
    with pytest.raises(LimitError) as caught:
        await t.request("start", {"runId": "r1"})
    e = caught.value
    assert not isinstance(e, EngineUnavailableError)
    assert (e.code, e.limit, e.op) == (code, limit, "start")
    assert f"{code} said the engine" in str(e)


async def test_the_back_off_rate_a_second_the_others_30_s() -> None:
    assert limit_backoff_s(LimitError("poll", "tenant_limit", "rate", "")) == 1.0
    assert limit_backoff_s(LimitError("poll", "licence_limit", "workers", "")) == 30.0


# --- no client call retries a limit --------------------------------------------------------------


async def test_start_signal_cancel_one_request_raised() -> None:
    nc = FakeNc(lambda *_: _refusal("licence_limit", limit="runs"))
    c = create_client(nc)  # type: ignore[arg-type]
    with pytest.raises(LimitError):
        await c.start(WF, {})
    with pytest.raises(LimitError):
        await c.signal("r1", "go", 1)
    with pytest.raises(LimitError):
        await c.cancel("r1")
    assert [o for o in nc.ops if o != "info"] == ["start", "signal", "cancel"]  # info: the sizes


async def test_result_raises_at_once_where_unavailable_is_asked_again() -> None:
    nc = FakeNc(lambda *_: _refusal("tenant_limit", limit="rate"))
    c = create_client(nc)  # type: ignore[arg-type]
    with pytest.raises(LimitError) as caught:
        await c.get("r1").result(5_000)
    assert caught.value.limit == "rate"
    assert nc.ops == ["result"]


# --- a worker refused at a limit is loud, never quiet --------------------------------------------


async def test_start_raises_when_its_first_poll_is_refused_and_runs_nothing() -> None:
    nc = FakeNc(
        lambda op, _: _refusal("licence_limit", limit="workflows") if op == "poll" else INFO
    )
    log = Recorder()
    w = create_worker(nc, [WF], logger=log)  # type: ignore[arg-type]
    with pytest.raises(LimitError) as caught:
        await w.start()
    assert (caught.value.code, caught.value.limit) == ("licence_limit", "workflows")
    assert not w.health().running
    await asyncio.sleep(0.05)
    assert nc.ops == ["info", "poll"]
    errors = [f for level, _, f in log.lines if level == "error"]
    assert len(errors) == 1
    assert (errors[0]["code"], errors[0]["limit"]) == ("licence_limit", "workflows")


async def test_an_info_refused_at_a_limit_raises_too() -> None:
    nc = FakeNc(lambda *_: _refusal("licence_limit", limit="tenants"))
    with pytest.raises(LimitError):
        await create_worker(nc, [WF]).start()  # type: ignore[arg-type]


async def test_rate_which_clears_within_a_second_does_not_fail_start() -> None:
    polls = 0

    async def none() -> dict[str, Any]:
        await asyncio.sleep(0.02)
        return {"none": True}

    def answer(op: str, _body: Any) -> Any:
        nonlocal polls
        if op != "poll":
            return INFO
        polls += 1
        return _refusal("tenant_limit", limit="rate") if polls == 1 else none()

    w = create_worker(FakeNc(answer), [WF], concurrency=1)  # type: ignore[arg-type]
    await w.start()
    assert w.health().running
    await w.stop()


async def test_a_later_refused_poll_is_one_error_per_back_off_and_not_polled_meanwhile() -> None:
    polls = 0

    def answer(op: str, _body: Any) -> Any:
        nonlocal polls
        if op != "poll":
            return INFO
        polls += 1
        return {"none": True} if polls == 1 else _refusal("licence_limit", limit="workers")

    nc = FakeNc(answer)
    log = Recorder()
    w = create_worker(nc, [WF], concurrency=4, logger=log)  # type: ignore[arg-type]
    await w.start()
    await asyncio.sleep(0.3)
    # The first poll at start, then one refused poll per slot: each slot now waits 30 s.
    assert nc.ops.count("poll") == 1 + 4
    errors = [(m, f) for level, m, f in log.lines if level == "error"]
    assert len(errors) == 1
    assert (errors[0][1]["code"], errors[0][1]["limit"], errors[0][1]["refused"]) == (
        "licence_limit",
        "workers",
        1,
    )
    assert "polling again in 30 s" in errors[0][0]
    t0 = time.monotonic()
    await w.stop()  # wakes the slots' back-off at once
    assert time.monotonic() - t0 < 1.0


# --- a worker's failing polls say so once a minute, with a count (F41) ---------------------------


async def test_every_log_one_line_per_interval_standing_for_what_it_kept_quiet() -> None:
    now = 0.0
    every = EveryLog(60.0, lambda: now)
    assert every.hit() == 1
    now = 10.0
    assert every.hit() == 0
    now = 59.999
    assert every.hit() == 0
    now = 60.0
    assert every.hit() == 3
    now = 60.001
    assert every.hit() == 0


async def test_every_slot_failing_for_a_while_is_one_warning() -> None:
    nc = FakeNc(lambda op, _: nats.errors.NoRespondersError() if op == "poll" else INFO)
    log = Recorder()
    w = create_worker(nc, [WF], concurrency=4, logger=log)  # type: ignore[arg-type]
    await w.start()
    await asyncio.sleep(0.9)
    await w.stop()
    assert nc.ops.count("poll") > 8
    warns = [f for _, m, f in log.lines if "poll failed; retrying" in m]
    assert len(warns) == 1
    assert (warns[0]["failures"], warns[0]["every_s"]) == (1, 60.0)


# --- the engine's floor: info.capabilities, never the version (F45) ------------------------------


async def test_start_raises_on_an_engine_with_no_capabilities_naming_it_by_engine_and_commit() -> (
    None
):
    nc = FakeNc(
        lambda op, _: (
            {"engine": "0.2.0-dev", "commit": "5047d79"} if op == "info" else {"none": True}
        )
    )
    log = Recorder()
    w = create_worker(nc, [WF], prefix="old", logger=log)  # type: ignore[arg-type]
    with pytest.raises(UnsupportedEngineError) as caught:
        await w.start()
    e = caught.value
    assert isinstance(e, EngineError)
    assert (e.code, e.engine, e.commit, e.capabilities, e.missing) == (
        "unsupported",
        "0.2.0-dev",
        "5047d79",
        None,
        ["withdrawn-remembered"],
    )
    for part in ('"old"', "0.2.0-dev", "5047d79", "16cb3da", "withdrawn-remembered"):
        assert part in str(e)
    assert nc.ops == ["info"]
    assert not w.health().running
    assert len([1 for level, _, _ in log.lines if level == "error"]) == 1


async def test_start_raises_on_capabilities_without_withdrawn_remembered() -> None:
    nc = FakeNc(
        lambda op, _: (
            {"engine": "0.2.0-dev", "capabilities": ["something-else"]}
            if op == "info"
            else {"none": True}
        )
    )
    with pytest.raises(UnsupportedEngineError) as caught:
        await create_worker(nc, [WF]).start()  # type: ignore[arg-type]
    assert caught.value.missing == ["withdrawn-remembered"]


async def test_an_engine_that_announces_it_starts() -> None:
    async def none() -> dict[str, Any]:
        await asyncio.sleep(0.02)
        return {"none": True}

    nc = FakeNc(lambda op, _: INFO if op == "info" else none())
    w = create_worker(nc, [WF], concurrency=1)  # type: ignore[arg-type]
    await w.start()
    assert w.health().running
    await w.stop()
    assert nc.ops.count("info") == 1  # checked once, at start()


async def test_no_engine_at_start_still_polls_waitms_0_and_the_first_answered_poll_checks_the_floor() -> (
    None
):
    up = False
    bodies: list[dict[str, Any]] = []

    async def item() -> dict[str, Any]:
        await asyncio.sleep(0.01)
        return {
            "runId": "r1",
            "lease": "L",
            "workflow": "lim",
            "version": 1,
            "attempt": 1,
            "journal": [],
            "ackWaitMs": 30_000,
            "workDeadlineMs": 0,
        }

    def answer(op: str, body: Any) -> Any:
        if op == "poll":
            bodies.append(body)
        if not up:
            return nats.errors.NoRespondersError()
        if op == "info":
            return {"engine": "0.2.0-dev", "commit": "5047d79"}
        if op == "poll":
            return {"none": True} if body.get("cancelPoll") else item()
        return {}

    nc = FakeNc(answer)
    log = Recorder()
    w = create_worker(nc, [WF], concurrency=3, logger=log)  # type: ignore[arg-type]
    await w.start()  # no engine: not fatal
    assert nc.ops[:2] == ["info", "poll"]
    assert bodies[0]["waitMs"] == 0
    assert w.health().running
    up = True
    for _ in range(100):
        if not w.health().running:
            break
        await asyncio.sleep(0.02)
    assert not w.health().running
    await asyncio.sleep(0.05)
    assert nc.ops.count("info") == 2  # start()'s, and one for every slot
    assert nc.ops.count("done") > 0  # the items it was handed go back
    errors = [m for level, m, _ in log.lines if level == "error"]
    assert len(errors) == 1
    assert "5047d79" in errors[0]
    assert "this worker stops" in errors[0]
    await w.stop()


async def test_stop_withdraws_once_and_returns_when_the_engine_answers_the_polls() -> None:
    waiting: list[asyncio.Future[dict[str, Any]]] = []
    cancels = 0

    def answer(op: str, body: Any) -> Any:
        nonlocal cancels
        if op == "info":
            return INFO
        if op != "poll":
            return {}
        if body.get("cancelPoll"):
            cancels += 1
            for w in waiting:
                w.set_result({"none": True})
            waiting.clear()
            return {"none": True}
        if body.get("waitMs") == 0:
            return {"none": True}
        f: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        waiting.append(f)
        return f

    w = create_worker(FakeNc(answer), [WF], concurrency=4)  # type: ignore[arg-type]
    await w.start()
    await asyncio.sleep(0.02)
    assert len(waiting) == 4
    t0 = time.monotonic()
    await w.stop()
    assert time.monotonic() - t0 < 0.5
    await asyncio.sleep(0.3)  # no repeat afterwards either
    assert cancels == 1
