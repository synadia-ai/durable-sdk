"""``with_reconnect_defaults`` — the connection posture a worker should run with (LEARNINGS #63)."""

from __future__ import annotations

from synadia_durable import RECONNECT_DEFAULTS, with_reconnect_defaults


def test_the_defaults_never_give_up() -> None:
    # nats-py's own default is 60 attempts, then the connection CLOSES for good — and a worker on a
    # closed connection never receives work again.
    assert RECONNECT_DEFAULTS["max_reconnect_attempts"] == -1
    assert RECONNECT_DEFAULTS["allow_reconnect"] is True


def test_a_caller_set_option_wins_and_nothing_is_mutated() -> None:
    before = dict(RECONNECT_DEFAULTS)
    opts = with_reconnect_defaults(max_reconnect_attempts=5, name="w1")
    assert opts["max_reconnect_attempts"] == 5
    assert opts["name"] == "w1"
    assert opts["reconnect_time_wait"] == RECONNECT_DEFAULTS["reconnect_time_wait"]
    assert before == RECONNECT_DEFAULTS


# durable-sdk: "the runbook's provisioning call is importable from the package root" is dropped.
# It pinned `Names`, `ProvisionOptions`, `provision`, `deprovision` and `resource_streams` in the
# package's exports; provisioning is the engine's (`durable-engine serve` provisions at its start)
# and the SDK does not export them (F15).
