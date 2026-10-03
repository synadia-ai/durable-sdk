"""NAK redelivery backoff — PROTOCOL §6.4 requires it to grow with the delivery count."""

from __future__ import annotations

from synadia_durable._internal.backoff import nak_backoff_ms


def test_the_first_delivery_retries_quickly() -> None:
    assert nak_backoff_ms(1) == 250


def test_the_delay_doubles_with_the_delivery_count() -> None:
    assert [nak_backoff_ms(n) for n in (1, 2, 3, 4)] == [250, 500, 1000, 2000]


def test_the_delay_is_capped() -> None:
    # Unbounded growth would strand a run behind a transient that healed hours ago.
    assert nak_backoff_ms(50) == 30_000
    assert nak_backoff_ms(10_000) == 30_000


def test_it_is_monotonic() -> None:
    delays = [nak_backoff_ms(n) for n in range(1, 30)]
    assert delays == sorted(delays)


def test_a_zero_or_negative_delivery_count_still_backs_off() -> None:
    # A NAK with no delay hot-loops, and with unlimited redelivery one poison item then
    # head-of-line-blocks the whole workflow's consumer. Never return 0.
    assert nak_backoff_ms(0) == 250
    assert nak_backoff_ms(-5) == 250
