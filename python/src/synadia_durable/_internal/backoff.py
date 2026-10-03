"""synadia-durable — redelivery backoff for a NAK'd work item.

Mirrors ``src/nats/worker.ts``'s ``nakBackoffMs``. PROTOCOL §6.4 requires the delay to GROW with the
delivery count, and the reason is worth stating: with unlimited redelivery, a NAK with no delay
hot-loops on a *persistent* transient error — one poison item then head-of-line-blocks the whole
workflow's consumer, reloading a full journal on every turn. Genuine blips still retry in a second or
two.
"""

from __future__ import annotations

__all__ = ["nak_backoff_ms"]

_BASE_MS = 250
_CAP_MS = 30_000
_MAX_DOUBLINGS = 7


def nak_backoff_ms(delivery_count: int) -> float:
    """Exponential in the delivery count: 250 ms → 30 s, capped."""
    attempt = max(1, int(delivery_count))
    return float(min(_CAP_MS, _BASE_MS * 2 ** min(attempt - 1, _MAX_DOUBLINGS)))
