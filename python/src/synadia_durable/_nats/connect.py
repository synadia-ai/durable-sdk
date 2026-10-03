"""synadia-durable — connection convenience helpers.

The library is BYO-connection: you pass a NATS client to ``create_client`` / ``create_worker``. This
just shapes the options for ``nats.connect``. Mirrors ``src/nats/connect.ts``.
"""

from __future__ import annotations

from typing import Any

__all__ = ["RECONNECT_DEFAULTS", "with_reconnect_defaults"]

#: Reconnect defaults: never give up. nats-py's own default is 60 attempts two seconds apart, after
#: which it CLOSES the connection for good — and a worker on a closed connection can never receive
#: work again (the library does not own the connection and cannot reopen it; ``health()`` reports
#: it, ``result()`` raises ``ConnectionClosedError``). A broker outage should be ridden out.
RECONNECT_DEFAULTS: dict[str, Any] = {
    "allow_reconnect": True,
    "max_reconnect_attempts": -1,
    "reconnect_time_wait": 2,
}


def with_reconnect_defaults(**options: Any) -> dict[str, Any]:
    """Fill the reconnect defaults in as fallbacks, never overwriting a caller-set option.

    ``nc = await nats.connect(url, **with_reconnect_defaults())``
    """
    return {**RECONNECT_DEFAULTS, **options}
