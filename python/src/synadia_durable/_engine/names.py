"""synadia-durable — the engine's API subjects. Mirrors ``src/engine/names.ts``.

The SDK knows the prefix and one subject pattern, ``{prefix}.api.v1.<op>`` (``durable-engine``
``docs/API.md`` "Transport"); the streams, buckets and consumers behind it are the engine's, and
their names appear nowhere in this package.
"""

from __future__ import annotations

from typing import Literal

__all__ = ["DEFAULT_PREFIX", "Op", "api_subject"]

#: The default prefix: the engine's and the library's (``de``).
DEFAULT_PREFIX = "de"

#: The 15 ops of API v1.
Op = Literal[
    "start",
    "result",
    "status",
    "signal",
    "cancel",
    "poll",
    "load",
    "append",
    "wake",
    "child",
    "done",
    "extend",
    "info",
    "put",
    "get",
]


def api_subject(prefix: str, op: Op) -> str:
    """The subject a request for ``op`` is published to."""
    return f"{prefix}.api.v1.{op}"
