"""synadia-durable — the injectable logger seam. Mirrors ``src/internal/logger.ts``.

A library must not print. The default is silent; pass ``logger=`` to see anything. Levels take a
message plus structured fields, because every interesting line here is about a specific run.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol, runtime_checkable

__all__ = ["Logger", "SilentLogger", "StdlibLogger"]


@runtime_checkable
class Logger(Protocol):
    def debug(self, message: str, **fields: Any) -> None: ...
    def info(self, message: str, **fields: Any) -> None: ...
    def warn(self, message: str, **fields: Any) -> None: ...
    def error(self, message: str, **fields: Any) -> None: ...


class SilentLogger:
    """The default. Discards everything."""

    def debug(self, message: str, **fields: Any) -> None: ...
    def info(self, message: str, **fields: Any) -> None: ...
    def warn(self, message: str, **fields: Any) -> None: ...
    def error(self, message: str, **fields: Any) -> None: ...


class StdlibLogger:
    """Adapts a ``logging.Logger``. Structured fields go through ``extra``, and are also appended to
    the message so they survive a formatter that ignores ``extra`` (most default ones do)."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._log = logger or logging.getLogger("synadia_durable")

    def _emit(self, level: int, message: str, fields: dict[str, Any]) -> None:
        suffix = " " + " ".join(f"{k}={v!r}" for k, v in fields.items()) if fields else ""
        self._log.log(level, "%s%s", message, suffix, extra={"durable": fields})

    def debug(self, message: str, **fields: Any) -> None:
        self._emit(logging.DEBUG, message, fields)

    def info(self, message: str, **fields: Any) -> None:
        self._emit(logging.INFO, message, fields)

    def warn(self, message: str, **fields: Any) -> None:
        self._emit(logging.WARNING, message, fields)

    def error(self, message: str, **fields: Any) -> None:
        self._emit(logging.ERROR, message, fields)
