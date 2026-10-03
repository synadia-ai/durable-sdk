"""synadia-durable — the metrics/health seam. Mirrors ``src/internal/metrics.ts``.

Injected like the logger, with a silent default: the library reports observable moments as
``(name, fields)`` events and commits to nothing beyond that — no counter/gauge/histogram API, no
aggregation, no exporter. A sink maps events onto whatever the host app runs (statsd, Prometheus,
OpenTelemetry, a log line).

Two naming decisions, deliberately different from each other:

* **Event names are identical to the reference implementation's** (``journal.append``,
  ``work.done``, …). They are the vocabulary an operator builds dashboards and alerts on, and a
  mixed-language fleet should be able to graph one number.
* **Field names are snake_case here**, where the reference uses camelCase. They are ordinary Python
  keyword arguments, and this SDK's logger already reports ``run_id=…``; a sink written for both
  SDKs maps four or five field names, which is cheaper than a Python API that reads like JSON.

Event catalog:

``journal.append``
    One CAS append attempt. Fields: ``run_id``, ``kind``, ``seq``, ``bytes``, ``ms``, ``outcome``:
    ``ok`` | ``duplicate`` | ``fenced`` | ``payload-too-large`` | ``serialization`` | ``error``.
    Fences and payload rejections are counted here.
``journal.load``
    One full journal read — every replay starts with one. Fields: ``run_id``, ``entries``,
    ``bytes``, ``ms``, ``outcome``: ``ok`` | ``short-read``.
``work.dispatch``
    A work item entered this worker's local queue. Fields: ``workflow``, ``run_id``, ``kind``,
    ``inflight`` (dispatched-but-unfinished depth).
``work.done``
    A work item finished. Fields: ``workflow``, ``run_id``, ``kind``, ``outcome`` (the ack/NAK
    reason — ``completed`` | ``failed`` | ``cancelled`` | ``continued`` | ``suspended`` | ``fenced``
    | ``retry`` | ``parked`` | ``lease-held`` | ``terminal-redrive`` | …), ``ms`` (execution),
    ``wait_ms`` (local queue wait), ``delivery_count``.
``pump.restart``
    A consumer pump failed and is recreating its consumer. Fields: ``stream``, ``durable``,
    ``restarts``, ``error``.
``maintenance.purge``
    A hygiene pass reclaimed consumed timer wakes. Fields: ``workflow``, ``floor``.
``maintenance.headroom``
    A gauge, once per hygiene pass, for the one stream that cannot be ``discard: new`` and therefore
    EVICTS rather than rejects when full (``DE_TIMERS``; an evicted pending schedule is a run that
    never wakes). Fields: ``stream``, ``bytes``, ``max_bytes``, ``used_pct``. **Alert on this one** —
    past ~75 % the worker also logs a warning.
``maintenance.reconcile``
    One sweep of the parked-timer index. Fields: ``overdue``, ``redriven``, ``executing`` (skipped
    because a live lease says a worker is on it). A non-zero ``redriven`` means wakes were genuinely
    lost — pair it with ``maintenance.headroom`` to find out why.
``lease.gate``
    One gating-lease arbitration before executing a work item. Fields: ``run_id``, ``outcome``:
    ``acquired`` | ``reentrant`` | ``takeover`` | ``held`` (+ ``retry_ms``) | ``fail-open`` |
    ``budget-exhausted`` (the holder hit ``max_work_ms`` and let the lease lapse for a peer).
``blob.put`` / ``blob.get``
    One offloaded-payload write or read. Fields: ``name``, ``bytes``, ``ms``, ``outcome``: ``ok`` |
    ``missing`` (get only) | ``error``.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["Metrics", "SilentMetrics", "emit_safe"]


# Fields arrive as a MAPPING rather than as keyword arguments on purpose: `name` is a legitimate
# field (a blob's object name) and would collide with the event's own name as a keyword — a
# collision that surfaces as a TypeError inside the guard below, i.e. as silently missing telemetry.
# The reference implementation passes a record for the same reason.
@runtime_checkable
class Metrics(Protocol):
    """The injectable sink. Implementations must be cheap and non-blocking: they are called inline."""

    def on_event(self, name: str, fields: Mapping[str, Any]) -> None: ...


class SilentMetrics:
    """The default. Discards everything, so the engine still computes nothing it does not need."""

    def on_event(self, name: str, fields: Mapping[str, Any]) -> None: ...


def emit_safe(metrics: Metrics | None, name: str, fields: Mapping[str, Any]) -> None:
    """Emit through a guard: a buggy or throwing user sink must NEVER break engine progress.

    Observability is not control flow. This is the one place that rule is enforced, and it is why
    every call site can emit without a try block of its own.
    """
    if metrics is None:
        return
    with contextlib.suppress(Exception):  # a metrics sink is observability, never control flow
        metrics.on_event(name, fields)
