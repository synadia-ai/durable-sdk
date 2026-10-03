"""synadia-durable — the Scheduler seam (``ctx.sleep`` and ``ctx.wait`` timeouts). Pure.

Mirrors ``src/core/scheduler.ts``. The core arms wakes through this; the NATS shell implements it with
native JetStream message scheduling.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

__all__ = ["Scheduler", "UnavailableScheduler"]


@runtime_checkable
class Scheduler(Protocol):
    async def schedule_wake(self, run_id: str, workflow: str, seq: int, due_ms: int) -> None:
        """Arm a single-shot wake for ``(run_id, seq)`` at absolute ``due_ms``.

        MUST be idempotent: replay re-arms every wake that is still pending, so arming the same
        ``(run_id, seq)`` twice must not produce two wakes.
        """
        ...


class UnavailableScheduler:
    """The default when no scheduler is injected: any timer use fails loudly rather than silently.

    A workflow that sleeps against this would otherwise park forever with nothing to wake it — the
    worst possible failure, because it looks like patience.
    """

    async def schedule_wake(self, run_id: str, workflow: str, seq: int, due_ms: int) -> None:
        raise RuntimeError(
            f"run {run_id}: no scheduler is available, so the wake for command {seq} "
            f"(workflow {workflow}, due {due_ms}) cannot be armed"
        )
