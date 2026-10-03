"""synadia-durable — the Children seam (``ctx.call``). Pure.

Mirrors ``src/core/children.ts``. The core starts sub-workflows through this; the NATS shell
implements it by appending the child's ``start`` entry (with the parent link) and enqueuing its work
item.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

__all__ = ["ChildStart", "Children", "ParentRef", "UnavailableChildren"]


@dataclass(frozen=True, slots=True)
class ParentRef:
    """Where a child's result must be delivered when it reaches a terminal (PROTOCOL §9)."""

    run_id: str
    seq: int
    wf: str


@dataclass(frozen=True, slots=True)
class ChildStart:
    child_run_id: str
    workflow: str
    version: int
    input: Any
    parent: ParentRef


@runtime_checkable
class Children(Protocol):
    async def start(self, child: ChildStart) -> None:
        """Start a child run idempotently, and enqueue its work item **unconditionally**.

        The unconditional enqueue is the point (PROTOCOL §9, §11.1): once the child's start entry has
        landed every re-append fences, so an enqueue gated on newness runs at most once ever — and a
        crash between the append and the enqueue would orphan the child forever, with the parent
        suspended on a run no worker will ever drive.
        """
        ...


class UnavailableChildren:
    """The default when no children implementation is injected: ``ctx.call`` fails loudly."""

    async def start(self, child: ChildStart) -> None:
        raise RuntimeError(
            f"cannot start sub-workflow {child.workflow} (run {child.child_run_id}): "
            f"no children implementation is available"
        )
