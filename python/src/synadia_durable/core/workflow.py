"""synadia-durable — workflow definition. Pure. Mirrors ``src/core/workflow.ts``.

A ``(name, version)`` pair is **immutable code** (PROTOCOL §15): a run always executes the exact
version its ``start`` entry recorded, so changing a version's command shape under an in-flight run is
a determinism violation the replay guards will catch. To change behaviour, register a new version.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from synadia_durable._internal.token import assert_token
from synadia_durable.types import Workflow, WorkflowFn

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["define_workflow", "workflow"]

InT = TypeVar("InT")
OutT = TypeVar("OutT")


def define_workflow(
    name: str,
    fn: WorkflowFn[InT, OutT],
    version: int = 1,
) -> Workflow[InT, OutT]:
    """Define a workflow programmatically. ``@workflow`` is the decorator form of this."""
    # The name is a NATS subject token (`de.work.{name}`) AND a consumer durable name, so guard it
    # here — at definition — rather than at the first publish, where the stack says nothing useful.
    assert_token("workflow name", name)
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise ValueError(f'workflow "{name}": version must be a positive integer, got {version!r}')
    return Workflow(name=name, version=version, fn=fn)


def workflow(
    *, name: str | None = None, version: int = 1
) -> Callable[[WorkflowFn[InT, OutT]], Workflow[InT, OutT]]:
    """Decorate an async function as a durable workflow.

    ::

        @workflow(name="charge", version=1)
        async def charge(ctx: Ctx, order: Order) -> Receipt: ...

    Returns the :class:`~synadia_durable.types.Workflow` token, not the function — so the decorated
    name is what you hand to ``client.start`` and ``ctx.call``, and calling it directly raises
    instead of running a workflow body outside the engine.

    Always call it with parentheses; a bare ``@workflow`` would receive the function as ``name``.
    """

    def decorate(fn: WorkflowFn[InT, OutT]) -> Workflow[InT, OutT]:
        resolved = name if name is not None else getattr(fn, "__name__", None)
        if not resolved:
            raise ValueError("workflow(): no name given and the function has no __name__")
        return define_workflow(resolved, fn, version)

    return decorate
