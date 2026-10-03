"""synadia-durable — the workflow registry: version-pinned dispatch. PROTOCOL §15. Pure.

A run always executes the **exact** version its ``start`` entry recorded. That makes replay
deterministic by construction — and makes a registered ``(name, version)`` immutable code: change a
version's command shape under an in-flight run and the determinism guards fail it loudly. To change
behaviour, register a new version and leave the old one registered until its runs drain.

Mirrors ``src/core/registry.ts``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

    from synadia_durable.types import Workflow

__all__ = ["Registry", "build_registry"]


class Registry:
    """``name → version → workflow``. Built once at worker construction; never mutated after."""

    def __init__(self, by_name: dict[str, dict[int, Workflow[Any, Any]]]) -> None:
        self._by_name = by_name

    @property
    def names(self) -> list[str]:
        """Every registered workflow NAME — one work consumer and one timer consumer each.

        All versions of a name share a queue: dispatch happens per run, from the journal.
        """
        return sorted(self._by_name)

    def versions(self, name: str) -> list[int]:
        return sorted(self._by_name.get(name, {}))

    def resolve(self, name: str, version: int) -> Workflow[Any, Any] | None:
        """The workflow for this exact ``(name, version)``, or ``None`` if it is not deployed here.

        ``None`` is not an error: the worker PARKS such a run as ``blocked`` rather than failing it,
        because the code exists somewhere — it just is not on this process yet.
        """
        return self._by_name.get(name, {}).get(version)


def build_registry(workflows: Iterable[Workflow[Any, Any]]) -> Registry:
    """Index workflows by name and version, rejecting a duplicated ``(name, version)``.

    A duplicate is always a mistake and always ambiguous — two different function bodies claiming to
    be the same immutable code — so it fails at construction rather than dispatching to whichever
    happened to be registered last.
    """
    by_name: dict[str, dict[int, Workflow[Any, Any]]] = {}
    for wf in workflows:
        versions = by_name.setdefault(wf.name, {})
        if wf.version in versions:
            raise ValueError(
                f'workflow "{wf.name}" version {wf.version} is registered twice — a (name, version) '
                f"is immutable code; register a new version instead"
            )
        versions[wf.version] = wf
    return Registry(by_name)
