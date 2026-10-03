"""Version-pinned dispatch — PROTOCOL §15."""

from __future__ import annotations

from typing import Any

import pytest

from synadia_durable.core.registry import build_registry
from synadia_durable.core.workflow import define_workflow


async def _body(_ctx: Any, _input: Any) -> None:
    return None


def test_an_empty_registry_has_no_names() -> None:
    assert build_registry([]).names == []


def test_a_workflow_resolves_by_its_exact_version() -> None:
    v1 = define_workflow("charge", _body, 1)
    v2 = define_workflow("charge", _body, 2)
    registry = build_registry([v1, v2])

    assert registry.resolve("charge", 1) is v1
    assert registry.resolve("charge", 2) is v2


def test_an_unregistered_version_resolves_to_none_rather_than_a_neighbour() -> None:
    # The whole point: a run must execute the exact code it began with. Falling back to "closest
    # version" would be a silent determinism violation, so the worker parks instead.
    registry = build_registry([define_workflow("charge", _body, 1)])
    assert registry.resolve("charge", 2) is None
    assert registry.resolve("refund", 1) is None


def test_all_versions_of_a_name_share_one_queue() -> None:
    # One consumer per NAME, not per (name, version): dispatch happens per run, from the journal.
    registry = build_registry(
        [
            define_workflow("charge", _body, 1),
            define_workflow("charge", _body, 7),
            define_workflow("refund", _body, 1),
        ]
    )
    assert registry.names == ["charge", "refund"]
    assert registry.versions("charge") == [1, 7]


def test_a_duplicate_name_and_version_is_rejected_at_construction() -> None:
    # Two bodies claiming to be the same immutable code is always a mistake and always ambiguous.
    with pytest.raises(ValueError, match="registered twice"):
        build_registry([define_workflow("charge", _body, 1), define_workflow("charge", _body, 1)])
