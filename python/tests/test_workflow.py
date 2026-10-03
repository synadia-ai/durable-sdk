"""Workflow definition — the token, the guards, and why the decorator does not return the function."""

from __future__ import annotations

from typing import Any

import pytest

from synadia_durable.core.workflow import define_workflow, workflow
from synadia_durable.types import Workflow


async def _body(_ctx: Any, value: int) -> int:
    return value + 1


def test_the_decorator_returns_a_workflow_token_not_the_function() -> None:
    @workflow(name="charge", version=2)
    async def charge(_ctx: Any, value: int) -> int:
        return value

    assert isinstance(charge, Workflow)
    assert (charge.name, charge.version) == ("charge", 2)


def test_the_name_defaults_to_the_function_name() -> None:
    @workflow()
    async def settle(_ctx: Any, value: int) -> int:
        return value

    assert settle.name == "settle"
    assert settle.version == 1


def test_calling_a_workflow_directly_fails_loudly() -> None:
    # Running the body outside a worker would execute every side effect with nothing journaled —
    # silently, and looking like it worked. Better to make it impossible to do by accident.
    wf = define_workflow("charge", _body)
    with pytest.raises(TypeError, match="cannot be called directly"):
        wf(None, 1)


@pytest.mark.parametrize("bad", ["has.dot", "has space", "sub~acme", ""])
def test_an_unsafe_workflow_name_is_rejected_at_definition(bad: str) -> None:
    # At definition, not at the first publish: the name is a subject token AND a consumer durable,
    # and a stack trace from inside a publish says nothing useful about where it came from.
    with pytest.raises(ValueError, match="workflow name"):
        define_workflow(bad, _body)


@pytest.mark.parametrize("bad", [0, -1, 1.5, True, "1"])
def test_a_non_positive_integer_version_is_rejected(bad: object) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        define_workflow("charge", _body, bad)  # type: ignore[arg-type]


def test_the_same_name_may_carry_several_versions() -> None:
    # Version-pinned dispatch (PROTOCOL §15): a run executes the exact version its start recorded,
    # so old versions stay registered until their runs drain.
    v1 = define_workflow("charge", _body, 1)
    v2 = define_workflow("charge", _body, 2)
    assert (v1.name, v2.name) == ("charge", "charge")
    assert v1.version != v2.version
