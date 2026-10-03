"""The subject-token ∩ KV-key guard — PROTOCOL §4.2, §4.3."""

from __future__ import annotations

import pytest

from synadia_durable._internal.token import assert_token


@pytest.mark.parametrize(
    "value",
    [
        "checkout",
        "checkout-ok",
        "sub_acme",
        "R2D2",
        "0",
        "sub-acme_g1",  # a continue-as-new generation (§4.3)
        "checkout-ok_4",  # a ctx.call child (§4.3)
    ],
)
def test_accepts_valid_tokens(value: str) -> None:
    assert_token("runId", value)


@pytest.mark.parametrize(
    "value",
    [
        "",
        "has.dot",  # a second subject token
        "has space",
        "has*star",
        "has>gt",
        "sub~acme",  # legal in a subject, ILLEGAL as a KV key — the whole reason for the guard
        "café",  # non-ASCII
        "trailing\n",
    ],
)
def test_rejects_unsafe_tokens(value: str) -> None:
    with pytest.raises(ValueError, match="runId must be ASCII alphanumeric"):
        assert_token("runId", value)


def test_the_message_names_the_kind_and_the_offending_value() -> None:
    with pytest.raises(ValueError, match=r'workflow must be .*got "bad name"'):
        assert_token("workflow", "bad name")
