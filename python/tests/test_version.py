"""Version metadata — pinned, because the TypeScript twin's copy silently drifted twice."""

from __future__ import annotations

import tomllib
from pathlib import Path

from synadia_durable.version import VERSION, WIRE_VERSION, WIRE_VERSION_BLOBS

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


def test_version_matches_pyproject() -> None:
    with PYPROJECT.open("rb") as fh:
        declared: str = tomllib.load(fh)["project"]["version"]
    assert declared == VERSION


def test_wire_versions_are_the_protocols_not_the_packages() -> None:
    # The package version moves freely; these two are the shared contract and move only when the
    # wire format does (PROTOCOL §1.2, §15).
    assert WIRE_VERSION == 1
    assert WIRE_VERSION_BLOBS == 2
