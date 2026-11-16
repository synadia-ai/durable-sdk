"""The publication controls of scripts/python-release.py, on small artifacts made here."""

from __future__ import annotations

import importlib.util
import io
import json
import os
import tarfile
import tempfile
import tomllib
import unittest
import zipfile
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).resolve().parents[1] / "python-release.py"
SPEC = importlib.util.spec_from_file_location("python_release", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)

VERSION = "0.2.0"
STEM = f"synadia_durable-{VERSION}"
README = "# synadia-durable\n\nPython ≥ 3.11 → [the repository](https://github.com/synadia-ai/durable-sdk).\n"
LICENSE = (release.ROOT / "LICENSE").read_text()
PROJECT = f"""[project]
name = "synadia-durable"
version = "{VERSION}"
dependencies = ["nats-py>=2.15.0"]

[build-system]
requires = ["{release.BACKEND_REQUIREMENT}"]
build-backend = "hatchling.build"
"""
SOURCES = {
    "synadia_durable/__init__.py": 'VERSION = "0.2.0"\n',
    "synadia_durable/py.typed": "",
}


def metadata(readme: str = README, version: str = VERSION) -> str:
    return (
        "Metadata-Version: 2.4\n"
        "Name: synadia-durable\n"
        f"Version: {version}\n"
        "Summary: The thin durable execution SDK\n"
        "Project-URL: Repository, https://github.com/synadia-ai/durable-sdk\n"
        "License-Expression: Apache-2.0\n"
        "License-File: LICENSE\n"
        "Requires-Python: >=3.11\n"
        "Requires-Dist: nats-py>=2.15.0\n"
        "Provides-Extra: crypto\n"
        "Requires-Dist: cryptography>=42; extra == 'crypto'\n"
        "Description-Content-Type: text/markdown\n"
        f"\n{readme}"
    )


def make_artifacts(
    root: Path,
    *,
    wheel_files: dict[str, str] | None = None,
    sdist_extra: dict[str, str] | None = None,
    readme: str = README,
    description: str | None = None,
    generator: str = release.BUILD_BACKEND.replace("==", " "),
) -> tuple[Path, Path]:
    wheel = root / f"{STEM}-py3-none-any.whl"
    sdist = root / f"{STEM}.tar.gz"
    info = f"{STEM}.dist-info/"
    with zipfile.ZipFile(wheel, "w") as archive:
        for name, value in (wheel_files if wheel_files is not None else SOURCES).items():
            archive.writestr(name, value)
        archive.writestr(
            info + "METADATA", metadata(readme if description is None else description)
        )
        archive.writestr(info + "WHEEL", f"Wheel-Version: 1.0\nGenerator: {generator}\n")
        archive.writestr(info + "licenses/LICENSE", LICENSE)
        archive.writestr(info + "RECORD", "")
    files = {
        "PKG-INFO": metadata(readme),
        "pyproject.toml": PROJECT,
        "README.md": readme,
        "LICENSE": LICENSE,
        **{f"src/{name}": value for name, value in SOURCES.items()},
        **(sdist_extra or {}),
    }
    with tarfile.open(sdist, "w:gz") as archive:
        for name, value in files.items():
            data = value.encode()
            member = tarfile.TarInfo(f"{STEM}/{name}")
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
    return wheel, sdist


def junit(cases: list[str]) -> str:
    return (
        '<?xml version="1.0"?><testsuites><testsuite>'
        + "".join(cases)
        + "</testsuite></testsuites>"
    )


INJECTED_SKIPPED = (
    '<testcase classname="tests.integration.test_injected_failure" name="test_e">'
    '<skipped type="pytest.skip" message="DE_INJECT_FAILURE=1 only"/></testcase>'
)
UNIT_SKIPPED = (
    '<testcase classname="tests.test_engine_errors" name="test_f">'
    '<skipped type="pytest.skip" message="no DE_ENGINE_API_MD"/></testcase>'
)
UNIT_XFAILED = (
    '<testcase classname="tests.test_replay" name="test_g">'
    '<skipped type="pytest.xfail" message="known"/></testcase>'
)
UNIT_PASSED = '<testcase classname="tests.test_replay" name="test_a"/>'
INTEGRATION_SKIPPED = (
    '<testcase classname="tests.integration.test_worker" name="test_b">'
    '<skipped type="pytest.skip" message="durable-engine unavailable (no DE_ENGINE)"/></testcase>'
)


class PythonReleaseTests(unittest.TestCase):
    def test_only_uv_generated_ignore_file_is_removed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ignore = root / ".gitignore"
            ignore.write_bytes(b"*")
            wheel = root / "keep.whl"
            wheel.write_bytes(b"keep artifact bytes")
            release.clean_uv_output(root)
            self.assertFalse(ignore.exists())
            self.assertEqual(wheel.read_bytes(), b"keep artifact bytes")
            ignore.write_bytes(b"unexpected")
            with self.assertRaisesRegex(ValueError, "unexpected generated ignore contents"):
                release.clean_uv_output(root)
            self.assertEqual(ignore.read_bytes(), b"unexpected")

    def test_the_project_declares_the_backend_requirement_not_the_pin(self) -> None:
        project = tomllib.loads(PROJECT)
        release.check_project(project)
        pinned = tomllib.loads(PROJECT.replace(release.BACKEND_REQUIREMENT, release.BUILD_BACKEND))
        with self.assertRaisesRegex(ValueError, "build-system requires"):
            release.check_project(pinned)
        project["tool"] = {"uv": {"sources": {"nats-py": {"path": "../nats"}}}}
        with self.assertRaisesRegex(ValueError, "source override"):
            release.check_project(project)

    def test_the_committed_project_declares_the_backend_requirement(self) -> None:
        text = (release.ROOT / "python" / "pyproject.toml").read_text()
        release.check_project(tomllib.loads(text))

    def test_public_boundary_admits_this_repository_only(self) -> None:
        release.public_bytes(b"https://github.com/synadia-ai/durable-sdk/blob/main/x", "readme")
        release.public_bytes(b"git@github.com:synadia-ai/durable-sdk.git", "readme")
        with self.assertRaisesRegex(ValueError, "unapproved repository"):
            release.public_bytes(b"https://github.com/synadia-ai/other-repo", "readme")
        with self.assertRaisesRegex(ValueError, "local build path"):
            release.public_bytes(b"built in /Users/someone/src", "module")

    def test_links_must_work_from_pypi(self) -> None:
        release.links_work_from_pypi("[a](https://github.com/synadia-ai/durable-sdk)", "readme")
        for target in ("../docs/FABRIC.md", "#quickstart", "examples/README.md", "http://x"):
            with self.assertRaisesRegex(ValueError, "does not work from PyPI"):
                release.links_work_from_pypi(f"see [it]({target})", "readme")

    def test_the_committed_readme_links_work_from_pypi(self) -> None:
        release.links_work_from_pypi(
            (release.ROOT / "python" / "README.md").read_text(), "python/README.md"
        )

    def test_the_committed_licence_copy_is_the_repositorys(self) -> None:
        self.assertEqual(
            (release.ROOT / "python" / "LICENSE").read_bytes(),
            (release.ROOT / "LICENSE").read_bytes(),
        )

    def test_inspection_admits_a_valid_pair_and_refuses_a_version_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            make_artifacts(root)
            self.assertEqual(len(release.inspect(root, VERSION)), 2)
            with self.assertRaisesRegex(ValueError, "filename differs"):
                release.inspect(root, "0.2.1")

    def test_inspection_refuses_a_wheel_built_by_another_backend(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            make_artifacts(root, generator="hatchling 1.27.0")
            with self.assertRaisesRegex(ValueError, "not built by"):
                release.inspect(root, VERSION)

    def test_inspection_refuses_a_wheel_that_is_not_the_sdists_sources(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            make_artifacts(root, wheel_files={**SOURCES, "synadia_durable/extra.py": "x = 1\n"})
            with self.assertRaisesRegex(ValueError, "not the sdist's sources"):
                release.inspect(root, VERSION)

    def test_inspection_refuses_the_suites_in_the_sdist(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            make_artifacts(root, sdist_extra={"tests/test_x.py": "def test(): pass\n"})
            with self.assertRaisesRegex(ValueError, "files beyond the package"):
                release.inspect(root, VERSION)

    def test_inspection_refuses_a_readme_with_a_relative_link(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            make_artifacts(root, readme=README + "[fabric](../docs/FABRIC.md)\n")
            with self.assertRaisesRegex(ValueError, "does not work from PyPI"):
                release.inspect(root, VERSION)

    def test_inspection_refuses_a_description_that_is_not_the_readme(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            make_artifacts(root, description="# another text\n")
            with self.assertRaisesRegex(ValueError, "not the README"):
                release.inspect(root, VERSION)

    @mock.patch.dict(os.environ, {"GITHUB_SHA": "0" * 40})
    def test_digest_guard_detects_changed_distribution(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            wheel, sdist = make_artifacts(root)
            record = {
                "schema": 1,
                "name": release.NAME,
                "version": VERSION,
                "source_commit": "0" * 40,
                "external_cutoff": release.CUTOFF,
                "build_backend": release.BUILD_BACKEND,
                "artifacts": {p.name: release.sha256(p) for p in (wheel, sdist)},
            }
            (root / "artifacts.json").write_text(json.dumps(record))
            release.verify(root, VERSION, False)
            with (
                mock.patch.dict(os.environ, {"GITHUB_SHA": "1" * 40}),
                self.assertRaisesRegex(ValueError, "source differs"),
            ):
                release.verify(root, VERSION, False)
            with wheel.open("ab") as stream:
                stream.write(b"changed")
            with self.assertRaisesRegex(ValueError, "digest differs"):
                release.verify(root, VERSION, False)

    def check_suite(self, cases: list[str]) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            report = Path(temporary) / "pytest.xml"
            report.write_text(junit(cases))
            release.check_suite(report)

    def test_the_suite_check_wants_units_passed_and_integration_skipped_for_no_engine(
        self,
    ) -> None:
        self.check_suite(
            [UNIT_PASSED, INTEGRATION_SKIPPED, INTEGRATION_SKIPPED, INJECTED_SKIPPED]
            + [UNIT_SKIPPED] * release.MAX_UNIT_SKIPS
            + [UNIT_XFAILED] * 5
        )
        cases = {
            "did not skip for want of an engine": [UNIT_PASSED, INJECTED_SKIPPED],
            "no unit test passed": [INTEGRATION_SKIPPED],
            "the integration tests did not skip": [UNIT_PASSED],
            "test_c ran without an engine": [
                UNIT_PASSED,
                INTEGRATION_SKIPPED,
                '<testcase classname="tests.integration.test_worker" name="test_c"/>',
            ],
            "did not skip for want": [
                UNIT_PASSED,
                INTEGRATION_SKIPPED,
                INJECTED_SKIPPED,
                '<testcase classname="tests.integration.test_worker" name="test_d">'
                '<skipped type="pytest.skip" message="nats-server not found on PATH"/></testcase>',
            ],
            "4 unit tests skipped": [UNIT_PASSED, INTEGRATION_SKIPPED, *[UNIT_SKIPPED] * 4],
            "t: failure": [
                UNIT_PASSED,
                INTEGRATION_SKIPPED,
                '<testcase classname="tests.test_x" name="t"><failure message="no"/></testcase>',
            ],
        }
        for message, given in cases.items():
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                self.check_suite(given)


if __name__ == "__main__":
    unittest.main()
