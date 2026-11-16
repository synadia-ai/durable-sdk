#!/usr/bin/env python3
"""Build, inspect and install-test the exact Python publication artifacts of synadia-durable.

    python-release.py version
    python-release.py build --version V --artifacts DIR
    python-release.py verify --version V --artifacts DIR [--install]
    python-release.py check-suite --junit FILE
    python-release.py smoke --version V [--crypto]

`build` makes the sdist from the committed source and the wheel from that sdist, inspects both and
records their checksums. `verify` checks a directory against that record and inspects again; with
`--install` it installs each artifact into a new environment on every supported Python and runs
`smoke` there. `check-suite` reads the source suite's JUnit report and fails unless the unit suites
passed and every integration test was skipped for want of an engine. Standard library only.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.metadata
import importlib.util
import json
import os
import re
import socket
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import zipfile
from email.message import Message
from email.parser import Parser
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "python"
NAME = "synadia-durable"
MODULE = "synadia_durable"
REPOSITORY = "synadia-ai/durable-sdk"
# External dependencies (build and install) resolve as of this date: at least seven days before
# the release. Move it deliberately for a later release (docs/python-publication.md).
CUTOFF = "2026-11-09T00:00:00Z"
# The release build's backend, exact, by a build constraint; the wheel's WHEEL file names it.
BUILD_BACKEND = "hatchling==1.33.0"
# The project's own requirement, which the published sdist keeps (a licence expression needs 1.27).
BACKEND_REQUIREMENT = "hatchling>=1.27"
# The wheel's Requires-Dist, exactly, with spaces removed and quotes made double.
REQUIRES = {"nats-py>=2.15.0", 'cryptography>=42;extra=="crypto"'}
EXTRAS = {"crypto"}
PYTHONS = ("3.11", "3.12", "3.13")
# What the sdist holds besides src/synadia_durable/: [tool.hatch.build.targets.sdist] include.
SDIST_FILES = {"PKG-INFO", "pyproject.toml", "README.md", "LICENSE"}
# An integration test skips with this reason when no engine is given (tests/integration/conftest.py)
NO_ENGINE = "durable-engine unavailable"
# Without an engine the unit suites skip one test (no DE_ENGINE_API_MD); more means a lost suite.
MAX_UNIT_SKIPS = 3


def run(command: list[str], **kwargs: Any) -> None:
    subprocess.run(command, check=True, **kwargs)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition: object, message: str) -> None:
    if not condition:
        raise ValueError(message)


def public_bytes(data: bytes, name: str) -> None:
    """The repository is public, and so is everything published from it: no local paths and no
    reference to any repository of the organization but this one."""
    for value in (b"/Users/", b"/home/"):
        require(value not in data, f"local build path in {name}")
    for repository in re.findall(rb"github\.com[:/](synadia-ai/[A-Za-z0-9_.-]+)", data):
        require(
            repository.decode().removesuffix(".git") == REPOSITORY,
            f"unapproved repository reference in {name}",
        )


def links_work_from_pypi(markdown: str, name: str) -> None:
    """PyPI renders the README away from the repository: a relative link or an in-page anchor goes
    nowhere there, so every link is absolute."""
    for target in re.findall(r"\]\(\s*<?([^)\s>]*)", markdown):
        require(
            target.startswith("https://"), f"link that does not work from PyPI in {name}: {target}"
        )


def requirement(value: str) -> str:
    return value.replace(" ", "").replace("'", '"')


def wheel_metadata(values: Message, version: str, readme: str) -> None:
    require(values["Name"] == NAME and values["Version"] == version, "wheel name/version differs")
    require(values["Requires-Python"] == ">=3.11", "wheel Requires-Python differs")
    require(values["License-Expression"] == "Apache-2.0", "wheel license expression differs")
    require(values.get_all("License-File", []) == ["LICENSE"], "wheel license file differs")
    require(
        {requirement(r) for r in values.get_all("Requires-Dist", [])} == REQUIRES,
        "wheel dependencies differ",
    )
    require(set(values.get_all("Provides-Extra", [])) == EXTRAS, "wheel extras differ")
    require(values["Summary"], "wheel summary missing")
    require(values["Description-Content-Type"] == "text/markdown", "wheel readme type differs")
    description = values.get_payload()
    require(isinstance(description, str) and description.strip(), "wheel description missing")
    assert isinstance(description, str)
    require(description.strip() == readme.strip(), "wheel description is not the README")
    links_work_from_pypi(description, "the wheel's description")
    urls = values.get_all("Project-URL", [])
    require(urls, "wheel project URLs missing")
    for url in urls:
        target = url.split(",", 1)[1].strip()
        require(
            target.startswith(f"https://github.com/{REPOSITORY}"),
            f"project URL outside this repository: {url}",
        )


def inspect(directory: Path, version: str) -> list[Path]:
    """The two artifacts' names, members, metadata and the public boundary; and that the wheel holds
    exactly the sdist's package sources, byte for byte."""
    wheels = list(directory.glob("*.whl"))
    sdists = list(directory.glob("*.tar.gz"))
    require(len(wheels) == len(sdists) == 1, "expected one wheel and one sdist")
    wheel, sdist = wheels[0], sdists[0]
    stem = f"{MODULE}-{version}"
    require(wheel.name == f"{stem}-py3-none-any.whl", "wheel filename differs")
    require(sdist.name == f"{stem}.tar.gz", "sdist filename differs")
    license_text = (ROOT / "LICENSE").read_bytes()

    with tarfile.open(sdist) as archive:
        members = archive.getmembers()
        require(len({m.name for m in members}) == len(members), "duplicate sdist members")
        roots = {Path(m.name).parts[0] for m in members}
        require(roots == {stem}, "sdist must have one root, named for the version")
        files: dict[str, bytes] = {}
        for member in members:
            require(
                not member.name.startswith("/") and ".." not in Path(member.name).parts,
                "unsafe sdist member",
            )
            require(member.isfile() or member.isdir(), "non-regular sdist member")
            if member.isfile():
                stream = archive.extractfile(member)
                require(stream is not None, "unreadable sdist member")
                assert stream is not None
                data = stream.read()
                public_bytes(data, member.name)
                files[Path(member.name).relative_to(stem).as_posix()] = data
    package = f"src/{MODULE}/"
    require(
        {p for p in files if not p.startswith(package)} == SDIST_FILES,
        "sdist holds files beyond the package, its metadata, README and LICENSE",
    )
    require(files["LICENSE"] == license_text, "sdist license is not the repository's")
    project = tomllib.loads(files["pyproject.toml"].decode())
    require(
        project["project"]["name"] == NAME and project["project"]["version"] == version,
        "sdist name/version differs",
    )
    check_project(project)
    readme = files["README.md"].decode()
    links_work_from_pypi(readme, "the sdist's README")
    sources = {p.removeprefix("src/"): data for p, data in files.items() if p.startswith(package)}
    require(f"{MODULE}/py.typed" in sources, "sdist type marker missing")
    require(not any("__pycache__" in p or p.endswith(".pyc") for p in sources), "sdist bytecode")

    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        require(len(names) == len(set(names)), "duplicate wheel members")
        info = f"{stem}.dist-info/"
        for name in names:
            require(
                not name.startswith("/") and ".." not in Path(name).parts, "unsafe wheel member"
            )
            require(
                name.startswith((f"{MODULE}/", info)), f"wheel member outside the package: {name}"
            )
            public_bytes(archive.read(name), name)
        require(
            {n for n in names if n.startswith(info)}
            == {f"{info}{n}" for n in ("METADATA", "WHEEL", "RECORD", "licenses/LICENSE")},
            "wheel metadata files differ",
        )
        require(archive.read(f"{info}licenses/LICENSE") == license_text, "wheel license differs")
        # Core metadata is UTF-8 (the README in it has ≥, →, …): parsed as text, not as bytes.
        values = Parser().parsestr(archive.read(f"{info}METADATA").decode("utf-8"))
        wheel_metadata(values, version, readme)
        generator = f"Generator: {BUILD_BACKEND.replace('==', ' ')}"
        require(
            generator in archive.read(f"{info}WHEEL").decode().splitlines(),
            f"the wheel was not built by {BUILD_BACKEND}",
        )
        packaged = {n: archive.read(n) for n in names if n.startswith(f"{MODULE}/")}
    require(packaged == sources, "the wheel's package is not the sdist's sources")
    return [wheel, sdist]


def check_project(project: dict[str, Any]) -> None:
    """The project as committed and as published: the backend requirement the project declares,
    never the release build's pin, and no source overrides."""
    require(
        project["build-system"]["requires"] == [BACKEND_REQUIREMENT],
        f"build-system requires is not [{BACKEND_REQUIREMENT!r}]",
    )
    require("sources" not in project.get("tool", {}).get("uv", {}), "source override present")


def clean_uv_output(directory: Path) -> None:
    ignore = directory / ".gitignore"
    if ignore.exists():
        require(ignore.is_file() and not ignore.is_symlink(), "unexpected generated ignore file")
        require(ignore.read_bytes() == b"*", "unexpected generated ignore contents")
        ignore.unlink()


def source_version() -> str:
    project = tomllib.loads((ROOT / PROJECT / "pyproject.toml").read_text())
    version: str = project["project"]["version"]
    return version


def build(directory: Path, version: str) -> None:
    require(not directory.exists(), "artifact output already exists")
    require(not directory.is_relative_to(ROOT), "artifact output must be outside the checkout")
    require(
        not subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT).strip(),
        "release source must be clean and committed",
    )
    directory.mkdir(parents=True)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    epoch = subprocess.check_output(
        ["git", "show", "-s", "--format=%ct", "HEAD"], cwd=ROOT, text=True
    ).strip()
    env = dict(os.environ, UV_EXCLUDE_NEWER=CUTOFF, SOURCE_DATE_EPOCH=epoch)
    with tempfile.TemporaryDirectory(prefix="durable-python-release-") as temporary:
        stage = Path(temporary)
        archive = stage / "source.tar"
        with archive.open("wb") as output:
            run(["git", "archive", "HEAD", PROJECT, "LICENSE"], cwd=ROOT, stdout=output)
        with tarfile.open(archive) as source:
            source.extractall(stage, filter="data")
        require(
            (stage / PROJECT / "LICENSE").read_bytes() == (stage / "LICENSE").read_bytes(),
            "python/LICENSE differs from the repository's LICENSE",
        )
        project = stage / PROJECT / "pyproject.toml"
        require(
            tomllib.loads(project.read_text())["project"]["version"] == version,
            "tag and source version differ",
        )
        module_version = (stage / PROJECT / "src" / MODULE / "version.py").read_text()
        require(
            re.search(rf'^VERSION = "{re.escape(version)}"$', module_version, re.M),
            "the module's VERSION differs from pyproject.toml's",
        )
        check_project(tomllib.loads(project.read_text()))
        constraints = stage / "build-constraints.txt"
        constraints.write_text(BUILD_BACKEND + "\n")
        pinned = ["--build-constraints", str(constraints)]
        run(
            ["uv", "build", "--sdist", *pinned, "--out-dir", str(directory), str(stage / PROJECT)],
            env=env,
        )
        (sdist,) = directory.glob("*.tar.gz")
        unpack = stage / "unpack"
        unpack.mkdir()
        with tarfile.open(sdist) as source:
            source.extractall(unpack, filter="data")
        (project_root,) = unpack.iterdir()
        run(
            ["uv", "build", "--wheel", *pinned, "--out-dir", str(directory), str(project_root)],
            env=env,
        )
    clean_uv_output(directory)
    artifacts = inspect(directory, version)
    record = {
        "schema": 1,
        "name": NAME,
        "version": version,
        "source_commit": commit,
        "external_cutoff": CUTOFF,
        "build_backend": BUILD_BACKEND,
        "artifacts": {p.name: sha256(p) for p in artifacts},
    }
    (directory / "artifacts.json").write_text(json.dumps(record, indent=2) + "\n")
    print(f"Built and inspected {len(artifacts)} distributions from {commit}")


def verify(directory: Path, version: str, install: bool) -> None:
    record = json.loads((directory / "artifacts.json").read_text())
    require(
        record["schema"] == 1 and record["name"] == NAME and record["version"] == version,
        "artifact record differs",
    )
    require(
        record["external_cutoff"] == CUTOFF and record["build_backend"] == BUILD_BACKEND,
        "release policy differs",
    )
    require(re.fullmatch(r"[0-9a-f]{40}", record["source_commit"]), "invalid source revision")
    if expected_source := os.environ.get("GITHUB_SHA"):
        require(
            record["source_commit"] == expected_source,
            "artifact source differs from the workflow revision",
        )
    require(len(record["artifacts"]) == 2, "artifact count differs")
    for name, digest in record["artifacts"].items():
        require(
            Path(name).name == name and not (directory / name).is_symlink(),
            "unsafe artifact filename",
        )
        require(sha256(directory / name) == digest, "artifact digest differs")
    require(
        {p.name for p in directory.iterdir()} == set(record["artifacts"]) | {"artifacts.json"},
        "unexpected artifact files",
    )
    artifacts = inspect(directory, version)
    count = 0
    if install:
        env = dict(os.environ, UV_EXCLUDE_NEWER=CUTOFF)
        for py in PYTHONS:
            for artifact in artifacts:
                with tempfile.TemporaryDirectory(prefix="durable-python-install-") as temporary:
                    venv = Path(temporary) / ".venv"
                    python = str(venv / "bin" / "python")
                    smoke_command = [python, "-I", str(Path(__file__).resolve()), "smoke"]
                    smoke_command += ["--version", version]
                    run(["uv", "venv", "--python", py, str(venv)], env=env)
                    # The package alone: the core has no crypto dependency.
                    run(["uv", "pip", "install", "--python", python, str(artifact)], env=env)
                    run(smoke_command, cwd=temporary)
                    # The same artifact with its `crypto` extra, as a user names it.
                    extra = f"{NAME}[crypto] @ {artifact.resolve().as_uri()}"
                    run(["uv", "pip", "install", "--python", python, extra], env=env)
                    run([*smoke_command, "--crypto"], cwd=temporary)
                    count += 1
    print(f"Verified {len(artifacts)} artifact digests; completed {count} clean installs")


def check_suite(report: Path) -> None:
    """The source suite without an engine: no failure and no error, at least one unit test passed
    and at most a handful skipped, and every integration test collected and skipped, most for want
    of an engine (a skip, never a pass of zero). The skips' reasons are printed with their counts."""
    tree = ElementTree.parse(report)
    passed = skipped = xfailed = 0
    reasons: dict[str, int] = {}
    for case in tree.iter("testcase"):
        outcomes = [child for child in case if child.tag in {"failure", "error", "skipped"}]
        name = f"{case.get('classname')}::{case.get('name')}"
        for outcome in outcomes:
            require(outcome.tag == "skipped", f"{name}: {outcome.tag}")
        if "integration" in (case.get("classname") or "").split("."):
            require(outcomes, f"{name} ran without an engine")
            reason = outcomes[0].get("message") or ""
            reason = NO_ENGINE if NO_ENGINE in reason else reason
            reasons[reason] = reasons.get(reason, 0) + 1
        elif not outcomes:
            passed += 1
        elif outcomes[0].get("type") == "pytest.xfail":
            xfailed += 1
        else:
            skipped += 1
    require(passed > 0, "no unit test passed")
    require(skipped <= MAX_UNIT_SKIPS, f"{skipped} unit tests skipped, more than {MAX_UNIT_SKIPS}")
    total = sum(reasons.values())
    require(
        reasons.get(NO_ENGINE, 0) * 2 > total,
        "the integration tests did not skip for want of an engine",
    )
    print(
        f"unit: {passed} passed, {skipped} skipped, {xfailed} xfailed; integration: {total} skipped"
    )
    for reason, count in sorted(reasons.items(), key=lambda item: -item[1]):
        print(f"  {count:4d}  {reason}")


def smoke(version: str, crypto: bool) -> None:
    """Run inside a clean install: the installed package, its version, and what needs no engine
    and no network (a client and a worker built, never started; a run and its replay in memory)."""

    def no_network(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("the smoke opened a network connection")

    socket.socket.connect = no_network  # type: ignore[method-assign]
    socket.socket.connect_ex = no_network  # type: ignore[method-assign,assignment]

    import synadia_durable

    require(importlib.metadata.version(NAME) == version, "installed version differs")
    require(version == synadia_durable.VERSION, "the module's VERSION differs")
    require(
        Path(synadia_durable.__file__).resolve().is_relative_to(Path(sys.prefix).resolve()),
        "the package imported is not the installed one",
    )
    importlib.metadata.version("nats-py")
    has_crypto = importlib.util.find_spec("cryptography") is not None
    require(has_crypto == crypto, "cryptography installed without the extra, or missing with it")
    asyncio.run(_smoke(synadia_durable, crypto))
    print(
        f"{NAME} {version}: imported, built a client and a worker, replayed a run in memory"
        + (", round-tripped a payload through aes_gcm_payload_codec" if crypto else "")
    )


async def _smoke(sd: Any, crypto: bool) -> None:
    from nats.aio.client import Client as NatsClient
    from synadia_durable.core.execute import execute_run
    from synadia_durable.journal.entry import TERMINAL_KINDS, MsgId, stamp
    from synadia_durable.journal.store import InMemoryJournal, JournalAppend

    calls: list[str] = []

    async def greet(ctx: Any, name: str) -> str:
        def upper() -> str:
            calls.append("upper")
            return name.upper()

        shouted = await ctx.run("upper", upper)
        return f"hello {shouted}"

    workflow = sd.define_workflow("smoke", greet)
    nc = NatsClient()  # never connected
    require(isinstance(sd.create_client(nc), sd.Client), "create_client")
    require(isinstance(sd.create_worker(nc, [workflow]), sd.Worker), "create_worker")
    require(sd.with_reconnect_defaults()["max_reconnect_attempts"] == -1, "reconnect defaults")

    journal = InMemoryJournal()
    start = stamp({"kind": "start", "seq": 0, "wf": "smoke", "wfVersion": 1, "input": "ada"}, 1)
    await journal.append("r1", JournalAppend(start, MsgId.start("r1"), 0))

    async def drive(replay: bool) -> Any:
        loaded = await journal.load("r1")
        if replay:  # as if a worker crashed after the step, before the terminal: replay it
            loaded = [le for le in loaded if le.entry["kind"] not in TERMINAL_KINDS]
        return await execute_run("r1", workflow=workflow, journal=journal, loaded=loaded)

    first = await drive(replay=False)
    require(first.kind == "completed" and first.value == "hello ADA", "the run did not complete")
    again = await drive(replay=True)
    require(again.kind == "completed" and again.value == "hello ADA", "the replay differs")
    require(calls == ["upper"], "a journaled step ran again on replay")

    if crypto:
        codec = sd.aes_gcm_payload_codec(bytes(range(32)))
        sealed = await codec.encode(b"payload")
        require(sealed != b"payload" and await codec.decode(sealed) == b"payload", "codec")
    else:
        try:
            sd.aes_gcm_payload_codec(bytes(32))
        except sd.PayloadCodecError as e:
            require("synadia-durable[crypto]" in str(e), "the codec's error names no extra")
        else:
            raise ValueError("aes_gcm_payload_codec worked without cryptography")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("version", help="print python/pyproject.toml's version")
    for name in ("build", "verify"):
        command = commands.add_parser(name)
        command.add_argument("--version", required=True)
        command.add_argument("--artifacts", type=Path, required=True)
        if name == "verify":
            command.add_argument("--install", action="store_true")
    command = commands.add_parser("check-suite")
    command.add_argument("--junit", type=Path, required=True)
    command = commands.add_parser("smoke")
    command.add_argument("--version", required=True)
    command.add_argument("--crypto", action="store_true")
    args = parser.parse_args()
    if args.command == "version":
        print(source_version())
        return
    if args.command == "check-suite":
        check_suite(args.junit)
        return
    require(re.fullmatch(r"\d+\.\d+\.\d+", args.version), "invalid package version")
    if args.command == "smoke":
        smoke(args.version, args.crypto)
    elif args.command == "build":
        build(args.artifacts.resolve(), args.version)
    else:
        verify(args.artifacts.resolve(), args.version, args.install)


if __name__ == "__main__":
    main()
