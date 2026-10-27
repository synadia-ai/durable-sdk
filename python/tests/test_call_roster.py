"""The call-level roster, re-expressed for the thin SDK (F4, F15). Twin of
``test/unit/call-roster.test.ts``.

The library's suite of this name (``7456c75`` ``python/tests/test_call_roster.py``) pins every
method the library invokes on nats-py, by file, under ``_nats/`` and ``client.py``. That shell is
the engine's now and is not in this package, so the frozen suite cannot run here unchanged: its
rosters name files that do not exist. This is the same idea for what replaced it. The SDK speaks to
the broker in exactly one way, a request to ``{prefix}.api.v1.<op>``, from one file, through
nats-py's core alone. It creates no consumer, stream, bucket or subscription, publishes nothing,
and names no JetStream subject. A new call fails here, and whoever added it comes and says why —
the conversation the library's roster exists to force.

It also checks that ``pyproject.toml`` depends on ``nats-py`` alone, and that no file under
``src/`` imports its JetStream, KV or object-store modules.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "synadia_durable"
TESTS = ROOT / "tests"


def _sources(root: Path) -> dict[str, str]:
    return {
        p.relative_to(root).as_posix(): p.read_text(encoding="utf-8")
        for p in sorted(root.rglob("*.py"))
        if ".venv" not in p.parts and "__pycache__" not in p.parts
    }


LIB = _sources(SRC)
# The tests' own broker access, minus this file (it quotes every forbidden name) and the library's
# shell copied for staging and reading the streams (tests/shell/, the library's code, rostered by
# the library's own suite).
TEST_TREE = {
    p: t
    for p, t in _sources(TESTS).items()
    if p != "test_call_roster.py" and not p.startswith("shell/")
}


def _code(text: str) -> str:
    """Source with comments and docstrings stripped: a docstring MENTIONING ``watchall()`` is not
    a call."""
    text = re.sub(r'"""[\s\S]*?"""', "", text)
    text = re.sub(r"'''[\s\S]*?'''", "", text)
    return re.sub(r"(^|[^\"'])#[^\n]*", r"\1", text)


def _roster(files: dict[str, str], receiver: re.Pattern[str]) -> list[str]:
    out = []
    for path, text in files.items():
        methods = sorted({m.group(1) for m in receiver.finditer(_code(text))})
        if methods:
            out.append(f"{path}: {', '.join(methods)}")
    return out


NC = re.compile(r"(?<![\w])_?nc\s*\.\s*(\w+)\s*\(")
IMPORTS_NATS = re.compile(r"^\s*(?:import nats\b|from nats\b)", re.MULTILINE)

# --- where the broker is spoken to at all --------------------------------------------------------


def test_pyproject_depends_on_nats_py_alone() -> None:
    with (ROOT / "pyproject.toml").open("rb") as fh:
        project = tomllib.load(fh)["project"]
    assert [re.split(r"[<>=!~ \[]", d)[0] for d in project["dependencies"]] == ["nats-py"]
    # The one optional extra is the ready-made AES-GCM payload codec's crypto, as the library's.
    assert list(project.get("optional-dependencies", {})) == ["crypto"]


def test_no_file_under_src_imports_jetstream_kv_or_the_object_store() -> None:
    hits = [
        p
        for p, t in LIB.items()
        if re.search(
            r"^\s*(?:import nats\.js|from nats\.js|from nats import js)\b", t, re.MULTILINE
        )
        or re.search(r"\b(?:jetstream|jsm|key_value|object_store)\s*\(", _code(t))
    ]
    assert hits == []


def test_only_the_engine_client_imports_nats() -> None:
    importers = [p for p, t in LIB.items() if IMPORTS_NATS.search(t)]
    assert importers == ["_engine/client.py", "_engine/transport.py", "_engine/worker.py"]
    # The client and the worker import it for a type only; the transport alone at run time.
    for path in ("_engine/client.py", "_engine/worker.py"):
        assert re.findall(r"^(?:import nats|from nats)\b.*$", LIB[path], re.MULTILINE) == [], path


def test_every_request_goes_through_the_one_transport_to_prefix_api_v1_op() -> None:
    assert _roster(LIB, NC) == ["_engine/transport.py: request"]
    assert 'f"{prefix}.api.v1.{op}"' in _code(LIB["_engine/names.py"])
    assert re.search(r"_nc\.request\(api_subject\(self\._prefix, op\)", _code(LIB["_engine/transport.py"]))  # fmt: skip


def test_nothing_under_src_publishes_subscribes_or_names_a_jetstream_kv_or_object_subject() -> None:
    routes = [
        ("publish", r"\.publish\s*\("),
        ("subscribe", r"\.subscribe\s*\("),
        ("a JetStream API subject", r"\$JS\."),
        ("a KV subject", r"\$KV\."),
        ("an object-store subject", r"\$O\."),
        (
            "a journal/work/timer subject",
            r"[\"'](?:[A-Za-z0-9_-]+|\{[^}]+\})\.(?:journal|work|timer)\.",
        ),
    ]
    offenders = [f"{p}: {what}" for p, t in LIB.items() for what, re_ in routes if re.search(re_, _code(t))]  # fmt: skip
    assert offenders == []


# --- the consumer roster — the SDK creates no consumer at all ------------------------------------

FORBIDDEN = [
    ("KV watch (LEARNINGS #56)", r"\.watch\s*\("),
    ("KV watchall (LEARNINGS #59)", r"\.watchall\s*\("),
    ("KV history", r"\.history\s*\("),
    # KV keys() is a watch underneath. dict.keys() is synchronous and never awaited; the KV one is
    # a coroutine, so an AWAITED ``.keys(`` is the tell.
    ("KV keys (a watch underneath)", r"await\s+[^\n]*?\.keys\s*\("),
    ("ordered consumer (object-store get / KV watch)", r"ordered_consumer"),
]


def test_no_route_to_a_consumer_exists_anywhere_under_src() -> None:
    routes = [
        *FORBIDDEN,
        ("add_consumer", r"\badd_consumer\s*\("),
        ("pull subscribe", r"pull_subscribe(?:_bind)?\s*\("),
        ("fetch", r"\.fetch\s*\("),
    ]
    offenders = [f"{p}: {what}" for p, t in LIB.items() for what, re_ in routes if re.search(re_, _code(t))]  # fmt: skip
    assert offenders == []


def test_in_the_tests_a_consumer_is_created_only_where_a_test_says_why_and_who_deletes_it() -> None:
    """The library's rule, kept for the suites' own JetStream client (findings F13)."""
    allowed: dict[str, tuple[str, str, str]] = {
        # path: (the permitted call, its owner, why)
    }
    consumer_calls = [
        ("pull_subscribe (creates when unbound)", r"(?<!_)\bpull_subscribe\s*\("),
        ("JetStream push subscribe", r"(?:\bjs|_js|jetstream\(\))\s*\.\s*subscribe\s*\("),
        ("add_consumer", r"\badd_consumer\s*\("),
    ]
    offenders = []
    for path, text in TEST_TREE.items():
        c = _code(text)
        for what, re_ in [*FORBIDDEN, *consumer_calls]:
            if not re.search(re_, c):
                continue
            entry = allowed.get(path)
            if entry is None or not re.search(entry[0], what):
                offenders.append(f"{path}: {what}")
        entry = allowed.get(path)
        if entry is not None and not re.search(entry[1], c):
            offenders.append(f"{path}: owner missing ({entry[2]})")
    assert offenders == []
