"""synadia-durable — the on-stream journal wire layout (language-neutral, frozen at ``v:1``).

PROTOCOL §5. One ordered log per run on subject ``de.journal.{runId}``.

``seq`` is the COMMAND position (monotonic per run, 0, 1, 2, …). A two-phase step shares one ``seq``
across its ``step-begin`` and ``step-end``. ``signal``, ``resolve``, ``cancel-requested`` and the
terminals do not consume a command position (a ``signal`` is matched to an await by name and order; a
``resolve`` references ``resolvesSeq``); they carry ``seq = NON_COMMAND_SEQ``.

**These TypedDicts are the wire, not a model of it.** Field names are the JSON keys verbatim — hence
camelCase (``wfVersion``, ``resolvesSeq``, ``childRunId``) in Python source — and an entry is encoded
by handing the dict straight to the codec, with no mapping layer that could drift from the spec. The
``kind`` literals make these a discriminated union, so ``mypy`` narrows on ``entry["kind"]`` exactly
as TypeScript narrows on the interface union it mirrors (``src/journal/entry.ts``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, NotRequired, TypedDict, cast

from synadia_durable.version import WIRE_VERSION

__all__ = [
    "NON_COMMAND_SEQ",
    "AwaitEntry",
    "BlobRef",
    "CallEntry",
    "CancelRequestedEntry",
    "CancelledEntry",
    "ContinuedEntry",
    "EntryKind",
    "ErrorShape",
    "FailEntry",
    "JournalEntry",
    "LoadedEntry",
    "MsgId",
    "ParentLink",
    "ResolveEntry",
    "ResultEntry",
    "SignalEntry",
    "SleepEntry",
    "StartEntry",
    "StepBeginEntry",
    "StepEndEntry",
    "ValueEntry",
    "stamp",
]

#: The ``seq`` carried by entries that do not occupy a command position.
NON_COMMAND_SEQ = -1

EntryKind = Literal[
    "start",
    "step-begin",
    "step-end",
    "now",
    "random",
    "uuid",
    "sleep",
    "await",
    "signal",
    "call",
    "resolve",
    "result",
    "fail",
    "continued",
    "cancel-requested",
    "cancelled",
]


class BlobRef(TypedDict):
    """A content-addressed reference to a payload blob offloaded to ``DE_BLOBS`` (PROTOCOL §5.6).

    When an encoded entry would exceed the offload threshold, its single payload slot (``input`` /
    ``value`` / ``data`` / ``error``) is written to the blob store FIRST (blob-before-pointer: a
    published ref always resolves) and the entry carries this ref in the matching ``*Ref`` field
    instead, stamped ``v:2``. Names are content-addressed (``{runId}/{digest}``) so two CAS racers
    holding DIFFERENT bytes for the same seq can never overwrite each other — the journal CAS picks
    the surviving ref and the loser's blob is an inert orphan. Readers MUST verify ``digest``.
    """

    #: Object name in the blob bucket: ``{runId}/{digest}`` (the runId prefix is the GC unit).
    name: str
    #: Digest of the encoded value, ``sha256:<64 lowercase hex>``. Readers MUST verify.
    digest: str
    #: Size in bytes of the encoded value blob.
    size: int


class ErrorShape(TypedDict):
    """How an error crosses the wire. Deliberately just two strings — an error is data here."""

    name: str
    message: str


class ParentLink(TypedDict):
    """Set on a sub-workflow's ``start``: where to deliver its result on completion (PROTOCOL §9)."""

    runId: str
    seq: int
    wf: str


class _Base(TypedDict):
    """Fields every entry carries.

    ``v`` and ``ts`` are ``NotRequired`` only so that :func:`stamp` can be the single place that sets
    them — a decoded entry always has both, and the codec rejects one that does not.
    """

    seq: int
    v: NotRequired[int]
    ts: NotRequired[int]


class StartEntry(_Base):
    """The first entry of every run (``seq`` 0)."""

    kind: Literal["start"]
    wf: str
    wfVersion: int
    #: Absent on the wire when offloaded (``v:2``); ``inputRef`` locates it. Resolved on load.
    input: NotRequired[Any]
    inputRef: NotRequired[BlobRef]
    #: Transformed form (v:3): ``input`` is absent and this holds the base64 codec output.
    inputEnc: NotRequired[str]
    parent: NotRequired[ParentLink]
    #: continue-as-new chain: this run's generation (0 for the original).
    generation: NotRequired[int]
    #: continue-as-new chain: the logical (generation-0) runId all generations share.
    base: NotRequired[str]


class StepBeginEntry(_Base):
    """A step's intent, appended BEFORE the side effect runs."""

    kind: Literal["step-begin"]
    name: str
    #: Idempotency key for the side effect: ``{runId}:{seq}`` (exposed via ``ctx.key()``).
    key: str


class StepEndEntry(_Base):
    """A step's recorded result (same ``seq`` as its ``step-begin``)."""

    kind: Literal["step-end"]
    name: str
    value: NotRequired[Any]
    valueRef: NotRequired[BlobRef]
    #: Transformed form (v:3): ``value`` is absent and this holds the base64 codec output.
    valueEnc: NotRequired[str]


class ValueEntry(_Base):
    """A captured deterministic value (``now`` / ``random`` / ``uuid``)."""

    kind: Literal["now", "random", "uuid"]
    value: Any


class SleepEntry(_Base):
    """A durable timer command."""

    kind: Literal["sleep"]
    name: str
    #: Absolute deadline (ms since epoch), deterministic once recorded.
    dueMs: int
    #: Reserved by the protocol and written by nobody — the schedule subject is derivable from
    #: ``runId`` + ``seq``. Present here so a foreign writer's entry still decodes.
    schedSubj: NotRequired[str]


class AwaitEntry(_Base):
    """Block until a signal of ``name`` arrives (wire kind ``await``; ``ctx.wait`` in Python)."""

    kind: Literal["await"]
    name: str
    #: Absolute deadline when a timeout was given; a timer races the signal.
    dueMs: NotRequired[int]


class SignalEntry(_Base):
    """A delivered external signal (non-command)."""

    kind: Literal["signal"]
    name: str
    data: NotRequired[Any]
    dataRef: NotRequired[BlobRef]
    #: Transformed form (v:3): ``data`` is absent and this holds the base64 codec output.
    dataEnc: NotRequired[str]
    #: Dedup id so a redelivered signal is appended at most once.
    signalId: str


class CallEntry(_Base):
    """Invoke a sub-workflow."""

    kind: Literal["call"]
    name: str
    childRunId: str
    wf: str
    input: NotRequired[Any]
    inputRef: NotRequired[BlobRef]
    #: Transformed form (v:3): ``input`` is absent and this holds the base64 codec output.
    inputEnc: NotRequired[str]


class ResolveEntry(_Base):
    """Completes a ``sleep`` / ``await`` / ``call`` (non-command).

    Neither ``value`` nor ``error`` completes a fired ``sleep`` or a timed-out ``await``; one of them
    completes a ``call``.
    """

    kind: Literal["resolve"]
    resolvesSeq: int
    value: NotRequired[Any]
    valueRef: NotRequired[BlobRef]
    #: Transformed form (v:3): ``value`` is absent and this holds the base64 codec output.
    valueEnc: NotRequired[str]
    error: NotRequired[ErrorShape]
    errorRef: NotRequired[BlobRef]
    #: Transformed form (v:3): ``error`` is absent and this holds the base64 codec output.
    errorEnc: NotRequired[str]


class ResultEntry(_Base):
    """Terminal: the run returned."""

    kind: Literal["result"]
    value: NotRequired[Any]
    valueRef: NotRequired[BlobRef]
    #: Transformed form (v:3): ``value`` is absent and this holds the base64 codec output.
    valueEnc: NotRequired[str]


class FailEntry(_Base):
    """Terminal: the run failed with a logic error."""

    kind: Literal["fail"]
    error: NotRequired[ErrorShape]
    errorRef: NotRequired[BlobRef]
    #: Transformed form (v:3): ``error`` is absent and this holds the base64 codec output.
    errorEnc: NotRequired[str]


class ContinuedEntry(_Base):
    """Terminal for this generation: continue-as-new started a fresh generation."""

    kind: Literal["continued"]
    #: The next generation's runId (deterministic: ``{base}_g{generation}``).
    nextRunId: str
    input: NotRequired[Any]
    inputRef: NotRequired[BlobRef]
    #: Transformed form (v:3): ``input`` is absent and this holds the base64 codec output.
    inputEnc: NotRequired[str]
    #: The next generation's number (this run's generation + 1).
    generation: int
    #: The chain's logical (generation-0) runId.
    base: str
    #: The next generation's workflow version. Optional and additive: absent on pre-versioning
    #: entries, which coalesce to the current generation's version on re-drive. Recorded so a crash
    #: mid-rollover re-drives to the SAME version (PROTOCOL §5.3, §15).
    version: NotRequired[int]


class CancelRequestedEntry(_Base):
    """Cancellation was requested for this run (non-command; matched by stream-seq, like a signal)."""

    kind: Literal["cancel-requested"]
    reason: NotRequired[str]


class CancelledEntry(_Base):
    """Terminal: the run was hard-stopped at a command boundary."""

    kind: Literal["cancelled"]
    reason: NotRequired[str]


JournalEntry = (
    StartEntry
    | StepBeginEntry
    | StepEndEntry
    | ValueEntry
    | SleepEntry
    | AwaitEntry
    | SignalEntry
    | CallEntry
    | ResolveEntry
    | ResultEntry
    | FailEntry
    | ContinuedEntry
    | CancelRequestedEntry
    | CancelledEntry
)

#: The kinds that end a run. Exactly one may appear in a journal (PROTOCOL §5.4).
TERMINAL_KINDS: frozenset[str] = frozenset({"result", "fail", "continued", "cancelled"})


@dataclass(frozen=True, slots=True)
class LoadedEntry:
    """A journal entry together with its JetStream stream sequence (used to chain CAS appends)."""

    entry: JournalEntry
    stream_seq: int


def stamp(body: JournalEntry, now_ms: int, wire_version: int = WIRE_VERSION) -> JournalEntry:
    """Stamp the wire version and timestamp onto an entry body.

    The single place either is written, so ``v`` cannot drift between call sites. ``wire_version``
    is a parameter rather than a constant because an offloaded entry is stamped ``v:2``
    (PROTOCOL §5.6) — the *only* thing that distinguishes the two forms.
    """
    out = dict(cast("dict[str, object]", body))
    out["v"] = wire_version
    out["ts"] = now_ms
    # The dict is `body` plus two keys its TypedDict already declares. Taking and returning the
    # UNION (rather than a TypeVar) is deliberate: a bare dict literal at a call site has no
    # expected type to infer a TypeVar from, so every caller would have to name the entry type in a
    # local first. Against the union, the literal matches its member directly.
    return cast("JournalEntry", out)


class MsgId:
    """Per-entry ``Nats-Msg-Id`` conventions (PROTOCOL §5.4, Appendix B).

    Dedup ids, not identity: a retried publish lands at most once. The single ``{runId}:terminal``
    for all four terminal kinds is what guarantees a run records at most one terminal — a second
    writer attempting a different one is deduplicated away.
    """

    @staticmethod
    def start(run: str) -> str:
        return f"{run}:start"

    @staticmethod
    def step_begin(run: str, seq: int) -> str:
        return f"{run}:{seq}:b"

    @staticmethod
    def step_end(run: str, seq: int) -> str:
        return f"{run}:{seq}:e"

    @staticmethod
    def value(run: str, seq: int) -> str:
        return f"{run}:{seq}"

    @staticmethod
    def command(run: str, seq: int) -> str:
        return f"{run}:{seq}"

    @staticmethod
    def signal(run: str, signal_id: str) -> str:
        return f"{run}:sig:{signal_id}"

    @staticmethod
    def resolve(run: str, seq: int) -> str:
        return f"{run}:{seq}:r"

    @staticmethod
    def cancel(run: str) -> str:
        return f"{run}:cancel"

    @staticmethod
    def terminal(run: str) -> str:
        return f"{run}:terminal"
