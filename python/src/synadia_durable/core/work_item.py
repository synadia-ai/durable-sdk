"""synadia-durable — the on-queue work-item shape (``DE_WORK``). PROTOCOL §6.1.

One durable queue drives dispatch, resumes, timer and signal wakes, and child-done deliveries. Like
the journal entries, this TypedDict IS the JSON.

The SHAPE lives in the pure core because the core is what turns an item into journal entries
(``inbound.py``); what puts one on a queue — and shrinks its payload to a blob ref on the way — is
the shell's business (``_nats/protocol.py``). Mirrors ``src/core/work-item.ts``.
"""

from __future__ import annotations

from typing import Any, Literal, NotRequired, TypedDict

from synadia_durable.journal.entry import BlobRef, ErrorShape

__all__ = ["ChildDone", "SignalPayload", "WorkItem", "WorkItemKind"]

WorkItemKind = Literal["start", "resume", "timer", "signal", "child-done", "cancel"]


class SignalPayload(TypedDict):
    """The inbound signal on a ``signal`` item (``id`` gives idempotency)."""

    name: str
    data: NotRequired[Any]
    #: Replaces ``data`` when the payload is offloaded; the worker journals the ref verbatim.
    dataRef: NotRequired[BlobRef]
    id: str


class ChildDone(TypedDict):
    """The finished sub-workflow's outcome on a ``child-done`` item (``id`` = childRunId)."""

    id: str
    ok: bool
    value: NotRequired[Any]
    valueRef: NotRequired[BlobRef]
    error: NotRequired[ErrorShape]
    errorRef: NotRequired[BlobRef]


class WorkItem(TypedDict):
    """A unit of work on ``de.work.{workflow}``."""

    runId: str
    kind: WorkItemKind
    #: For ``timer`` / ``child-done``: the command seq this wake completes.
    resolvesSeq: NotRequired[int]
    #: For ``cancel``: an optional reason, carried onto the ``cancel-requested`` entry.
    reason: NotRequired[str]
    signal: NotRequired[SignalPayload]
    child: NotRequired[ChildDone]
    #: True when the blob this item's ``*Ref`` points at holds payload-codec output rather than the
    #: plain encoded value — so the worker stamps the journal entry it builds ``v:3``, not ``v:2``.
    #: Absent on every item written without a codec, which is why enabling one changes no bytes.
    payloadEnc: NotRequired[Literal[True]]
