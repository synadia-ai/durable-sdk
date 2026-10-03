"""synadia-durable — batched DIRECT GET: every message on one subject (or subject filter —
wildcards are accepted), in stream order, in as few requests as the server allows, and NO consumer.
Mirrors ``src/nats/direct.ts``.

Used for journal loads, blob chunk reads and the parked-index sweep (LEARNINGS #58, #59). nats-py has no batched direct
get, so this is the raw request: publish to ``$JS.API.DIRECT.GET.<stream>`` with an inbox reply and
drain it. Each message the server returns carries the number still pending on the subject after
it, so a subject longer than one batch is read in consecutive requests that resume from the last
sequence seen; the server ends every batch with a ``204 EOB`` status, and an empty subject is a
``404`` ("message not found"), which is simply "nothing there". Requires ``allow_direct`` on the
stream (provisioned and reconciled by resources.py) and nats-server >= 2.11.
"""

from __future__ import annotations

import contextlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nats.aio.client import Client as NatsClient

__all__ = ["DirectRead", "read_subject"]

#: Messages per request; a longer subject is read in consecutive batches.
DEFAULT_BATCH = 1024
#: How long to wait for the next message of a batch before treating the read as failed.
READ_TIMEOUT_S = 5.0


@dataclass(frozen=True, slots=True)
class DirectRead:
    """One message of a direct read: payload, stream sequence, subject, and its headers."""

    data: bytes
    seq: int
    #: The message's subject (a KV key's subject is ``$KV.<bucket>.<key>``).
    subject: str
    #: For a KV bucket's stream: the ``KV-Operation`` header — "" for a value, DEL/PURGE for a marker.
    kv_operation: str


async def read_subject(
    nc: NatsClient,
    stream: str,
    subject: str,
    *,
    batch: int = DEFAULT_BATCH,
    timeout_s: float = READ_TIMEOUT_S,
) -> list[DirectRead]:
    """Read a whole subject.

    The result is a consistent prefix of the subject as the answering server saw it — a follower
    may answer on a replicated stream — so callers that must not act on a prefix verify completeness
    against a leader-served count (journal_store.py) or a known size (blob_store.py).
    """
    out: list[DirectRead] = []
    seq = 1
    api_subject = f"$JS.API.DIRECT.GET.{stream}"
    while True:
        delivered = 0
        pending = 0
        last = 0
        inbox = nc.new_inbox()
        sub = await nc.subscribe(inbox)
        try:
            await nc.publish(
                api_subject,
                json.dumps({"seq": seq, "batch": batch, "next_by_subj": subject}).encode(),
                reply=inbox,
            )
            while True:
                msg = await sub.next_msg(timeout=timeout_s)
                headers = msg.headers or {}
                status = headers.get("Status")
                if status is not None:
                    if status == "204":  # end of batch
                        break
                    if status == "404" and not out:  # no messages on the subject
                        return out
                    raise RuntimeError(
                        f"direct get {api_subject} {subject}: status {status} "
                        f"{headers.get('Description', '')}".rstrip()
                    )
                stream_seq = int(headers.get("Nats-Sequence", "0"))
                out.append(
                    DirectRead(
                        data=bytes(msg.data),
                        seq=stream_seq,
                        subject=str(headers.get("Nats-Subject", "")),
                        kv_operation=str(headers.get("KV-Operation", "")),
                    )
                )
                delivered += 1
                pending = int(headers.get("Nats-Num-Pending", "0"))
                last = stream_seq
        finally:
            with contextlib.suppress(Exception):
                await sub.unsubscribe()
        if delivered == 0 or pending <= 0:
            return out
        seq = last + 1
