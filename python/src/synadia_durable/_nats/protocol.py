"""synadia-durable — putting a work item on the queue (``DE_WORK``). PROTOCOL §6.1.

One durable queue drives dispatch, resumes, timer and signal wakes, and child-done deliveries; the
item's SHAPE is the core's (``core/work_item.py``, re-exported here). Mirrors
``src/nats/protocol.ts``.

The item ITSELF is plaintext even under a ``payload_codec``: it lives on a workqueue stream until
it is consumed, and the engine dispatches on its fields. The BLOB an oversized item offloads to is
not — it stays in ``DE_BLOBS`` until GC, which is the "at rest" the codec exists for — so this path
transforms what it offloads and says so with ``payloadEnc``, or whether a payload was encrypted
would depend on its size.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from synadia_durable._internal.json import encode_json
from synadia_durable.core.work_item import ChildDone, SignalPayload, WorkItem, WorkItemKind
from synadia_durable.journal.blobs import store_payload_blob

if TYPE_CHECKING:
    from synadia_durable.journal.blobs import BlobStore
    from synadia_durable.journal.codec import Codec
    from synadia_durable.journal.payload_codec import PayloadCodec

__all__ = ["ChildDone", "SignalPayload", "WorkItem", "WorkItemKind", "shrink_work_item"]


async def shrink_work_item(
    item: WorkItem,
    codec: Codec,
    blobs: BlobStore | None,
    threshold_bytes: int | None,
    payload_codec: PayloadCodec | None = None,
) -> WorkItem:
    """Offload an over-threshold work item's payload slot, so the queue carries a ref instead.

    The blob is named under ``item.runId`` — the run whose journal will carry the ref verbatim, and
    whose GC will therefore reclaim it. A no-op when under threshold, when offload is off, when the
    kind carries no payload, or when the slot is already a ref (a re-publish of somebody else's
    shrunken item).

    Without this, a signal or a child's result near ``max_payload`` would fail its publish and
    NAK-loop forever — the queue has the same size limit as the journal, and the value arrived from
    a place that had already offloaded it once.

    With a ``payload_codec`` the blob holds the codec's output and the item carries
    ``payloadEnc: True``. Without one nothing changes, byte for byte.
    """
    if blobs is None or threshold_bytes is None:
        return item
    if len(encode_json(item)) <= threshold_bytes:
        return item

    run_id = item["runId"]

    def marked(shrunk: WorkItem, transformed: bool) -> WorkItem:
        """Tag the item from what the WRITE actually did, never from a parallel boolean: the flag
        and the bytes cannot drift apart if only one of them exists."""
        if transformed:
            shrunk["payloadEnc"] = True
        return shrunk

    if item["kind"] == "signal":
        signal = item.get("signal")
        if signal is not None and signal.get("data") is not None and "dataRef" not in signal:
            ref, transformed = await store_payload_blob(
                run_id,
                f'the data of signal "{signal["name"]}" (work item)',
                signal["data"],
                codec,
                blobs,
                payload_codec,
            )
            shrunk: SignalPayload = {"name": signal["name"], "id": signal["id"], "dataRef": ref}
            return marked({**item, "signal": shrunk}, transformed)
    elif item["kind"] == "child-done":
        child = item.get("child")
        if child is not None and child["ok"] and child.get("value") is not None:
            if "valueRef" in child:
                return item
            ref, transformed = await store_payload_blob(
                run_id,
                f"the value of child {child['id']}'s outcome (work item)",
                child["value"],
                codec,
                blobs,
                payload_codec,
            )
            done: ChildDone = {"id": child["id"], "ok": True, "valueRef": ref}
            return marked({**item, "child": done}, transformed)
        if child is not None and not child["ok"] and child.get("error") is not None:
            if "errorRef" in child:
                return item
            ref, transformed = await store_payload_blob(
                run_id,
                f"the error of child {child['id']}'s outcome (work item)",
                child["error"],
                codec,
                blobs,
                payload_codec,
            )
            failed: ChildDone = {"id": child["id"], "ok": False, "errorRef": ref}
            return marked({**item, "child": failed}, transformed)
    return item
