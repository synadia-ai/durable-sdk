// @synadia-ai/durable — putting a work item on the queue (DE_WORK). One durable queue drives
// dispatch, resumes, timer/signal wakes, and child-done deliveries; the item's SHAPE is the core's
// (`core/work-item.ts`, re-exported here). See DESIGN.md §5.
//
// Work items ride the same max_payload as everything else, so the two payload-bearing kinds
// (signal data, child-done outcome) shrink to blob refs when oversized — the SAME content-addressed
// refs the journal uses, created once and passed through verbatim into the journal entry the worker
// appends (a big child result is stored once per owning run, referenced everywhere else).
//
// The item ITSELF is plaintext even under a `payloadCodec`: it lives on a workqueue stream until it
// is consumed, and the engine dispatches on its fields. The BLOB is not — it stays in DE_BLOBS
// until GC, which is the "at rest" the codec exists for — so this path transforms what it offloads
// and says so with `payloadEnc`, or whether a payload was encrypted would depend on its size.

import { type WorkItem } from "../core/work-item.js";
import { type BlobStore, storePayloadBlob } from "../journal/blobs.js";
import { type Codec } from "../journal/codec.js";
import { type PayloadCodec } from "../journal/payload-codec.js";
import { encodeJson } from "../internal/json.js";

export { type WorkItem };

/**
 * Shrink a work item whose encoding exceeds the threshold by offloading its payload slot
 * (signal.data / child.value / child.error) to the blob store, named under `item.runId` — the run
 * whose journal will carry the ref. No-op (returns the input) when under threshold, offload is
 * disabled, the kind carries no payload, or the slot is already a ref.
 *
 * With a `payloadCodec` the blob holds the codec's output and the item carries `payloadEnc: true`.
 * Without one nothing changes, byte for byte.
 */
export async function shrinkWorkItem(
  item: WorkItem,
  codec: Codec,
  offload: { blobs: BlobStore; thresholdBytes: number } | undefined,
  payloadCodec?: PayloadCodec,
): Promise<WorkItem> {
  if (!offload) return item;
  if (encodeJson(item).byteLength <= offload.thresholdBytes) return item;
  // `payloadEnc` is derived from what the write ACTUALLY did, never from a parallel boolean: the
  // flag and the bytes cannot drift apart if only one of them exists.
  const mark = (transformed: boolean) => (transformed ? ({ payloadEnc: true } as const) : {});

  if (item.kind === "signal" && item.signal?.data !== undefined && !item.signal.dataRef) {
    const s = item.signal;
    const { ref: dataRef, transformed } = await storePayloadBlob(
      item.runId,
      `the data of signal "${s.name}" (work item)`,
      s.data,
      codec,
      offload.blobs,
      payloadCodec,
    );
    return { ...item, ...mark(transformed), signal: { name: s.name, id: s.id, dataRef } };
  }
  if (item.kind === "child-done" && item.child) {
    const c = item.child;
    if (c.ok && c.value !== undefined && !c.valueRef) {
      const { ref: valueRef, transformed } = await storePayloadBlob(
        item.runId,
        `the value of child ${c.id}'s outcome (work item)`,
        c.value,
        codec,
        offload.blobs,
        payloadCodec,
      );
      return { ...item, ...mark(transformed), child: { id: c.id, ok: true, valueRef } };
    }
    if (!c.ok && c.error !== undefined && !c.errorRef) {
      const { ref: errorRef, transformed } = await storePayloadBlob(
        item.runId,
        `the error of child ${c.id}'s outcome (work item)`,
        c.error,
        codec,
        offload.blobs,
        payloadCodec,
      );
      return { ...item, ...mark(transformed), child: { id: c.id, ok: false, errorRef } };
    }
  }
  return item;
}
