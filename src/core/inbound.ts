// @synadia-ai/durable — recording an INBOUND EVENT (a timer wake, a signal, a child's outcome, a
// cancel request) into a run's journal, before replay, so the matching ctx.* continues. Pure:
// depends only on the Journal seam, which is what lets the deterministic simulation
// (test/harness/sim) drive THIS code — not a model of it — under fences it chooses. PROTOCOL §6.4.

import { FencedError } from "../errors.js";
import { withBlobRef } from "../journal/blobs.js";
import { type LoadedEntry, msgId, NON_COMMAND_SEQ, stamp } from "../journal/entry.js";
import { type Journal } from "../journal/store.js";
import { type WorkItem } from "./work-item.js";

/** How many times an inbound-event append is retried past a fence before the item is redelivered. */
export const EVENT_APPEND_ATTEMPTS = 5;

export function hasResolve(loaded: LoadedEntry[], seq: number): boolean {
  return loaded.some((l) => l.entry.kind === "resolve" && l.entry.resolvesSeq === seq);
}

/**
 * Journal the event a work item carries — idempotently, by CONTENT (a `resolve` only for a seq that
 * has none, a `signal` only for an unseen signalId, one `cancel-requested` per run) — and return
 * the journal to replay from: `loaded` itself when there was nothing to record (a `start`/`resume`
 * item, or an event already present), a fresh load otherwise.
 *
 * Throws when the event could not be journaled; the caller NAKs and the item comes back. It never
 * reports an event as recorded that is not in the journal.
 */
export async function recordInbound(
  journal: Journal,
  work: WorkItem,
  loaded: LoadedEntry[],
  clock: () => number = Date.now,
): Promise<LoadedEntry[]> {
  if (work.kind === "timer" && work.resolvesSeq !== undefined) {
    const wake = work.resolvesSeq;
    if (hasResolve(loaded, wake)) return loaded;
    return appendEvent(
      journal,
      work.runId,
      loaded,
      msgId.resolve(work.runId, wake),
      stamp({ kind: "resolve", seq: NON_COMMAND_SEQ, resolvesSeq: wake }, clock()),
      (fresh) => hasResolve(fresh, wake),
    );
  }
  if (work.kind === "signal" && work.signal) {
    const sig = work.signal;
    const seen = (ls: LoadedEntry[]): boolean =>
      ls.some((l) => l.entry.kind === "signal" && l.entry.signalId === sig.id);
    if (seen(loaded)) return loaded;
    // A shrunk work item carries a blob ref instead of inline data: journal the ref VERBATIM —
    // the value is already durably in the blob store, and load() resolves it at replay. Never
    // inflate here just to re-offload. `payloadEnc` says whether the blob holds payloadCodec
    // output, which is what picks v:3 over v:2; it comes from the item rather than from THIS
    // worker's config, because the writer is who knows.
    const base = stamp(
      {
        kind: "signal" as const,
        seq: NON_COMMAND_SEQ,
        name: sig.name,
        data: sig.data,
        signalId: sig.id,
      },
      clock(),
    );
    const entry = sig.dataRef
      ? withBlobRef(base, "data", sig.dataRef, work.payloadEnc === true)
      : base;
    return appendEvent(journal, work.runId, loaded, msgId.signal(work.runId, sig.id), entry, seen);
  }
  if (work.kind === "child-done" && work.resolvesSeq !== undefined && work.child) {
    const seq = work.resolvesSeq;
    const child = work.child;
    if (hasResolve(loaded, seq)) return loaded;
    let entry;
    if (child.ok) {
      const base = stamp(
        { kind: "resolve" as const, seq: NON_COMMAND_SEQ, resolvesSeq: seq, value: child.value },
        clock(),
      );
      entry = child.valueRef
        ? withBlobRef(base, "value", child.valueRef, work.payloadEnc === true)
        : base;
    } else {
      const base = stamp(
        {
          kind: "resolve" as const,
          seq: NON_COMMAND_SEQ,
          resolvesSeq: seq,
          ...(child.error !== undefined ? { error: child.error } : {}),
        },
        clock(),
      );
      entry = child.errorRef
        ? withBlobRef(base, "error", child.errorRef, work.payloadEnc === true)
        : base;
    }
    return appendEvent(
      journal,
      work.runId,
      loaded,
      msgId.resolve(work.runId, seq),
      entry,
      (fresh) => hasResolve(fresh, seq),
    );
  }
  if (work.kind === "cancel") {
    // Cancellation enters the journal like a signal: record it (idempotently) before replay so the
    // engine hard-stops at the next command boundary.
    const requested = (ls: LoadedEntry[]): boolean =>
      ls.some((l) => l.entry.kind === "cancel-requested");
    if (requested(loaded)) return loaded;
    return appendEvent(
      journal,
      work.runId,
      loaded,
      msgId.cancel(work.runId),
      stamp(
        work.reason !== undefined
          ? { kind: "cancel-requested", seq: NON_COMMAND_SEQ, reason: work.reason }
          : { kind: "cancel-requested", seq: NON_COMMAND_SEQ },
        clock(),
      ),
      requested,
    );
  }
  return loaded;
}

/**
 * Journal an inbound event, RETRYING past a fence, and return the fresh journal.
 *
 * A fence here means the append did NOT land — another writer advanced the log between this
 * worker's load and its CAS. The msgID makes the event at-most-once; nothing but a retry makes it
 * at-least-once. Treating a fence as "already handled" LOSES the event, and the work item carrying
 * it is acked moments later: for a `child-done`, that is a parent parked forever on a child that
 * finished. The race is ordinary — two different children of one parent completing at once, on two
 * workers — and event recording is deliberately ungated by the lease (§6.6) so a cancel can always
 * land, which is exactly why these appends collide.
 *
 * `recorded` is the caller's own "is it already there?" predicate: a fence caused by another worker
 * recording THIS event is success, not a retry. If the event is still absent after the attempts,
 * this throws — the worker's catch NAKs, and the item comes back instead of being lost.
 *
 * Found by the Python SDK's multi-worker chaos test; the bug was identical in both.
 */
export async function appendEvent(
  journal: Journal,
  runId: string,
  loaded: LoadedEntry[],
  mid: string,
  entry: LoadedEntry["entry"],
  recorded: (fresh: LoadedEntry[]) => boolean,
): Promise<LoadedEntry[]> {
  let current = loaded;
  for (let attempt = 0; attempt < EVENT_APPEND_ATTEMPTS; attempt++) {
    try {
      await journal.append(runId, {
        entry,
        msgID: mid,
        expectLastSeq: current.at(-1)?.streamSeq ?? 0,
      });
      return await journal.load(runId);
    } catch (e) {
      if (!(e instanceof FencedError)) throw e;
      current = await journal.load(runId);
      if (recorded(current)) return current; // another worker recorded this very event
    }
  }
  throw new Error(
    `run ${runId}: could not journal an inbound event after ${EVENT_APPEND_ATTEMPTS} attempts (contended); redelivering`,
  );
}
