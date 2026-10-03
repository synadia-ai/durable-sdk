// @synadia-ai/durable — batched DIRECT GET: read every message on one subject (or subject
// filter — wildcards are accepted), in stream order, in as few requests as the server allows, and
// create NO consumer. Used for journal loads, blob chunk reads and the parked-index sweep
// (LEARNINGS #58, #59). Each message the server returns carries the number still
// pending on the subject after it, so a subject longer than one batch is read in consecutive
// requests that resume from the last sequence seen; an empty subject is a 404 ("message not
// found"), which is simply "nothing there". Requires `allow_direct` on the stream (provisioned
// and reconciled by resources.ts) and nats-server >= 2.11.

import { type JetStreamManager, JetStreamError } from "@nats-io/jetstream";

export interface DirectRead {
  data: Uint8Array;
  /** The message's stream sequence. */
  seq: number;
  /** The message's subject (a KV key's subject is `$KV.<bucket>.<key>`). */
  subject: string;
  /** For a KV bucket's stream: the `KV-Operation` header — "" for a value, DEL / PURGE for a marker. */
  kvOperation: string;
}

/** The direct-get status for a subject with no messages at all (an empty read, not an error). */
export function isMessageNotFound(e: unknown): boolean {
  // The status error class is not on the package's public surface; it is a JetStreamError whose
  // `code` is the inline status (404 = "message not found").
  return e instanceof JetStreamError && (e as { code?: unknown }).code === 404;
}

/**
 * Read a whole subject. `batch` bounds one request; the loop continues while the server reports
 * messages pending. The result is a consistent prefix of the subject as the answering server saw it
 * — a follower may answer on a replicated stream — so callers that must not act on a prefix verify
 * completeness against a leader-served count (journal-store.ts) or a known size (blob-store.ts).
 */
export async function readSubject(
  jsm: JetStreamManager,
  stream: string,
  subject: string,
  batch = 1024,
): Promise<{ out: DirectRead[]; bytes: number }> {
  const out: DirectRead[] = [];
  let bytes = 0;
  let seq = 1;
  for (;;) {
    let delivered = 0;
    let pending = 0;
    let last = 0;
    try {
      const it = await jsm.direct.getBatch(stream, { seq, batch, next_by_subj: subject });
      for await (const m of it) {
        out.push({
          data: m.data,
          seq: m.seq,
          subject: m.subject,
          kvOperation: m.header.get("KV-Operation"),
        });
        bytes += m.data.byteLength;
        delivered++;
        pending = m.pending;
        last = m.seq;
      }
    } catch (e) {
      if (isMessageNotFound(e) && out.length === 0) return { out, bytes };
      throw e;
    }
    if (delivered === 0 || pending <= 0) return { out, bytes };
    seq = last + 1;
  }
}
