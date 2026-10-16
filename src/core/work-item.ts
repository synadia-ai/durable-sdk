// @synadia-ai/durable — the on-queue work-item shape (DE_WORK). One durable queue drives dispatch,
// resumes, timer/signal wakes, and child-done deliveries. See PROTOCOL §6.1.
//
// The SHAPE lives in the pure core because the core is what turns an item into journal entries
// (`inbound.ts`); what puts one on a queue — and shrinks its payload to a blob ref on the way — is
// the shell's business (`nats/protocol.ts`).

import { type BlobRef } from "../journal/entry.js";

export interface WorkItem {
  runId: string;
  kind: "start" | "resume" | "timer" | "signal" | "child-done" | "cancel";
  /** For kind "timer"/"child-done": the command seq this resolves. */
  resolvesSeq?: number;
  /** For kind "cancel": an optional human-readable reason (carried onto the cancel-requested entry). */
  reason?: string;
  /** For kind "signal": the inbound signal (id gives idempotency). `dataRef` replaces `data` when
   *  the payload is offloaded — the worker journals the ref verbatim; replay resolves it. */
  signal?: { name: string; data?: unknown; dataRef?: BlobRef; id: string };
  /** For kind "child-done": the finished sub-workflow's outcome (id = childRunId, for dedup).
   *  `valueRef`/`errorRef` replace the inline slot when offloaded. */
  child?:
    | { id: string; ok: true; value?: unknown; valueRef?: BlobRef }
    | { id: string; ok: false; error?: { name: string; message: string }; errorRef?: BlobRef };
  /** True when the blob this item's `*Ref` points at holds payloadCodec output rather than the
   *  plain encoded value — so the worker stamps the journal entry it builds v:3, not v:2. Absent
   *  on every item written without a codec, which is why enabling one changes no existing bytes. */
  payloadEnc?: true;
}
