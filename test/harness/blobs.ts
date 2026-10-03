// Test-side blob inspection WITHOUT the object-store client's `list()` / `getBlob()`: both stream
// through an ordered push consumer they never delete (LEARNINGS #59), which the harness teardown
// now counts as a leak. Objects are listed from their meta records with a multi-subject last-message
// direct get, and read through the library's own consumer-free reader.

import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { type NatsConnection } from "@nats-io/nats-core";
import { NatsBlobStore } from "./shell/blob-store.js";

export interface ObjectMeta {
  name: string;
  size: number;
  deleted: boolean;
}

/** Every live object in the bucket (deleted ones excluded), from the meta subjects' last messages. */
export async function listObjects(nc: NatsConnection, bucket: string): Promise<ObjectMeta[]> {
  const jsm = await jetstreamManager(nc);
  const out: ObjectMeta[] = [];
  try {
    const it = await jsm.direct.getLastMessagesFor(`OBJ_${bucket}`, {
      multi_last: [`$O.${bucket}.M.>`],
    });
    for await (const m of it) {
      const info = JSON.parse(new TextDecoder().decode(m.data)) as {
        name: string;
        size?: number;
        deleted?: boolean;
      };
      if (info.deleted === true) continue;
      out.push({ name: info.name, size: info.size ?? 0, deleted: false });
    }
  } catch (e) {
    // no objects at all → the server answers 404 "no results"
    if (!(e instanceof Error && /not found|no results/i.test(e.message))) throw e;
  }
  return out;
}

/** The object's bytes through the library's reader (one batched direct get; null if absent). */
export function readBlob(
  nc: NatsConnection,
  bucket: string,
  name: string,
): Promise<Uint8Array | null> {
  // prettier-ignore
  return new NatsBlobStore(jetstream(nc), bucket).get(name);
}
