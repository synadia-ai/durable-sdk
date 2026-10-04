// durable-sdk: the transport's reading of a reply and the blob store's `put` wait, against a fake
// connection (no server). What a refusal is (the engine's F31: an `error` member with a string
// `code`, never a failed run's error value), what `blob_gone` becomes (the engine's F30: the
// library's BlobMissingError or BlobIntegrityError, this repo's F31), and how long a `put` part is
// waited for and what its failure becomes (this repo's F33).

import { type NatsConnection, TimeoutError } from "@nats-io/nats-core";
import { describe, expect, it } from "vitest";
import {
  EngineBlobs,
  PUT_MS_PER_MIB,
  PUT_RTTS_PER_CHUNK,
  STORE_CHUNK_BYTES,
  putTimeoutMs,
} from "../../src/engine/blobs.js";
import { EngineUnavailableError, LeaseExpiredError } from "../../src/engine/errors.js";
import { createTransport, REQUEST_MARGIN_MS, type Transport } from "../../src/engine/transport.js";
import {
  BlobIntegrityError,
  BlobMissingError,
  PayloadTooLargeError,
  SerializationError,
} from "../../src/errors.js";

/** A connection whose every request answers `reply` (or fails with it), recording the timeouts. */
function fakeNc(reply: object | Error): { nc: NatsConnection; timeouts: number[] } {
  const timeouts: number[] = [];
  const nc = {
    info: { max_payload: 1024 * 1024 },
    request(_subject: string, _payload: Uint8Array, opts: { timeout: number }) {
      timeouts.push(opts.timeout);
      if (reply instanceof Error) return Promise.reject(reply);
      return Promise.resolve({ data: new TextEncoder().encode(JSON.stringify(reply)) });
    },
  };
  return { nc: nc as unknown as NatsConnection, timeouts };
}

const NAME = `r1/sha256:${"a".repeat(64)}`;

describe("a refusal is an error member with a string code (the engine's F31)", () => {
  it("a failed run's result and status carry its error value, and are answered, not thrown", async () => {
    const failed = { kind: "fail", error: { name: "Error", message: "boom" } };
    expect(await createTransport(fakeNc(failed).nc, "de")("result", { runId: "r1" })).toEqual(
      failed,
    );
    const status = { state: "failed", error: { name: "Error", message: "boom" } };
    expect(await createTransport(fakeNc(status).nc, "de")("status", { runId: "r1" })).toEqual(
      status,
    );
  });

  it("an error member with a code is the refusal", async () => {
    const t = createTransport(fakeNc({ error: { code: "expired", message: "gone" } }).nc, "de");
    await expect(t("load", { runId: "r1" })).rejects.toBeInstanceOf(LeaseExpiredError);
  });
});

describe("blob_gone (the engine's F30) is the library's missing-blob error", () => {
  it("a blob that is gone is BlobMissingError, naming the blob and its run", async () => {
    const message = `run r1 seq -1: blob ${NAME} is gone from the blob store; restore it or cancel the run`;
    const t = createTransport(
      fakeNc({ error: { code: "blob_gone", message, blob: NAME } }).nc,
      "de",
    );
    const e = await t("get", { runId: "r1", name: NAME, part: 0 }).catch((x: unknown) => x);
    expect(e).toBeInstanceOf(BlobMissingError);
    expect(e).toMatchObject({ runId: "r1", blobName: NAME, seq: -1, reason: "absent" });
  });

  it("bytes that are not the digest's are BlobIntegrityError", async () => {
    for (const detail of [
      `content is sha256:${"b".repeat(64)}, not the digest its name carries`,
      `digest mismatch: ref has sha256:${"a".repeat(64)}, content is sha256:${"b".repeat(64)}`,
    ]) {
      const message = `run r1 seq 4: blob ${NAME}: ${detail}`;
      const t = createTransport(
        fakeNc({ error: { code: "blob_gone", message, blob: NAME } }).nc,
        "de",
      );
      const e = await t("result", { runId: "r1" }).catch((x: unknown) => x);
      expect(e).toBeInstanceOf(BlobIntegrityError);
      expect(e).toMatchObject({ runId: "r1", blobName: NAME });
    }
  });

  it("the blob store answers null for a gone blob, so the library names the entry", async () => {
    const message = `run r1 seq -1: blob ${NAME} is gone from the blob store; restore it or cancel the run`;
    const request = createTransport(
      fakeNc({ error: { code: "blob_gone", message, blob: NAME } }).nc,
      "de",
    );
    const blobs = new EngineBlobs({ request, sizes: () => Promise.resolve({ requestMax: 1 << 20, partBytes: 1 << 19 }) }); // prettier-ignore
    expect(await blobs.get(NAME)).toBeNull();
  });

  it("internal is no longer read as gone: it stays the operator's, transient", async () => {
    const request = createTransport(
      fakeNc({ error: { code: "internal", message: "codec" } }).nc,
      "de",
    );
    const blobs = new EngineBlobs({ request, sizes: () => Promise.resolve({ requestMax: 1 << 20, partBytes: 1 << 19 }) }); // prettier-ignore
    await expect(blobs.get(NAME)).rejects.toBeInstanceOf(EngineUnavailableError);
  });
});

describe("a put part's wait scales with what the engine stores for it (F33)", () => {
  const MiB = 1024 * 1024;

  it("the margin, a round trip per store chunk times the allowance, and the throughput floor", () => {
    expect(putTimeoutMs(0, 50)).toBe(REQUEST_MARGIN_MS);
    const chunks = Math.ceil(MiB / STORE_CHUNK_BYTES);
    expect(putTimeoutMs(MiB, 50)).toBe(REQUEST_MARGIN_MS + chunks * PUT_RTTS_PER_CHUNK * 50 + PUT_MS_PER_MIB); // prettier-ignore
    // The NGS run's case: a 12 MiB value's last part (30 KiB of its own, the value read back and
    // written) at 51 ms, where the flat 10 s gave up.
    expect(putTimeoutMs(30_720 + 2 * 12 * MiB, 51)).toBeGreaterThan(70_000);
  });

  it("each part asks for its own timeout; the last also for the whole value, twice", async () => {
    const asked: Array<number | undefined> = [];
    const request: Transport = <R>(_op: string, body: object, o?: { timeoutMs?: number }) => {
      asked.push(o?.timeoutMs);
      const last = (body as { last?: boolean }).last === true;
      return Promise.resolve((last ? { ok: true, ref: { name: NAME } } : { ok: true }) as R);
    };
    const partBytes = 3 * 1024;
    const blobs = new EngineBlobs({
      request,
      sizes: () => Promise.resolve({ requestMax: 8 * 1024, partBytes }),
      rtt: () => Promise.resolve(20),
    });
    const data = new Uint8Array(7 * 1024); // parts of 3, 3 and 1 KiB
    await blobs.upload("r1", data);
    expect(asked).toEqual([
      putTimeoutMs(partBytes, 20),
      putTimeoutMs(partBytes, 20),
      putTimeoutMs(1024 + 2 * data.byteLength, 20),
    ]);
  });

  it("the transport sends the timeout it is given", async () => {
    const { nc, timeouts } = fakeNc({ ok: true });
    await createTransport(nc, "de")("put", { runId: "r1" }, { timeoutMs: 123_456 });
    await createTransport(nc, "de")("info", {});
    expect(timeouts).toEqual([123_456, REQUEST_MARGIN_MS]);
  });
});

describe("a put that fails is deterministic for its value, never a retry loop (F33)", () => {
  const sizes = (): Promise<{ requestMax: number; partBytes: number }> =>
    Promise.resolve({ requestMax: 1 << 20, partBytes: 1 << 19 });

  it("no answer within the scaled wait is PayloadTooLargeError, which the pure core fails the run on", async () => {
    const request = createTransport(fakeNc(new TimeoutError()).nc, "de"); // prettier-ignore
    const e = await new EngineBlobs({ request, sizes }).upload("r1", new Uint8Array(10)).catch((x: unknown) => x); // prettier-ignore
    expect(e).toBeInstanceOf(PayloadTooLargeError);
    expect(e).toBeInstanceOf(SerializationError);
    expect((e as Error).message).toContain("did not store a 10-byte value in time");
  });

  it("an invalid put is a SerializationError", async () => {
    const request = createTransport(fakeNc({ error: { code: "invalid", message: "digest" } }).nc, "de"); // prettier-ignore
    await expect(new EngineBlobs({ request, sizes }).upload("r1", new Uint8Array(10))).rejects.toBeInstanceOf(SerializationError); // prettier-ignore
  });

  it("an expired lease and the engine's unavailable keep their meaning", async () => {
    for (const [code, type] of [
      ["expired", LeaseExpiredError],
      ["unavailable", EngineUnavailableError],
    ] as const) {
      // prettier-ignore
      const request = createTransport(fakeNc({ error: { code, message: code } }).nc, "de");
      const e = await new EngineBlobs({ request, sizes }).upload("r1", new Uint8Array(10)).catch((x: unknown) => x); // prettier-ignore
      expect(e).toBeInstanceOf(type);
      expect(e).not.toBeInstanceOf(SerializationError);
    }
  });
});
