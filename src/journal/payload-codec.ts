// @synadia-ai/durable — the payload-codec seam: a bytes→bytes transform applied to the USER
// PAYLOAD of a journal entry (the `input` / `value` / `data` / `error` slot), leaving the envelope
// — `v`, `seq`, `kind`, `ts`, step names, idempotency keys, blob refs — in the clear.
//
// WHY SLOT-LEVEL AND NOT WHOLE-ENTRY. The replay engine reads `kind`/`seq`/`name` to match commands
// and to run its determinism guards, the journal loader reads `v`, and the dashboard reads all of
// them. Encrypting the envelope would blind every one of those. Encrypting only the payload gives
// the property that matters — a principal with read rights on `de.journal.>` sees the SHAPE of a run
// and none of its data — while the run stays debuggable.
//
// WHY IT IS NOT THE `Codec`. `Codec` is the structural, JSON, byte-for-byte wire encoding, pinned
// across SDKs by `conformance/vectors/journal-entries.json`. This is a separate, deployment-local
// transform layered on top of it. Keeping them apart means enabling encryption cannot change how
// the wire format itself is defined — and the conformance corpus stays plaintext JSON.
//
// It is ASYNC by contract, because real key material lives behind WebCrypto or a KMS.

import { PayloadCodecError } from "../errors.js";

/**
 * A reversible transform over encoded payload bytes. `decode(await encode(x))` MUST equal `x`.
 *
 * It need NOT be deterministic — a fresh IV per call is expected and preferred. A payload slot is
 * encoded once at append time and only ever decoded afterwards, so replay never re-encrypts and
 * never compares ciphertexts.
 *
 * Both directions may be async. Throwing from `decode` parks the run (the operator can fix the key
 * and the run resumes) rather than failing it — see {@link PayloadCodecError}.
 */
export interface PayloadCodec {
  encode(plain: Uint8Array): Uint8Array | Promise<Uint8Array>;
  decode(encoded: Uint8Array): Uint8Array | Promise<Uint8Array>;
}

const B64_CHUNK = 0x8000; // stay well inside the argument-count limit of String.fromCharCode

/** Bytes → base64 (payload slots ride the entry's JSON, which cannot hold raw bytes). */
export function toBase64(bytes: Uint8Array): string {
  let s = "";
  for (let i = 0; i < bytes.length; i += B64_CHUNK) {
    s += String.fromCharCode(...bytes.subarray(i, i + B64_CHUNK));
  }
  return btoa(s);
}

/** base64 → bytes. Throws on malformed input (surfaced as a park, not a silent empty payload). */
export function fromBase64(s: string): Uint8Array {
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

const IV_BYTES = 12; // AES-GCM standard nonce length

/**
 * `CryptoKey` without naming it: the project's `lib` is ES2022 (no DOM), and the global differs
 * between Node, Bun and Deno. Deriving it from the API we already call keeps this file runtime-
 * neutral and adds no dependency.
 */
type SubtleKey = Awaited<ReturnType<typeof globalThis.crypto.subtle.importKey>>;

/**
 * A ready-made AES-256-GCM {@link PayloadCodec} over WebCrypto (`globalThis.crypto.subtle`, the same
 * API the blob digest already uses — no dependency, works on Node ≥ 18, Bun and Deno).
 *
 * Format: `iv(12) || ciphertext||tag`. A fresh random IV per call, so identical payloads do not
 * produce identical ciphertext — which also means offloaded blobs stop deduplicating by content
 * (correct: content-addressing a secret by its plaintext digest would leak equality).
 *
 * Key management is deliberately yours: pass a 32-byte key or an already-imported WebCrypto key. Rotation is
 * out of scope for this helper — a run's journal must stay readable for its whole life, so a
 * rotating deployment needs a codec that selects the key by a hint it writes into its own output.
 */
export function aesGcmPayloadCodec(key: Uint8Array | SubtleKey): PayloadCodec {
  let imported: Promise<SubtleKey> | undefined;
  const getKey = (): Promise<SubtleKey> => {
    if (key instanceof Uint8Array) {
      if (key.byteLength !== 32) {
        throw new PayloadCodecError(
          `aesGcmPayloadCodec: key must be 32 bytes (AES-256), got ${key.byteLength}`,
        );
      }
      imported ??= globalThis.crypto.subtle.importKey("raw", key, "AES-GCM", false, [
        "encrypt",
        "decrypt",
      ]);
      return imported;
    }
    return Promise.resolve(key);
  };

  return {
    async encode(plain: Uint8Array): Promise<Uint8Array> {
      const iv = globalThis.crypto.getRandomValues(new Uint8Array(IV_BYTES));
      const ct = new Uint8Array(
        await globalThis.crypto.subtle.encrypt({ name: "AES-GCM", iv }, await getKey(), plain),
      );
      const out = new Uint8Array(iv.byteLength + ct.byteLength);
      out.set(iv, 0);
      out.set(ct, iv.byteLength);
      return out;
    },
    async decode(encoded: Uint8Array): Promise<Uint8Array> {
      if (encoded.byteLength <= IV_BYTES) {
        throw new PayloadCodecError("aesGcmPayloadCodec: ciphertext is too short to hold an IV");
      }
      const iv = encoded.subarray(0, IV_BYTES);
      const body = encoded.subarray(IV_BYTES);
      return new Uint8Array(
        await globalThis.crypto.subtle.decrypt({ name: "AES-GCM", iv }, await getKey(), body),
      );
    },
  };
}
