// @synadia-ai/durable — package version metadata.

/** Must match `package.json`'s version — pinned by a unit test, because it silently drifted twice. */
export const VERSION = "0.1.1";

/** Wire-format version of the on-stream journal layout (see PROTOCOL §1.2). */
export const WIRE_VERSION = 1;

/**
 * Wire version stamped ONLY on entries whose payload slot is offloaded to the blob store (a
 * `*Ref` field replaces the inline value; see PROTOCOL "large-payload offload"). Deliberately a
 * DIFFERENT version: a pre-offload reader decodes a ref-stripped entry "successfully" and would
 * replay the missing slot as `undefined` — silent corruption. Bumping v makes old readers fail
 * loud ("unsupported wire version") and park the run until they are upgraded. Entries without
 * refs keep v:1, so runs that never offload are byte-identical to pre-offload versions.
 */
export const WIRE_VERSION_BLOBS = 2;

/**
 * Wire version stamped ONLY on entries whose payload slot passed through a
 * {@link import("./journal/payload-codec.js").PayloadCodec} — the slot is absent and a `*Enc`
 * (inline, base64) or `*Ref` (offloaded; the BLOB holds transformed bytes) field stands in for it.
 *
 * Same reasoning as {@link WIRE_VERSION_BLOBS}, one step further: a reader without the codec must
 * not decode a transformed payload as though it were plaintext. Bumping `v` makes it fail loud and
 * PARK — the run resumes once the codec is configured — instead of handing a workflow a base64
 * string where its input should be. v:3 subsumes v:2: an encrypted entry may also carry a ref.
 */
export const WIRE_VERSION_ENCODED = 3;
