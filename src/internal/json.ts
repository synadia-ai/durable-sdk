// @synadia-ai/durable — small JSON byte helpers for control payloads (work items, run records).

const enc = new TextEncoder();
const dec = new TextDecoder();

export const encodeJson = (value: unknown): Uint8Array => enc.encode(JSON.stringify(value));
export const decodeJson = <T>(data: Uint8Array): T => JSON.parse(dec.decode(data)) as T;
