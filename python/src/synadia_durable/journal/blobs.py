"""synadia-durable — large-payload offload: blob refs, their resolution, and writing them.

PROTOCOL §5.6. When an encoded entry exceeds the offload threshold, its single payload slot
(``input`` / ``value`` / ``data`` / ``error``) is written to the blob store FIRST and the entry
carries a content-addressed ref instead, stamped ``v:2``. Loading resolves refs back to inline values
before entries reach the core, so the replay engine never sees a ref.

**Why content-addressed names** (``{runId}/{digest}``) rather than msgID-derived ones: a step
function may legitimately be nondeterministic, so two CAS racers at the same seq can hold DIFFERENT
bytes. With a per-seq name the loser's put could overwrite the winner's blob AFTER the winner's ref
landed — silent corruption. Content addressing makes that structurally impossible: divergent racers
write different objects and the journal CAS picks the surviving ref, while identical racers share
one object. The corollary is a rule: **a fenced writer MUST NOT delete its blob** (the winner may
reference the very same object). Orphans are inert and are GC'd with the run.

The read half is what keeps a mixed fleet honest: a reader that meets a ``v:2`` entry it cannot
resolve MUST fail loud and park, never treat the stripped slot as an absent value.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, cast, runtime_checkable

from synadia_durable.errors import (
    BlobIntegrityError,
    BlobMissingError,
    PayloadCodecError,
    SerializationError,
)
from synadia_durable.version import WIRE_VERSION_BLOBS, WIRE_VERSION_ENCODED

if TYPE_CHECKING:
    from synadia_durable.journal.codec import Codec
    from synadia_durable.journal.entry import BlobRef, JournalEntry
    from synadia_durable.journal.payload_codec import PayloadCodec

__all__ = [
    "DEFAULT_BLOB_CACHE_BYTES",
    "MIN_OFFLOAD_THRESHOLD_BYTES",
    "OFFLOAD_MARGIN_BYTES",
    "BlobStore",
    "OffloadOptions",
    "ResolvedOffload",
    "blob_name",
    "digest_of",
    "effective_threshold",
    "encode_payload",
    "entry_ref",
    "offload_entry",
    "payload_slot",
    "resolve_entry",
    "resolve_offload_options",
    "store_payload_blob",
    "with_blob_ref",
]

#: Safety margin subtracted from the server's ``max_payload`` to form the default threshold: an
#: entry at exactly the threshold must still fit once publish headers (msgID, expected-last-sequence)
#: are added. Generous on purpose — offloading a borderline entry costs one object-store round trip,
#: while a too-tight margin costs a failed run.
OFFLOAD_MARGIN_BYTES = 16 * 1024

#: Floor for a configured threshold. Below it every entry would offload — metadata alone is bigger —
#: and the journal would degenerate into pure pointers.
MIN_OFFLOAD_THRESHOLD_BYTES = 1024

#: Default byte budget for the per-instance resolved-blob cache. Content-addressed names make a hit
#: impossible to stale.
DEFAULT_BLOB_CACHE_BYTES = 64 * 1024 * 1024

#: The one offloadable payload slot of each entry kind. A kind that is not here is pure metadata:
#: an oversized one cannot be helped by offload and must fail loud.
_SLOT_OF_KIND: dict[str, str] = {
    "start": "input",
    "call": "input",
    "continued": "input",
    "step-end": "value",
    "result": "value",
    "signal": "data",
    "fail": "error",
}

#: The ref field that stands in for each slot — the inverse of :data:`REF_SLOTS`.
_REF_FIELD_OF: dict[str, str] = {
    "input": "inputRef",
    "value": "valueRef",
    "data": "dataRef",
    "error": "errorRef",
}

#: Every offloadable payload slot, keyed by the ref field that stands in for it. Exactly one field
#: per kind is offloadable; everything else is metadata and must stay inline (PROTOCOL §5.6).
REF_SLOTS: dict[str, str] = {
    "inputRef": "input",
    "valueRef": "value",
    "dataRef": "data",
    "errorRef": "error",
}

#: The ``*Enc`` field that stands in for each slot when a PayloadCodec transformed it inline (v:3).
_ENC_FIELD_OF: dict[str, str] = {
    "input": "inputEnc",
    "value": "valueEnc",
    "data": "dataEnc",
    "error": "errorEnc",
}

#: Every transformable payload slot, keyed by the ``*Enc`` field that stands in for it.
_ENC_SLOTS: dict[str, str] = {
    "inputEnc": "input",
    "valueEnc": "value",
    "dataEnc": "data",
    "errorEnc": "error",
}


def entry_enc(entry: JournalEntry) -> tuple[str, object] | None:
    """The inline transformed slot an entry carries, if any (v:3 without a ref)."""
    fields = cast("dict[str, object]", entry)
    for field in _ENC_SLOTS:
        if field in fields:
            return field, fields[field]
    return None


async def _untransform(
    run_id: str, seq: int, data: bytes, payload_codec: PayloadCodec | None
) -> bytes:
    """Reverse the codec, mapping "no codec" and "codec raised" onto one parkable error."""
    if payload_codec is None:
        raise PayloadCodecError(
            "the journal carries payload-codec output (v:3) but this reader has no "
            "payload_codec configured",
            run_id,
            seq,
        )
    try:
        return await payload_codec.decode(data)
    except PayloadCodecError:
        raise
    except Exception as e:
        raise PayloadCodecError(
            "the configured payload_codec could not decode this payload "
            "(wrong key, or the bytes were altered)",
            run_id,
            seq,
        ) from e


def _b64decode(run_id: str, seq: int, s: str) -> bytes:
    try:
        return base64.b64decode(s, validate=True)
    except (binascii.Error, ValueError) as e:
        raise PayloadCodecError("payload slot is not valid base64", run_id, seq) from e


def _decode_value(run_id: str, seq: int, data: bytes, codec: Codec) -> object:
    try:
        return codec.decode_value(data)
    except (TypeError, ValueError) as e:
        raise PayloadCodecError(
            "payload decoded to bytes that are not a valid encoded value", run_id, seq
        ) from e


async def encode_payload(
    run_id: str,
    entry: JournalEntry,
    codec: Codec,
    payload_codec: PayloadCodec | None,
) -> JournalEntry:
    """Put an entry's payload slot through the codec, producing the stored (v:3) form.

    Returns the input untouched when there is no codec, no slot, an absent slot, or the entry
    already arrived transformed (a pass-through append — a work item's ref/enc is journaled verbatim
    and must not be transformed twice).
    """
    if payload_codec is None:
        return entry
    if entry_ref(entry) is not None or entry_enc(entry) is not None:
        return entry
    slot = payload_slot(entry)
    if slot is None:
        return entry
    fields = cast("dict[str, object]", entry)
    if slot not in fields:
        return entry

    seq = entry.get("seq", -1)
    try:
        plain = codec.encode_value(fields[slot])
    except (TypeError, ValueError) as e:
        # Same class as any other unencodable value: deterministic, fails the run.
        raise SerializationError(
            run_id,
            f"run {run_id}: cannot encode the {slot} of a {entry['kind']} entry (seq {seq})",
        ) from e
    try:
        out = await payload_codec.encode(plain)
    except PayloadCodecError:
        raise
    except Exception as e:
        raise PayloadCodecError(
            "the configured payload_codec failed to encode this payload", run_id, seq
        ) from e

    stored = dict(fields)
    del stored[slot]
    stored[_ENC_FIELD_OF[slot]] = base64.b64encode(out).decode("ascii")
    stored["v"] = WIRE_VERSION_ENCODED
    return cast("JournalEntry", stored)


@runtime_checkable
class BlobStore(Protocol):
    """The blob object store seam. Implemented over a JetStream object store by the NATS shell."""

    async def get(self, name: str) -> bytes:
        """Fetch a blob's bytes. Raises :class:`BlobMissingError` when definitively absent."""
        ...

    async def put(self, name: str, data: bytes) -> None:
        """Write a blob. Content-addressed, so writing an existing name is a no-op."""
        ...


def digest_of(data: bytes) -> str:
    """The content digest of a blob's bytes, in the wire's form: ``sha256:<64 lowercase hex>``."""
    return f"sha256:{hashlib.sha256(data).hexdigest()}"


def blob_name(run_id: str, digest: str) -> str:
    """The object name for a blob: ``{runId}/{digest}``.

    The runId prefix is the **GC unit** — deleting a run's journal plus its ``{runId}/`` objects is
    complete and cannot break another run, which is why identical content in two runs is stored twice
    on purpose.
    """
    return f"{run_id}/{digest}"


def entry_ref(entry: JournalEntry) -> tuple[str, BlobRef] | None:
    """The entry's ``(ref field, ref)`` if its payload slot is offloaded, else ``None``."""
    fields = cast("dict[str, object]", entry)
    for field in REF_SLOTS:
        ref = fields.get(field)
        if ref is not None:
            return field, cast("BlobRef", ref)
    return None


async def resolve_entry(
    run_id: str,
    entry: JournalEntry,
    codec: Codec,
    blobs: BlobStore | None = None,
    payload_codec: PayloadCodec | None = None,
) -> JournalEntry:
    """Inline an entry's offloaded payload slot. A no-op for an entry that carries no ref.

    The engine never sees a ref: resolution happens at journal load, so replay compares values, not
    pointers.
    """
    seq = entry.get("seq", -1)

    # An INLINE transformed slot (v:3 without a ref) never touches the blob store.
    enc = entry_enc(entry)
    if enc is not None:
        enc_field, encoded = enc
        if not isinstance(encoded, str):
            raise PayloadCodecError(
                f"malformed {enc_field} (expected a base64 string)", run_id, seq
            )
        plain = await _untransform(run_id, seq, _b64decode(run_id, seq, encoded), payload_codec)
        out = dict(cast("dict[str, object]", entry))
        del out[enc_field]
        out[_ENC_SLOTS[enc_field]] = _decode_value(run_id, seq, plain, codec)
        return cast("JournalEntry", out)

    found = entry_ref(entry)
    if found is None:
        # A v:3 entry with neither form is uninterpretable — only reachable via a newer writer or a
        # hand-edited stream. Reading it as plaintext is the corruption the version bump prevents.
        if entry.get("v", WIRE_VERSION_BLOBS) >= WIRE_VERSION_ENCODED:
            raise PayloadCodecError(
                "the journal carries payload-codec output (v:3) but this reader has no "
                "payload_codec configured"
                if payload_codec is None
                else "a v:3 entry carries neither an inline nor an offloaded payload slot",
                run_id,
                seq,
            )
        return entry
    field, ref = found

    name = ref.get("name") if isinstance(ref, dict) else None
    digest = ref.get("digest") if isinstance(ref, dict) else None
    if not isinstance(name, str) or not isinstance(digest, str):
        raise BlobIntegrityError(run_id, seq, str(name), f"malformed {field}: {ref!r}")

    if blobs is None:
        # A configuration error, not a transient one: waiting never heals it. Loud, and — crucially
        # — NOT a silent replay of the stripped slot as an absent value.
        raise BlobMissingError(
            run_id,
            seq,
            name,
            "this reader has no blob store, but the journal carries offloaded payloads "
            "(enable offload, or provision the blob bucket)",
            "no-store",
        )

    try:
        data = await blobs.get(name)
    except BlobMissingError as e:
        # A store knows the object name but not which run or entry wanted it; only here is that
        # context available, and it is the whole difference between an actionable runbook line and
        # "some blob is missing".
        raise BlobMissingError(run_id, seq, name, str(e), e.reason) from e
    actual = digest_of(data)
    if actual != digest:
        raise BlobIntegrityError(
            run_id, seq, name, f"digest mismatch — expected {digest}, got {actual}"
        )

    # An OFFLOADED transformed slot: the BLOB holds codec output, so reverse it before decoding.
    # The digest check above ran on the STORED bytes — i.e. the transformed form, which is exactly
    # what integrity should cover.
    if entry.get("v", WIRE_VERSION_BLOBS) >= WIRE_VERSION_ENCODED:
        data = await _untransform(run_id, seq, data, payload_codec)

    out = dict(cast("dict[str, object]", entry))
    del out[field]
    out[REF_SLOTS[field]] = _decode_value(run_id, seq, data, codec)
    return cast("JournalEntry", out)


@dataclass(frozen=True, slots=True)
class OffloadOptions:
    """Tuning for large-payload offload. Pass ``offload=False`` to disable it entirely.

    Disabling is a real choice with a real consequence: an oversized payload then fails the run
    deterministically (``PayloadTooLargeError``) instead of moving to the blob store.
    """

    #: Encoded-entry size above which the payload slot is offloaded. Default: the connection's
    #: ``max_payload`` − 16 KiB, i.e. offload ONLY what could not have been published inline. Lower
    #: it (say 256 KiB) to keep journal entries lean at the cost of a round trip per large value.
    threshold_bytes: int | None = None
    #: Byte budget for the per-instance resolved-blob cache; 0 disables caching.
    cache_bytes: int = DEFAULT_BLOB_CACHE_BYTES
    #: ``DE_BLOBS`` bucket ``max_bytes`` at provision time.
    max_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class ResolvedOffload:
    """The settled offload configuration a store actually uses."""

    enabled: bool
    threshold_bytes: int | None = None
    cache_bytes: int = 0
    max_bytes: int | None = None


def resolve_offload_options(offload: bool | OffloadOptions) -> ResolvedOffload:
    """Normalize the public ``offload`` option (``True`` / ``False`` / tuning) into one shape."""
    if offload is False:
        return ResolvedOffload(enabled=False)
    options = offload if isinstance(offload, OffloadOptions) else OffloadOptions()
    return ResolvedOffload(
        enabled=True,
        threshold_bytes=options.threshold_bytes,
        cache_bytes=max(0, options.cache_bytes),
        max_bytes=options.max_bytes,
    )


def effective_threshold(threshold_bytes: int | None, max_payload: int | None) -> int | None:
    """Clamp a configured or derived threshold into its sane range.

    ``None`` means "we do not know how big is too big" — the connection never reported a
    ``max_payload`` and the caller configured nothing — and the caller then offloads nothing rather
    than guessing a limit.
    """
    auto = max_payload - OFFLOAD_MARGIN_BYTES if max_payload is not None else None
    chosen: int | None
    if threshold_bytes is not None:
        chosen = min(threshold_bytes, auto) if auto is not None else threshold_bytes
    else:
        chosen = auto
    if chosen is None:
        return None
    return max(MIN_OFFLOAD_THRESHOLD_BYTES, chosen)


def payload_slot(entry: JournalEntry) -> str | None:
    """The single offloadable slot of this entry, or ``None`` when the kind carries no payload.

    ``resolve`` holds a value XOR an error, so its slot is whichever is populated.
    """
    kind = entry["kind"]
    if kind == "resolve":
        fields = cast("dict[str, object]", entry)
        return "error" if fields.get("error") is not None else "value"
    return _SLOT_OF_KIND.get(kind)


def with_blob_ref(
    entry: JournalEntry, slot: str, ref: BlobRef, *, transformed: bool = False
) -> JournalEntry:
    """The stored form of an entry: slot removed, ``*Ref`` added, stamped ``v:2``.

    ``transformed`` stamps ``v:3`` instead, for a blob holding payload-codec output — that flag is
    what tells a reader to untransform the bytes after fetching them. Getting it wrong is never a
    corruption: a ``v:2`` ref to ciphertext decodes to garbage and PARKS the run, and a ``v:3`` ref
    to plaintext fails to decrypt and parks likewise.
    """
    stored = dict(cast("dict[str, object]", entry))
    stored.pop(slot, None)
    stored[_REF_FIELD_OF[slot]] = ref
    stored["v"] = WIRE_VERSION_ENCODED if transformed else WIRE_VERSION_BLOBS
    return cast("JournalEntry", stored)


async def store_payload_blob(
    run_id: str,
    what: str,
    value: object,
    codec: Codec,
    blobs: BlobStore,
    payload_codec: PayloadCodec | None,
) -> tuple[BlobRef, bool]:
    """Encode one value, write it under its content-addressed name, and return ``(ref, transformed)``.

    **This is the ONE function that writes a user payload to the blob store**, and that is worth
    stating. A payload reaches durable storage by three routes — a journal append, a work item's
    shrink, the status hint's shrink — and each used to decide for itself whether the codec applied.
    One of them silently did not, so the same value was encrypted when it fit inline and stored in
    the clear when it did not. Two things here make that shape unavailable:

    1. ``payload_codec`` is a REQUIRED parameter. ``None`` is a decision a caller writes down, not
       one it reaches by omission. (``_put_blob`` is private, so there is no way around it.)
    2. It returns ``transformed`` with the ref. The v:2-vs-v:3 marker on an entry and the
       ``payloadEnc`` flag on a work item are derived from what this function ACTUALLY did, rather
       than from a boolean the caller tracks in parallel and can get out of step — which is exactly
       how the second half of that bug looked.

    **Blob-before-pointer**: the object is fully written before the ref exists to be published
    anywhere, so a ref on the stream always resolves. Shared by journal appends, work-item shrinking
    and the status finalize — identical value + codec ⇒ identical bytes ⇒ identical name, so the
    same value embedded in several places under one run converges on ONE object.

    With a ``payload_codec`` the BLOB holds the codec's output and the digest covers it — the same
    posture :func:`offload_entry` takes for a ``v:3`` entry, and the reason the content address is
    over ciphertext rather than over a secret's cleartext hash. Callers storing transformed bytes
    must mark the ref (``with_blob_ref(..., transformed=True)`` / ``WorkItem["payloadEnc"]``) so the
    reader untransforms after fetching.

    The failure classes stay apart: an encode failure is deterministic
    (:class:`SerializationError`, which fails the run), a codec failure is a
    :class:`PayloadCodecError` (which parks), and a put failure propagates raw as transient
    infrastructure for the caller to retry.
    """
    try:
        blob = codec.encode_value(value)
    except (TypeError, ValueError) as e:
        raise SerializationError(run_id, f"run {run_id}: cannot encode {what} as a blob") from e
    if payload_codec is not None:
        try:
            blob = await payload_codec.encode(blob)
        except PayloadCodecError:
            raise
        except Exception as e:
            raise PayloadCodecError(
                f"the configured payload_codec failed to encode {what}", run_id
            ) from e
    digest = digest_of(blob)
    ref: BlobRef = {"name": blob_name(run_id, digest), "digest": digest, "size": len(blob)}
    await blobs.put(ref["name"], blob)
    return ref, payload_codec is not None


async def offload_entry(
    run_id: str,
    entry: JournalEntry,
    encoded: bytes,
    codec: Codec,
    blobs: BlobStore,
    threshold_bytes: int,
) -> tuple[JournalEntry, bytes]:
    """Offload an entry's payload slot if its encoded form exceeds ``threshold_bytes``.

    Returns the entry to publish and its bytes — the input unchanged when nothing was offloaded:
    under threshold, a kind with no payload slot, an absent slot, or an entry that already carries a
    ref (a pass-through append of somebody else's offloaded entry). The caller re-checks the
    returned bytes against ``max_payload``, because an entry whose METADATA alone is oversized
    cannot be saved by offload and must fail loud.
    """
    if len(encoded) <= threshold_bytes:
        return entry, encoded
    if entry_ref(entry) is not None:
        return entry, encoded

    # Already through the payload codec (v:3 inline): the blob must hold the codec's output, not the
    # plaintext — so offload the ciphertext we already have rather than re-deriving it from a slot
    # that is no longer on the entry. Stays v:3, so the reader knows to reverse it after fetching.
    enc = entry_enc(entry)
    if enc is not None:
        enc_field, encoded_slot = enc
        if not isinstance(encoded_slot, str):
            return entry, encoded
        seq = entry.get("seq", -1)
        blob = _b64decode(run_id, seq, encoded_slot)
        blob_digest = digest_of(blob)
        enc_ref: BlobRef = {
            "name": blob_name(run_id, blob_digest),
            "digest": blob_digest,
            "size": len(blob),
        }
        await blobs.put(enc_ref["name"], blob)  # blob-before-pointer
        enc_stored = dict(cast("dict[str, object]", entry))
        del enc_stored[enc_field]
        enc_stored[_REF_FIELD_OF[_ENC_SLOTS[enc_field]]] = enc_ref
        enc_entry = cast("JournalEntry", enc_stored)
        return enc_entry, codec.encode(enc_entry)

    slot = payload_slot(entry)
    if slot is None:
        return entry, encoded
    value = cast("dict[str, object]", entry).get(slot)
    if value is None:
        return entry, encoded

    # ``None``: this entry reached here UNtransformed, so its blob is plaintext and the ref stays
    # v:2. An entry the codec did touch took the ``entry_enc`` branch above, which offloads the
    # ciphertext it already holds.
    ref, transformed = await store_payload_blob(
        run_id,
        f"the {slot} of a {entry['kind']} entry (seq {entry['seq']})",
        value,
        codec,
        blobs,
        None,
    )
    stored = with_blob_ref(entry, slot, ref, transformed=transformed)
    return stored, codec.encode(stored)
