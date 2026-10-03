"""The payload-codec seam: a bytes→bytes transform over a journal entry's USER PAYLOAD.

It is applied to the ``input`` / ``value`` / ``data`` / ``error`` slot only, leaving the envelope —
``v``, ``seq``, ``kind``, ``ts``, step names, idempotency keys, blob refs — in the clear.

**Why slot-level and not whole-entry.** The replay engine reads ``kind``/``seq``/``name`` to match
commands and run its determinism guards, the loader reads ``v``, and the dashboard reads all of them.
Encrypting the envelope would blind every one. Slot-level gives the property that matters — a
principal with read rights on ``de.journal.>`` sees the SHAPE of a run and none of its data — while
the run stays debuggable.

**Why it is not the** :class:`~synadia_durable.journal.codec.Codec`. That one is the structural JSON
wire encoding, pinned across SDKs by ``conformance/vectors/journal-entries.json``. This is a separate,
deployment-local transform layered on top, so enabling encryption never changes how the wire format
itself is defined and the conformance corpus stays plaintext JSON.

Both directions are ``async`` because real key material lives behind a KMS. It must interoperate
byte-for-byte with the TypeScript SDK's ``PayloadCodec`` for a mixed fleet to share runs — see
:func:`aes_gcm_payload_codec`, whose format matches its twin exactly.
"""

from __future__ import annotations

import os
from typing import Protocol, runtime_checkable

from synadia_durable.errors import PayloadCodecError

__all__ = ["PayloadCodec", "aes_gcm_payload_codec"]

#: AES-GCM standard nonce length, and the prefix length of the wire format below.
_IV_BYTES = 12
_KEY_BYTES = 32


@runtime_checkable
class PayloadCodec(Protocol):
    """A reversible transform over encoded payload bytes: ``decode(await encode(x)) == x``.

    It need NOT be deterministic — a fresh IV per call is expected and preferred. A payload slot is
    transformed once at append time and only ever reversed afterwards, so replay never re-encrypts
    and never compares ciphertexts.

    Raising from ``decode`` parks the run (an operator can fix the key and it resumes) rather than
    failing it — see :class:`~synadia_durable.errors.PayloadCodecError`.
    """

    async def encode(self, plain: bytes) -> bytes: ...

    async def decode(self, encoded: bytes) -> bytes: ...


class _AesGcmPayloadCodec:
    """AES-256-GCM over ``cryptography``. Wire format: ``iv(12) || ciphertext||tag``."""

    __slots__ = ("_aead",)

    def __init__(self, key: bytes) -> None:
        if len(key) != _KEY_BYTES:
            raise PayloadCodecError(
                f"aes_gcm_payload_codec: key must be {_KEY_BYTES} bytes (AES-256), got {len(key)}"
            )
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        except ImportError as e:  # pragma: no cover - exercised by the import-guard test
            raise PayloadCodecError(
                "aes_gcm_payload_codec needs the 'cryptography' package, which is an optional "
                "extra because the core library has no crypto dependency: "
                "pip install 'synadia-durable[crypto]'"
            ) from e
        self._aead = AESGCM(key)

    async def encode(self, plain: bytes) -> bytes:
        iv = os.urandom(_IV_BYTES)
        return iv + bytes(self._aead.encrypt(iv, plain, None))

    async def decode(self, encoded: bytes) -> bytes:
        if len(encoded) <= _IV_BYTES:
            raise PayloadCodecError("aes_gcm_payload_codec: ciphertext is too short to hold an IV")
        # InvalidTag (and anything else) propagates to the caller, which wraps it in a
        # PayloadCodecError carrying the run and entry — the context this layer does not have.
        return bytes(self._aead.decrypt(encoded[:_IV_BYTES], encoded[_IV_BYTES:], None))


def aes_gcm_payload_codec(key: bytes) -> PayloadCodec:
    """A ready-made AES-256-GCM :class:`PayloadCodec` (needs ``synadia-durable[crypto]``).

    Format: ``iv(12) || ciphertext||tag``, byte-for-byte identical to the TypeScript SDK's
    ``aesGcmPayloadCodec`` — a Python worker and a TypeScript client sharing a key read each other's
    payloads. A fresh random IV per call, so identical payloads do not produce identical ciphertext;
    offloaded blobs therefore stop deduplicating by content, which is correct (content-addressing a
    secret by its cleartext digest would leak equality).

    Key management is deliberately yours. Rotation is out of scope here: a run's journal must stay
    readable for its whole life, so a rotating deployment needs a codec that selects the key from a
    hint it writes into its own output.
    """
    return _AesGcmPayloadCodec(key)
