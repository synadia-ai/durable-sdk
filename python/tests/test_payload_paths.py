"""The structural guards on how a user payload reaches durable storage. Twin of
``test/unit/payload-paths.test.ts``.

A payload gets persisted by three different routes — a journal append, a work item's shrink, the
status hint's shrink — and each one used to decide for itself whether the payload codec applied. One
of them silently did not, so the same value was encrypted when it fit inline and stored in the clear
when it did not. Every other test checks a BEHAVIOUR; these check that the shape of the bug is
unavailable, so a fourth route cannot reintroduce it by omission.

Python cannot express "omitting this argument is a compile error" the way TypeScript can, so the
equivalent guard is done two ways: ``mypy`` rejects the call (the gate runs it, and the test below
asserts the signature really has no default), and the source rule keeps the choke point sole.
"""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any

import pytest

from synadia_durable.journal.blobs import BlobStore, store_payload_blob
from synadia_durable.journal.codec import json_codec
from synadia_durable.journal.payload_codec import aes_gcm_payload_codec

SRC = Path(__file__).resolve().parents[1] / "src" / "synadia_durable"
SOURCES = {p.relative_to(SRC).as_posix(): p.read_text(encoding="utf-8") for p in SRC.rglob("*.py")}


class _Store(BlobStore):
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def put(self, name: str, data: bytes) -> None:
        self.objects[name] = data

    async def get(self, name: str) -> bytes:
        return self.objects[name]


def test_only_blobs_py_writes_a_payload_blob() -> None:
    """A module reaching the store itself would be a fourth route, invisible until somebody read a
    bucket. ``_nats/blob_store.py`` is the adapter that IMPLEMENTS the seam, so it is exempt."""
    allowed = {"journal/blobs.py", "_nats/blob_store.py"}
    offenders = [
        path
        for path, text in SOURCES.items()
        if path not in allowed and "blobs.put(" in text.replace(" ", "")
    ]
    assert offenders == []


def test_store_payload_blob_is_defined_exactly_once() -> None:
    definitions = [p for p, t in SOURCES.items() if "async def store_payload_blob(" in t]
    assert definitions == ["journal/blobs.py"]


def test_every_caller_outside_the_choke_point_is_a_known_payload_route() -> None:
    """Not a restriction — a roster. A fourth route fails this, and whoever adds it has to come and
    say what it is, which is the conversation that did not happen last time."""
    callers = sorted(
        path
        for path, text in SOURCES.items()
        if path != "journal/blobs.py" and "store_payload_blob(" in text
    )
    # durable-sdk: ``_engine/payload.py`` (``fit_slot``) is the fourth route, and it says what it is:
    # a start's input, a signal's data or a child's input too large for one request, ``put`` in
    # parts (F1, F25 of the engine). The codec decision is still this function's.
    assert callers == [
        "_engine/payload.py",  # a start's input, a signal's data, a child's input over request_max
        "_nats/protocol.py",  # a work item's signal data / child outcome
    ]


def test_the_payload_codec_argument_has_no_default() -> None:
    """The guard itself. A default is exactly the diff that let an oversized signal be offloaded in
    the clear — it type-checked cleanly at the time, because the parameter was optional. With no
    default, ``None`` is a decision a caller writes down and mypy rejects omitting it."""
    parameter = inspect.signature(store_payload_blob).parameters["payload_codec"]
    assert parameter.default is inspect.Parameter.empty


@pytest.mark.asyncio
async def test_transformed_comes_from_the_write_not_from_the_caller() -> None:
    # The second half of the same bug was a marker kept in PARALLEL with the write: the entry said
    # v:2 while the blob held ciphertext. There is only one source for it now.
    blobs = _Store()
    _plain_ref, plain_transformed = await store_payload_blob(
        "r", "x", {"a": 1}, json_codec, blobs, None
    )
    assert plain_transformed is False
    _enc_ref, enc_transformed = await store_payload_blob(
        "r", "x", {"a": 1}, json_codec, blobs, aes_gcm_payload_codec(bytes([7]) * 32)
    )
    assert enc_transformed is True


@pytest.mark.asyncio
async def test_the_plaintext_write_is_plaintext_and_the_transformed_one_is_not() -> None:
    blobs = _Store()
    secret: dict[str, Any] = {"card": "4242424242424242"}
    plain_ref, _ = await store_payload_blob("r", "x", secret, json_codec, blobs, None)
    enc_ref, _ = await store_payload_blob(
        "r", "x", secret, json_codec, blobs, aes_gcm_payload_codec(bytes([7]) * 32)
    )
    assert b"4242424242424242" in blobs.objects[plain_ref["name"]]
    assert b"4242424242424242" not in blobs.objects[enc_ref["name"]]
    # …and they land under DIFFERENT content-addressed names, because one is ciphertext.
    assert plain_ref["name"] != enc_ref["name"]
