"""``js_error_code`` only, from the library's ``_nats/resources.py`` at ``7456c75`` (the one helper
``journal_store.py`` imports). Provisioning is the engine's."""

from __future__ import annotations


def js_error_code(err: BaseException) -> int | None:
    """The JetStream ``err_code`` of an API error, if it has one.

    nats-py maps API errors by HTTP-ish status (400/404/500/503), NOT by ``err_code`` — so a "wrong
    last sequence" arrives as ``BadRequestError`` and a missing stream as ``NotFoundError``. Branch
    on the code, never on the class.
    """
    code = getattr(err, "err_code", None)
    return code if isinstance(code, int) else None
