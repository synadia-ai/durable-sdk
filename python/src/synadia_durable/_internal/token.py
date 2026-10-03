"""synadia-durable — the subject-token ∩ KV-key guard.

Pure (no transport imports) so BOTH the core (``workflow``) and the NATS shell (client, names) can
fail loud at the earliest boundary. Mirrors ``src/internal/token.ts``.
"""

from __future__ import annotations

import re

# A runId / workflow name is used as BOTH a subject token (`de.journal.{runId}`) AND a KV key
# (`DE_RUNS`). KV keys reject characters that subjects allow (notably `~`), so require the safe
# intersection: ASCII alphanumerics, `-`, `_`. (continue-as-new chains use `_g{n}`; `ctx.call`
# children use `_{seq}` — both within this set.) PROTOCOL §4.2.
#
# Anchored with `fullmatch` and NOT with `^…$`, which is where a literal transcription of the
# TypeScript regex goes wrong: Python's `$` also matches just BEFORE a trailing newline, so
# `^[A-Za-z0-9_-]+$` accepts "checkout\n" — a newline smuggled into a NATS subject and a KV key.
# JavaScript's `$` has no such allowance. `fullmatch` means what both regexes were trying to say.
_SAFE_TOKEN = re.compile(r"[A-Za-z0-9_-]+")


def assert_token(kind: str, value: str) -> None:
    """Raise unless ``value`` is safe as a subject token AND a KV key (runIds / workflow names)."""
    if not _SAFE_TOKEN.fullmatch(value):
        raise ValueError(
            f"{kind} must be ASCII alphanumeric with '-' or '_' "
            f'(a valid subject token and KV key): got "{value}"'
        )
