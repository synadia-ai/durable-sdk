"""synadia-durable — resource and subject naming (prefix configurable, default ``de``).

PROTOCOL §3–§4. Pure: no ``nats`` import, so the core and the tests can use it freely. It lives under
the shell package because that is where the TypeScript twin keeps it (``src/nats/names.ts``) and
because every consumer of it is shell code.

All participants sharing a logical deployment MUST use the same prefix.
"""

from __future__ import annotations

from synadia_durable._internal.token import assert_token

__all__ = ["Names"]


class Names:
    """Every resource and subject name for one deployment prefix."""

    __slots__ = (
        "blobs_bucket",
        "journal_stream",
        "journal_subject_all",
        "leases_kv",
        "parked_kv",
        "prefix",
        "runs_kv",
        "timer_subject_all",
        "timers_stream",
        "work_stream",
        "work_subject_all",
    )

    def __init__(self, prefix: str = "de") -> None:
        # durable-sdk: against an existing server every prefix is the run's one (tests/existing.py).
        import existing

        if existing.tenant() is not None:
            # As a tenant (DE_TENANT) the tests' connection has no stream: a test that names the
            # engine's resources to read them is skipped, saying so (tests/existing.py).
            import pytest

            import os

            test = os.environ.get("PYTEST_CURRENT_TEST", "?").rsplit(" ", 1)[0]
            pytest.skip(f"{test} {existing.AS_TENANT}")
        prefix = existing.map_prefix(prefix)
        assert_token("prefix", prefix)
        up = prefix.upper()
        self.prefix = prefix
        self.journal_stream = f"{up}_JOURNAL"
        self.work_stream = f"{up}_WORK"
        self.timers_stream = f"{up}_TIMERS"
        self.runs_kv = f"{up}_RUNS"
        self.leases_kv = f"{up}_LEASES"
        #: Index of runs asleep on a timer (PROTOCOL §6.7).
        self.parked_kv = f"{up}_PARKED"
        #: Object-store bucket for offloaded payload blobs (backing stream ``OBJ_<bucket>``).
        self.blobs_bucket = f"{up}_BLOBS"
        self.journal_subject_all = f"{prefix}.journal.*"
        self.work_subject_all = f"{prefix}.work.*"
        self.timer_subject_all = f"{prefix}.timer.>"

    def journal_subject(self, run_id: str) -> str:
        """The run's append-only journal — the source of truth."""
        return f"{self.prefix}.journal.{run_id}"

    def runs_hint_subject(self, run_id: str) -> str:
        """The core subject a run's status-KV record is published on (``$KV.{bucket}.{runId}``).

        A plain subscription on it is the wake-up hint ``result()`` uses — no consumer, no
        JetStream API call.
        """
        return f"$KV.{self.runs_kv}.{run_id}"

    def work_subject(self, workflow: str) -> str:
        """The work queue for a workflow (start / wake items)."""
        return f"{self.prefix}.work.{workflow}"

    def timer_sched_subject(self, run_id: str, seq: int) -> str:
        """A timer's schedule-definition message."""
        return f"{self.prefix}.timer.sched.{run_id}.{seq}"

    def timer_wake_subject(self, workflow: str) -> str:
        """Where a fired timer delivers its wake work item.

        Under ``{prefix}.timer.>`` — and it has to be: a schedule's target MUST live in the same
        stream as the schedule, so the wake subject cannot be moved next to the work queue.
        """
        return f"{self.prefix}.timer.wake.{workflow}"

    def work_consumer(self, workflow: str) -> str:
        """The durable pull consumer on the work stream for one workflow (PROTOCOL §6.3)."""
        return f"{self.prefix}-worker-{workflow}"

    def timer_consumer(self, workflow: str) -> str:
        """The durable pull consumer on the timers stream for one workflow (PROTOCOL §7)."""
        return f"{self.prefix}-timer-{workflow}"

    def __repr__(self) -> str:
        return f"Names(prefix={self.prefix!r})"
