# `synadia-durable` (Python)

Lightweight **durable execution on NATS/JetStream** — a library, not a server.

This is the Python implementation of the same wire protocol as the TypeScript
[`@synadia-ai/durable`](../README.md): [`docs/PROTOCOL.md`](../docs/PROTOCOL.md). Both SDKs read and
write the same streams, so a Python client can drive a TypeScript worker, a Python workflow can call a
TypeScript sub-workflow, and either one's runs show up in the same observability UI.

> **Status: feature-complete, and the operational tranche is in.** `create_worker` executes workflows
> — steps, timers, signals, sub-workflows, durable fan-out, cancellation, continue-as-new and
> version-pinned dispatch — and `create_client` starts, observes, signals and cancels runs against
> workers written in **either** SDK (there are tests driving a real TypeScript worker from Python and
> the reverse). **495 tests green**, including a multi-worker chaos suite and a feature × concurrency matrix. See
> [`docs/DESIGN-python-sdk.md`](../docs/DESIGN-python-sdk.md) for the design and the phasing.

## Is it production-ready?

**Not yet — but the reason is mileage, not missing machinery.** The correctness core is sound and
proved against the reference implementation in both directions, and the operational hardening the
TypeScript package accumulated (phase **P3** in
[`../docs/DESIGN-python-sdk.md`](../docs/DESIGN-python-sdk.md)) is now all here. It has also been
through the first load runs against NGS alongside the TypeScript package (`../bench/README.md`),
which found five server-side consumer leaks in the client-library abstractions both SDKs read
through; all are fixed here too, every read is a direct get now, every integration test fails if it
leaves an ephemeral consumer on the server, and the whole suite runs under NGS-shaped account limits
(`DE_TEST_PROFILE`, default `ngs`). What is genuinely missing is the same thing
missing from the TypeScript package: **it is unpublished — not on PyPI, no managed option, and no
production track record.** One convenience the reference implementation has and
this one does not: a per-instance blob **read** cache (a performance nicety, not a correctness gap).

The two **parity gaps** that used to sit here are closed: the lease's renewal-**lost** path (the
lease is taken over mid-drive; the worker notices, stops renewing and carries on, because the journal
CAS is what protects correctness — and its guarded delete cannot revoke the new owner's key) and the
**journal-load completeness guard** (a short read fails loud rather than handing the engine a
truncated journal) are now exercised here too, each with a control. A correctness guard tested in one
SDK and not the other is the asymmetry that already produced one cross-SDK divergence, so they are
worth keeping paired.

Everything that used to head this list is **closed.** Timer hygiene: the worker purges consumed
wakes, warns before `DE_TIMERS` can start evicting, and repairs a wake the stream lost by re-deriving
it from the journal (`DE_PARKED` + the reconcile sweep, PROTOCOL §6.7) — the control proves the loss
is real, since with `reconcile_timers=False` the same run never wakes. And the **gating run lease**
(`DE_LEASES`, PROTOCOL §6.6, default on, advisory, fail-open): a signal storm across two workers now
executes every step exactly once, where the same storm with `lease=False` duplicates them. And
**ack-progress heartbeats** (PROTOCOL §6.4): a step that outlives the ack wait is no longer
redelivered mid-execution, bounded by `max_work_ms` (default 10× the ack wait) so a genuinely hung
step still lets go — of the ack extension _and_ of the run lease, so another worker can take the run
over. Plus **bounded concurrency**: 16 work items in flight per worker by default, with items for
the same run always serialized in-process, so one slow step no longer head-of-line-blocks every
other run of that workflow — and `stop()` drains the work it dispatched, not just the pumps. And
**large-payload offload now WRITES** as well as reads (PROTOCOL §5.6): an oversized value goes to
`DE_BLOBS` under a content-addressed name and the wire carries a ref, in the journal, on the work
queue and in the status hint. A TypeScript client resolving a payload this SDK offloaded is a test,
not a hope. And a **metrics seam plus `health()`**: journal appends and loads with their outcomes,
work items with their ack/NAK reason, pump restarts, the `DE_TIMERS` headroom gauge, lease
arbitrations and blob traffic — everything the tranches above compute and used to discard.

**And the chaos suite now exists too** — four workers under a signal storm, a fleet killed mid-run
and replaced, and a fan-out losing a worker while children, a timer and a signal are all in flight.
It audits journals rather than results: one terminal per run, one `step-end` per command seq, no
stranded runs. On its first run it found a real bug **in both SDKs** — a fenced inbound-event append
was treated as already-handled, which silently dropped a `child-done` and parked its parent forever
(now PROTOCOL §6.4, LEARNINGS #51). A Python **client** against TypeScript
**workers** remains the most conservative shape — that path is complete and
`tests/integration/test_interop.py` exercises it end to end — but Python workers are no longer the
liability they were two tranches ago.

**Beyond chaos, two suites answer questions the feature tests cannot.** `test_matrix.py` crosses each
feature with CONTENTION — a version-pinned run driven by two workers at once, a continue-as-new
rollover raced at the boundary, a cancel arriving mid-step, an offloaded payload under a signal storm
— and asserts the storm actually piled up, so it cannot pass having quietly run sequentially.
`test_worker_recovery.py` covers the paths that only run once something has already gone wrong (a
poison work item, a work item for a run with no journal, an event append that keeps losing its CAS, a
stale status hint after a crash, a deleted `DE_TIMERS`) and asserts the **ack decision** — ack vs
NAK-with-backoff vs term — rather than a return value, because several ack choices produce the same
result and only one of them is right.

## Try it

```sh
nats-server -js &
cd python && uv sync
NATS_URL=nats://127.0.0.1:4222 uv run python examples/crash_replay.py   # see examples/README.md
```

[`examples/`](examples/) holds a runnable program for everything this library does, each one a
single file that prints what it did: the core surface (`charge.py`), crash and replay in two
commands (`crash_replay.py`), the everyday failure modes (`recovery.py`), the exactly-once guarantee
and its caveat (`exactly_once.py`), what breaks replay (`determinism.py`), sagas, approvals,
continue-as-new, fan-out, cancellation, horizontal scale, the raw journal on the wire, two
production recipes, and — in [`examples/durable_agent/`](examples/durable_agent/) — two durable AI
agents whose tool-calling loop IS the workflow. The Python-shaped half (blocking steps on threads,
`ctx.key()` as an idempotency key, inline retry) is `blocking_io.py`. Their index, with what each
one teaches: [`examples/README.md`](examples/README.md).

Every example connects with `nats.connect(url, **with_reconnect_defaults())`, and a worker you
leave running should too: nats-py's own default gives up after 60 reconnect attempts and then
closes the connection for good — which a worker can report (`health().connection_closed`) but
never undo. See [`docs/OPERATIONS.md`](../docs/OPERATIONS.md) "Connections, outages and latency".

## Develop

```sh
cd python
uv sync                 # create the venv and install dev deps
uv run ruff format .    # format
uv run ruff check .     # lint
uv run mypy             # typecheck (strict)
uv run pytest           # test
```

Everything at once, the analogue of the TypeScript side's `bun run check`:

```sh
uv run ruff format --check . && uv run ruff check . && uv run mypy && uv run pytest
```

## Layout

Deliberately a file-for-file mirror of the TypeScript `src/`, so the two can be diffed by eye:

|                                 |                                                                           |
| ------------------------------- | ------------------------------------------------------------------------- |
| `src/synadia_durable/journal/`  | the wire layout, the codec, and the append/load seam                      |
| `src/synadia_durable/core/`     | the replay engine — **pure**, no `nats` import, testable without a server |
| `src/synadia_durable/client.py` | start / result / status / signal / cancel                                 |
| `src/synadia_durable/_nats/`    | the shell: the only package that talks to `nats-py`                       |

`../conformance/vectors/` holds journal bytes generated from the reference implementation; both SDKs
assert they reproduce them exactly.
