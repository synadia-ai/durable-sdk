# Examples

Runnable, self-contained workflows for `synadia-durable`. Each one is a small `@workflow` plus a
worker/client harness you can read top to bottom, and each prints what it did.

They need a local NATS and nothing else:

```sh
nats-server -js &
cd python
NATS_URL=nats://127.0.0.1:4222 uv run python examples/charge.py
```

Most have a TypeScript twin in [`../../examples/`](../../examples) with the same structure, so the
two SDKs can be read side by side. The model calls in the AI examples are stubbed, so everything
here runs offline with no API keys.

## The one to run first

`crash_replay.py`, in two commands — it is the whole idea in fifteen seconds:

```sh
# phase 1: two steps run, then the worker "crashes" before the third
CRASH=1 DE_RUN_ID=crash-demo NATS_URL=nats://127.0.0.1:4222 uv run python examples/crash_replay.py
# phase 2: a FRESH process picks the run back up
        DE_RUN_ID=crash-demo NATS_URL=nats://127.0.0.1:4222 uv run python examples/crash_replay.py
```

```
--- phase 1 ---                         --- phase 2 ---
→ workflow entered for vm-1             → workflow entered for vm-1
   ⚡ allocate-ip   EXECUTED                ⚡ attach        EXECUTED
   ⚡ create-disk   EXECUTED             result: {'ip': '10.0.0.7', 'disk': 'disk-42', …}
   ✗ CRASH — the worker dies here
```

Phase 2 re-enters the workflow from the top — the body is just control flow — but the two completed
steps are read from the journal instead of re-running, so neither prints. Only `attach` executes,
and the result stitches together values produced in two different processes with each side effect
having fired exactly once.

### The basics

- **[`charge.py`](charge.py)** — the core surface in one order workflow: `ctx.run`, `ctx.wait` (a
  signal), `ctx.sleep` (a durable timer), `ctx.call` (a sub-workflow), plus
  `client.start` / `signal` / `result` / `status`. The canonical first read.
- **[`blocking_io.py`](blocking_io.py)** — the Python-shaped half: a blocking vendor call as a step
  (it runs on a thread), `ctx.key()` as the vendor's idempotency key, and an inline `RetryPolicy`.

### Correctness & failure

- **[`crash_replay.py`](crash_replay.py)** — kill a worker mid-run; a fresh one resumes from the
  journal with completed steps never re-executed.
- **[`recovery.py`](recovery.py)** — the everyday failure modes: a raise that fails the run, a flaky
  step a retry recovers, a wait that runs out of time and falls back.
- **[`exactly_once.py`](exactly_once.py)** — the guarantee AND its caveat: an effect that fires
  twice across a crash, both times under the same `ctx.key()`.
- **[`determinism.py`](determinism.py)** — what _breaks_ replay (here: a renamed step under a
  running instance) and how the engine catches it loud.

### Real-world patterns

- **[`saga.py`](saga.py)** — a distributed transaction whose compensations run in reverse on
  failure — crash-safe rollback.
- **[`approval.py`](approval.py)** + **[`approval_signal.py`](approval_signal.py)** — approvals via
  `ctx.wait` + signals, with a timeout-escalation chain. The second is a client-only CLI that sends
  any signal to any run — the same thing the dashboard's "Send signal" button does.
- **[`ai_image_review.py`](ai_image_review.py)** — generate an image, then **regenerate until a
  human approves**, driven from the dashboard or the signal CLI.
- **[`cron.py`](cron.py)** — `continue_as_new` as "run forever with a bounded journal": a
  subscription that bills, sleeps and renews across generations.
- **[`fan_out.py`](fan_out.py)** · **[`cancel.py`](cancel.py)** ·
  **[`saga_cancel.py`](saga_cancel.py)** — concurrent sub-workflows (`ctx.start_call` +
  `asyncio.gather` / `ctx.race`), hard-stop `client.cancel`, and `ctx.on_cancel` compensation.

### Operating it

- **[`scale_worker.py`](scale_worker.py)** + **[`scale_seed.py`](scale_seed.py)** — many workers
  share one durable work-queue; a burst distributes and drains across them (shared workflow in
  [`scale_shared.py`](scale_shared.py)). The workers may be a mix of Python and TypeScript.
- **[`journal_dump.py`](journal_dump.py)** — print a run's raw `v:1` journal entries straight off
  the stream.

### Putting it together

- **[`checkout.py`](checkout.py)** — the capstone: `run` + `wait` + `sleep` + `call` (a shipping
  sub-workflow) + a saga unwind.

### Production recipes

- **[`ai_image.py`](ai_image.py)** — an expensive, non-idempotent model call made crash-safe and
  exactly-once (the real OpenAI version is in the commented block at the bottom).
- **[`weekly_email.py`](weekly_email.py)** — a recurring job via `continue_as_new` (the cron recipe).

### Durable agents

- **[`durable_agent/`](durable_agent)** — two durable AI agents (an SRE assistant and a sandboxed
  coding agent) whose tool-calling loop IS a durable workflow: every model turn and every tool call
  is a journaled step, and a dangerous tool parks on a human-approval signal. Crash mid-task and it
  resumes without re-reasoning or re-firing the effect. See its
  [README](durable_agent/README.md).

## Two notes

**Fixed demo runIds start with `py-`.** The workflow NAMES match their TypeScript twins on purpose —
a Python and a TypeScript worker for `saga` share one work queue and one wire format, which is a
feature — but if both SDKs seeded the same runIds, the second start would silently dedupe into the
first SDK's run.

**Prints inside a step happen once; prints in the workflow body happen on every replay.** That is not
a quirk to work around, it is the model: the body is control flow that re-runs, and `ctx.run` is the
thing that does not. `ai_image.py` and `ai_image_review.py` put the same narration on opposite sides
of that line, so running both shows the difference.
