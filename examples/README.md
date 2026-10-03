# Examples

Runnable, self-contained workflows for `@synadia-ai/durable`. Each one is a small `defineWorkflow` plus a
worker/client harness you can read top to bottom.

Run any of them with a local NATS and its npm script:

```sh
nats-server -js &
NATS_URL=nats://127.0.0.1:4222 npm run example:<name>
```

Most pair with an **illustrated [walkthrough](../docs/walkthroughs/)** — the same scenario explained with
real dashboard screenshots and the concept it demonstrates. The model call in the AI examples is stubbed,
so everything here runs offline with no API keys.

### The basics

- **[`example:charge`](./charge.ts)** — an order workflow showcasing `run` + `await` + `sleep` + `call`;
  the canonical first read. → [Anatomy of a workflow](../docs/walkthroughs/anatomy.md)
- **[`example:seed`](./seed-demo.ts)** — seeds a realistic mix of runs (completed / failed / parked / a
  continue-as-new chain) to populate the dashboard. → [UI tour](../docs/walkthroughs/ui-tour.md)

### Correctness & failure

- **[`example:crash-replay`](./crash-replay.ts)** — kill a worker mid-run; a fresh one resumes from the
  journal with completed steps never re-executed. → [Crash & replay](../docs/walkthroughs/crash-replay.md)
- **[`example:recovery`](./recovery.ts)** — the everyday failure modes: a throw that fails, a flaky step a
  `retry` recovers, an infra blip that retries. → [When things go wrong](../docs/walkthroughs/when-things-go-wrong.md)
- **[`example:exactly-once`](./exactly-once.ts)** — the guarantee in action: an effect that fires once
  across a crash, keyed on `ctx.key()`. → [Exactly-once](../docs/walkthroughs/exactly-once.md)
- **[`example:determinism`](./determinism.ts)** — what _breaks_ replay (wall-clock, randomness, a renamed
  step) and how the engine catches it loud. → [The rules of workflow code](../docs/walkthroughs/determinism.md)

### Real-world patterns

- **[`example:saga`](./saga.ts)** — a distributed transaction whose compensations run in reverse on
  failure — crash-safe rollback. → [Saga & compensation](../docs/walkthroughs/saga.md)
- **[`example:approval`](./approval.ts)** + **[`example:approval-signal`](./approval-signal.ts)** —
  approvals via `ctx.await` + signals, with a timeout-escalation chain (the helper sends a signal from the
  CLI). → [Human-in-the-loop](../docs/walkthroughs/human-in-the-loop.md)
- **[`example:ai-image-review`](./ai-image-review/)** — generate an image, then **regenerate until a human
  approves** — driven from the dashboard's Approve/Reject buttons. → [Review loop](../docs/walkthroughs/review-loop.md)
- **[`example:cron`](./cron.ts)** — `continueAsNew` as "run forever with a bounded journal": a
  subscription that bills, sleeps, and renews across generations. → [Long-running & cron](../docs/walkthroughs/cron.md)
- **[`example:fan-out`](./fan-out.ts)** · **[`example:cancel`](./cancel.ts)** ·
  **[`example:fanout-cancel-demo`](./fanout-cancel-demo.ts)** · **[`example:saga-cancel`](./saga-cancel.ts)**
  — concurrent sub-workflows (`ctx.startCall` + `Promise.all` / `ctx.race`) and hard-stop `client.cancel`
  with `ctx.onCancel` compensation. → [Fan-out & cancellation](../docs/walkthroughs/fan-out-and-cancellation.md)

### Operating it

- **[`example:scale-worker`](./scale-worker.ts)** + **[`example:scale-seed`](./scale-seed.ts)** — many
  workers share one durable work-queue; the System view shows a burst distribute and drain (shared setup
  in [`scale-shared.ts`](./scale-shared.ts)). → [Scaling out](../docs/walkthroughs/scaling-out.md)

### Putting it together

- **[`example:checkout`](./checkout.ts)** — the capstone: `run` + `await` + `sleep` + `call` (a shipping
  sub-workflow) + a saga unwind. → [Build a real app](../docs/walkthroughs/build-a-real-app.md)
- **[`example:journal-dump`](./journal-dump.ts)** — print a run's raw `v:1` journal entries straight off
  the stream. → [The journal on the wire](../docs/walkthroughs/journal-on-the-wire.md)

### Production recipes

- **[`example:ai-image`](./ai-image/)** — an expensive, non-idempotent model call made crash-safe and
  exactly-once (the real OpenAI call, stubbed offline). → [recipe](../docs/recipes/ai-image.md) ·
  [README](./ai-image/README.md)
- **[`example:weekly-email`](./weekly-email/)** — a recurring job via `continueAsNew` (the cron recipe).
  → [README](./weekly-email/README.md)
