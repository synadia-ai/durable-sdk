# Examples

Runnable, self-contained workflows for `synadia-durable`, the library's examples on the thin SDK. Each one is a small `@workflow` plus a worker/client harness you can read top to bottom. The files are the library's at `7456c75`, unchanged; their header comments predate the engine, so the commands below are the ones to use.

Run any of them with a local NATS, the engine next to it, and `uv`:

```sh
nats-server -js &
durable-engine serve --nats nats://127.0.0.1:4222 &
cd python
NATS_URL=nats://127.0.0.1:4222 uv run python examples/<name>.py
```

- **[`charge.py`](charge.py)**: an order workflow with `ctx.run`, `ctx.wait` (a signal), `ctx.sleep` and `ctx.call` (a `ship` sub-workflow), driven by `start`, `signal`, `result` and `status`. The canonical first read.
- **[`saga.py`](saga.py)**: a trip booking whose compensations run in reverse when a step fails, a crash-safe rollback. Two runs: one booked, one rolled back.
- **[`crash_replay.py`](crash_replay.py)**: kill a worker mid-run; a fresh one resumes with completed steps never re-executed. The library set a 2 s ack wait on its worker (`ack_wait_ms=2_000`, still passed and now ignored); it is the engine's, so start the engine with `--ack-wait 2s`, then:

  ```sh
  CRASH=1 DE_RUN_ID=crash-demo NATS_URL=nats://127.0.0.1:4222 uv run python examples/crash_replay.py   # dies before 'attach'
          DE_RUN_ID=crash-demo NATS_URL=nats://127.0.0.1:4222 uv run python examples/crash_replay.py   # resumes: only 'attach' runs
  ```

- **[`fan_out.py`](fan_out.py)**: concurrent sub-workflows with `ctx.start_call` + `asyncio.gather`, and `ctx.race`.
- **[`cancel.py`](cancel.py)**: a hard stop with `client.cancel` on a parked run; `result()` raises `CancelledError` and the status is `cancelled`.

The rest of the library's examples (the AI, scale and journal-dump demos) stay in the library. The `durable_agent` example is left out: its front door registers it as a Synadia agent through the fabric's agent SDK (`synadia-ai-agent-service`), which this repo does not depend on (`docs/findings.md` F18).
