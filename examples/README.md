# Examples

Runnable, self-contained workflows for `@synadia-ai/durable`, the library's examples on the thin SDK. Each one is a small `defineWorkflow` plus a worker/client harness you can read top to bottom. The files are the library's at `7456c75`, unchanged; their header comments predate the engine, so the commands below are the ones to use.

Run any of them with a local NATS, the engine next to it, and its npm script:

```sh
nats-server -js &
durable-engine serve --nats nats://127.0.0.1:4222 &
NATS_URL=nats://127.0.0.1:4222 npm run example:<name>
```

- **[`example:charge`](./charge.ts)**: an order workflow with `run`, `await`, `sleep` and `call` (a `ship` sub-workflow), driven by `start`, `signal`, `result` and `status`. The canonical first read. Kill it while it is parked and re-run it with the same `DE_RUN_ID`: it resumes from the journal.
- **[`example:saga`](./saga.ts)**: a trip booking whose compensations run in reverse when a step fails, a crash-safe rollback. Two runs: one booked, one rolled back.
- **[`example:crash-replay`](./crash-replay.ts)**: kill a worker mid-run; a fresh one resumes with completed steps never re-executed. The library set a 2 s ack wait on its worker; it is the engine's now, so start the engine with `--ack-wait 2s`, then:

  ```sh
  CRASH=1 DE_RUN_ID=crash-demo NATS_URL=nats://127.0.0.1:4222 npm run example:crash-replay   # dies before 'attach'
          DE_RUN_ID=crash-demo NATS_URL=nats://127.0.0.1:4222 npm run example:crash-replay   # resumes: only 'attach' runs
  ```

- **[`example:fan-out`](./fan-out.ts)**: concurrent sub-workflows with `ctx.startCall` + `Promise.all`, and `ctx.race`.
- **[`example:cancel`](./cancel.ts)**: a hard stop with `client.cancel`, and `ctx.onCancel` compensation.

The rest of the library's examples (the dashboard, the AI and scale demos) stay in the library; its walkthroughs are under `../synadia-durable-execution/docs/walkthroughs/`.
