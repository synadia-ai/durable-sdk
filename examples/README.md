# Examples

Runnable, self-contained workflows for `@synadia-ai/durable`, the library's examples on the thin SDK. Each one is a small `defineWorkflow` plus a worker/client harness you can read top to bottom. The files come from the retired library and were edited since; their header comments predate the engine, so the commands below are the ones to use. Python's are in [`../python/examples/`](../python/examples/README.md).

| Example                                     | Shows                                                                                                                                                           |
| ------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`example:charge`](./charge.ts)             | an order workflow with `run`, `await`, `sleep` and `call` (a `ship` sub-workflow), driven by `start`, `signal`, `result` and `status`. The canonical first read |
| [`example:saga`](./saga.ts)                 | a trip booking whose compensations run in reverse when a step fails, a crash-safe rollback. Two runs: one booked, one rolled back                               |
| [`example:crash-replay`](./crash-replay.ts) | a worker killed mid-run; a fresh one resumes with completed steps never re-executed                                                                             |
| [`example:fan-out`](./fan-out.ts)           | concurrent sub-workflows with `ctx.startCall` + `Promise.all`, and `ctx.race`                                                                                   |
| [`example:cancel`](./cancel.ts)             | a hard stop with `client.cancel`, and `ctx.onCancel` compensation                                                                                               |

## Standalone

The quickstart's NATS and engine (the root [README](../README.md#quickstart): the engine Synadia sent you, with your licence), then the example's npm script from the repo's root:

```sh
nats-server -js &
./durable-engine_<version>_<os>_<arch>/durable-engine serve --nats nats://127.0.0.1:4222 --licence durable.licence &
NATS_URL=nats://127.0.0.1:4222 npm run example:<name>
```

`charge` resumes after a kill: stop it while it is parked and re-run it with the same `DE_RUN_ID`, and it continues from the journal.

`crash-replay` needs a short ack wait. The library set 2 s on its worker; it is the engine's now, so start the engine with `--ack-wait 2s`, then:

```sh
CRASH=1 DE_RUN_ID=crash-demo NATS_URL=nats://127.0.0.1:4222 npm run example:crash-replay   # dies before 'attach'
        DE_RUN_ID=crash-demo NATS_URL=nats://127.0.0.1:4222 npm run example:crash-replay   # resumes: only 'attach' runs
```

## On the fabric

The workflows run unchanged on the fabric; the prefix is `de` on both. **The connection is what differs** ([`../docs/FABRIC.md`](../docs/FABRIC.md)): your tenant's user and its creds file, where the examples read `NATS_URL` alone and take no creds, and nats.js takes no `user:password@` from a URL. So on the fabric each example is the code to copy, with its `connect(…)` line replaced by [the fabric's](../docs/FABRIC.md#the-code). What each shows there:

| Example                               | On the fabric                                                                                                                                                                                   |
| ------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `charge`, `saga`, `fan-out`, `cancel` | as standalone; each run in the console's Runs section with `by` (your user), and under no task, since their run ids name no thread ([the join](../docs/FABRIC.md#the-join-for-an-agent-author)) |
| `crash-replay`                        | the ack wait is the fabric engine's (`info`'s `ack_wait_ms`, 30 s by default), not yours to set: the fresh worker resumes after it, not after 2 s                                               |

## The rest

The rest of the library's examples (the dashboard, the AI and scale demos) and its walkthroughs stayed in the library, which is retired. The byte-level journal of a whole run is in the durable execution protocol, Appendix C.

On the fabric every example takes `NATS_CREDS=<your creds file>` next to `NATS_URL` (Python needs `nkeys`): the code does not change, the connection does ([`docs/FABRIC.md`](../docs/FABRIC.md)).
