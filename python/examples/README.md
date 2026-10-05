# Examples

Runnable, self-contained workflows for `synadia-durable`, the library's examples on the thin SDK. Each one is a small `@workflow` plus a worker/client harness you can read top to bottom. The files are the library's at `7456c75`, unchanged; their header comments predate the engine, so the commands below are the ones to use. TypeScript's are in [`../../examples/`](../../examples/README.md).

| Example                              | Shows                                                                                                                                                                                     |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`charge.py`](charge.py)             | an order workflow with `ctx.run`, `ctx.wait` (a signal), `ctx.sleep` and `ctx.call` (a `ship` sub-workflow), driven by `start`, `signal`, `result` and `status`. The canonical first read |
| [`saga.py`](saga.py)                 | a trip booking whose compensations run in reverse when a step fails, a crash-safe rollback. Two runs: one booked, one rolled back                                                         |
| [`crash_replay.py`](crash_replay.py) | a worker killed mid-run; a fresh one resumes with completed steps never re-executed                                                                                                       |
| [`fan_out.py`](fan_out.py)           | concurrent sub-workflows with `ctx.start_call` + `asyncio.gather`, and `ctx.race`                                                                                                         |
| [`cancel.py`](cancel.py)             | a hard stop with `client.cancel` on a parked run; `result()` raises `CancelledError` and the status is `cancelled`                                                                        |

## Standalone

The quickstart's NATS and engine (the [README](../README.md#quickstart): `make build-dev` and a test licence), then the example with `uv`, from the repo's root:

```sh
nats-server -js &
../durable-engine/bin/durable-engine-dev serve --nats nats://127.0.0.1:4222 --licence de.licence &
cd python
NATS_URL=nats://127.0.0.1:4222 uv run python examples/<name>.py
```

`charge.py` resumes after a kill: stop it while it is parked and re-run it with the same `DE_RUN_ID`, and it continues from the journal.

`crash_replay.py` needs a short ack wait. The library set 2 s on its worker (`ack_wait_ms=2_000`, still passed and now ignored); it is the engine's now, so start the engine with `--ack-wait 2s`, then:

```sh
CRASH=1 DE_RUN_ID=crash-demo NATS_URL=nats://127.0.0.1:4222 uv run python examples/crash_replay.py   # dies before 'attach'
        DE_RUN_ID=crash-demo NATS_URL=nats://127.0.0.1:4222 uv run python examples/crash_replay.py   # resumes: only 'attach' runs
```

## On the fabric

The workflows run unchanged on the fabric; the prefix is `de` on both. **The connection is what differs** ([`../../docs/FABRIC.md`](../../docs/FABRIC.md)): your tenant's user. nats-py reads `user:password@` from `NATS_URL`, so a tenant whose user has a password runs the examples as they are (walked on a local three-account server, [Walk it locally](../../docs/FABRIC.md#walk-it-locally)). The examples take no creds file, so with one (NGS) each example is the code to copy, with its `nats.connect(…)` line replaced by [the fabric's](../../docs/FABRIC.md#the-code). What each shows there:

| Example                                           | On the fabric                                                                                                                                                                                      |
| ------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `charge.py`, `saga.py`, `fan_out.py`, `cancel.py` | as standalone; each run in the console's Runs section with `by` (your user), and under no task, since their run ids name no thread ([the join](../../docs/FABRIC.md#the-join-for-an-agent-author)) |
| `crash_replay.py`                                 | the ack wait is the fabric engine's (`info`'s `ack_wait_ms`, 30 s by default), not yours to set: the fresh worker resumes after it, not after 2 s                                                  |

## The rest

The rest of the library's examples (the AI, scale and journal-dump demos) stay in the library. The `durable_agent` example is left out: its front door registers it as a Synadia agent through the fabric's agent SDK (`synadia-ai-agent-service`), which this repo does not depend on (`docs/findings.md` F18); the fabric SDK's reference agents carry a durable agent of their own (its `docs/durable.md`).

On the fabric every example takes `NATS_CREDS=<your creds file>` next to `NATS_URL` (since 2026-10-05; Python needs `nkeys`): the code does not change, the connection does ([`docs/FABRIC.md`](../docs/FABRIC.md)).
