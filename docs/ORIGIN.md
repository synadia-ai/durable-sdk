# Origin

Which file came from where. The library is `@synadia-ai/durable`, `../synadia-durable-execution` (GitHub `synadia-ai/durable-ts`), **frozen at `7456c75`**: copied from, never edited, never imported at run time. Its `docs/PROTOCOL.md` (normative), `docs/IMPLEMENTATION-GUIDE.md` and `docs/DESIGN.md` stay there and are cited by path. The engine is `../durable-engine` at the commit `engine.pin` names (`d8c0d73`); its `docs/API.md` is the contract `src/engine/` is built from.

Every path below is the same in this repo and in the library unless a second path is given, so a diff against `7456c75` stays readable:

```sh
git -C ../synadia-durable-execution show 7456c75:src/core/ctx.ts | diff - src/core/ctx.ts   # empty
```

## Copied unchanged from `7456c75` (the diff is empty)

| Area                            | Files                                                                                                                                                                                                      |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The pure core                   | `src/core/{ctx,execute,registry,workflow,work-item,children,scheduler,inbound}.ts` (`inbound.ts`: docs/findings.md F3)                                                                                     |
| The journal                     | `src/journal/{entry,codec,payload-codec,store,blobs}.ts` (all of `blobs.ts`: F3)                                                                                                                           |
| The rest of the pure side       | `src/errors.ts`, `src/types.ts`, `src/internal/{assert,json,logger,metrics,token}.ts`                                                                                                                      |
| Two pure files of the old shell | `src/nats/protocol.ts` (the `payload-codec` unit suite's `shrinkWorkItem`), `src/nats/connect.ts` (`withReconnectDefaults`): F3                                                                            |
| Unit suites (10)                | `test/unit/{replay,cancel,fanout-foundation,registry,codec,payload-codec,payload,wire-vectors,simulate,sim-findings,version}.test.ts` (`version` passes unchanged against the edited `src/version.ts`)     |
| Test harness                    | `test/harness/sim/{index,program,rng,world}.ts`, `test/harness/{replayable,payload,nats-server,proxy}.ts`, `test/harness/server-profiles.json`, `test/harness/shell/direct.ts` (from `src/nats/direct.ts`) |
| Golden vectors                  | `conformance/vectors/journal-entries.json` (34 vectors, read by `wire-vectors`)                                                                                                                            |
| Interop                         | `conformance/interop/ts-worker.ts`                                                                                                                                                                         |
| Examples                        | `examples/{charge,saga,crash-replay,fan-out,cancel}.ts`                                                                                                                                                    |
| Tooling                         | `tsconfig.json`, `tsconfig.build.json`, `tsup.config.ts`, `eslint.config.mjs`, `.prettierrc`, `.prettierignore`, `LICENSE`                                                                                 |

## Copied with edits

| File                                                          | Edit                                                                                                                                                                                                                                             |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `src/version.ts`                                              | `VERSION` `0.6.1` → `0.7.0-alpha` (one line; F4)                                                                                                                                                                                                 |
| `src/index.ts`                                                | the same exports minus the shell's (`provision`, `deprovision`, `resourceStreams`, `ProvisionOptions`, `names`, `Names`); `createClient` / `createWorker` from `src/engine/`; `WorkItem` from `src/core/work-item.ts`; the engine's errors added |
| `test/unit/payload-paths.test.ts`                             | `nats/runs.ts` removed from one roster (one line; F4)                                                                                                                                                                                            |
| `test/unit/call-roster.test.ts`                               | rewritten as the SDK's roster; the library's rosters `src/nats/` (F4)                                                                                                                                                                            |
| `test/harness/global-setup.ts`                                | resolves the engine binary first and skips the suites without one                                                                                                                                                                                |
| `test/harness/setup-each.ts`                                  | stops the test's engines before it deletes the streams                                                                                                                                                                                           |
| `test/harness/leftovers.ts`, `test/harness/blobs.ts`          | imports re-pointed to `test/harness/shell/`                                                                                                                                                                                                      |
| `test/harness/shell/{journal-store,blob-store,runs,names}.ts` | from `src/nats/`, imports re-pointed (`../` → `../../../src/`); the suites' own staging and reading of the streams (F13)                                                                                                                         |
| `test/harness/shell/resources.ts`                             | `jsErrorCode` only, from `src/nats/resources.ts`                                                                                                                                                                                                 |
| `test/integration/*.test.ts` (10)                             | imports; what a suite did with the shell, dropped with a note or re-expressed through the engine; blocked tests `it.skip` with their finding. Every edit beyond an import is marked `durable-sdk:` in the file                                   |
| `examples/README.md`                                          | trimmed to the five examples that are here, with the engine's command line                                                                                                                                                                       |

## New

| File                                                                             | What                                                                                                                                           |
| -------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/engine/transport.ts`                                                        | the one request function, `{prefix}.api.v1.<op>`, and the closed set of error codes                                                            |
| `src/engine/payload.ts`                                                          | the two payload forms (`API.md` "Payloads")                                                                                                    |
| `src/engine/client.ts`                                                           | `createClient`: `start`, `signal`, `cancel`, `get`, a handle with `result` and `status`                                                        |
| `src/engine/worker.ts`                                                           | `createWorker`: "The worker's loop" of `API.md`                                                                                                |
| `src/engine/errors.ts`                                                           | `EngineError`, `LeaseExpiredError`, `InvalidRequestError`, `EngineUnavailableError`, `RunNotFoundError`, `EngineFencedError` (a `FencedError`) |
| `src/engine/names.ts`                                                            | the prefix and the subject                                                                                                                     |
| `test/harness/engine.ts`, `test/harness/engine-bin.ts`                           | `durable-engine serve` per test prefix; the binary from `DE_ENGINE` or built at `engine.pin`                                                   |
| `vitest.config.ts`, `vitest.integration.config.ts`, `package.json`, `engine.pin` | the unit run (no server), the integration run, the package (`0.7.0-alpha`, one dependency), the engine's commit                                |
