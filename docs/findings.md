# Findings

What building the thin TypeScript SDK found against the engine's API (`../durable-engine/docs/API.md` at `d8c0d73`, which `engine.pin` names), the frozen library (`synadia-durable-execution` at `7456c75`, retired on 2026-10-16; the protocol's text is now the engine's `docs/protocol/`) and the brief (`synadia-agent-fabric/local/briefs/2026-10-03-durable-sdk-ts.md`). Where the brief and `API.md` differ, `API.md` won and the difference is here. Nothing in the engine or the library was changed; each gap in them is an **ask**.

**Date:** 2026-10-03 · **Engine:** `d8c0d73` (`0.2.0-dev`) · **Library:** `7456c75` · **Server:** nats-server 2.14.6 · **Node:** 25.9 · **Python:** 3.14.7, nats-py 2.15.0
**Follow-up:** 2026-10-04 · **Engine:** `1efd2ec` (its PR #3, which answered F1, F2, F5 to F9 as its own F18 to F25) · the brief `2026-10-04-durable-sdk-followup.md`
**Follow-up 2:** 2026-10-04 · **Engine:** `57ae3c9` (its PR #4, pull on demand) · the brief `2026-10-04-durable-sdk-ngs-mode.md`: the harness's mode for an existing server (F29, F30)

F1 to F14 came from the TypeScript SDK (P2-19a). F15 to F21 came from the Python SDK (P2-19b, the brief `2026-10-03-durable-sdk-py.md`); where a TypeScript finding also blocks a Python test, its **Blocks** line says so. F22 onwards came from the follow-up. A finding the engine answered keeps its text and gains a **Closed** line.

| #   | Kind             | One line                                                                                                                            | Blocks                    |
| --- | ---------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ------------------------- |
| F1  | closed           | API v1 has no chunked upload: a value over `max_payload` − 16 KiB cannot reach the engine, where the library offloaded it           | — (was 16 TS + 5 Py)      |
| F2  | closed           | `serve` has no `--offload-threshold`, and at the default threshold the engine never offloads an appended entry                      | — (was 6 Python tests)    |
| F3  | brief vs cut     | The cut needs four files the brief left behind or trimmed: `inbound.ts`, all of `blobs.ts`, `nats/protocol.ts`, `nats/connect.ts`   | —                         |
| F4  | brief vs suites  | Three of the 13 unit suites cannot pass unchanged: `call-roster`, `payload-paths`, `version`                                        | —                         |
| F5  | closed           | A poll cannot be withdrawn: the engine hands an item to a poll whose caller is gone, and the item waits out its ack wait            | —                         |
| F6  | closed           | A signal sent as `dataEnc` is journaled as a blob ref whatever its size, where the library journals a small one inline              | —                         |
| F7  | closed           | With `DE_TIMERS` gone the engine refuses every `poll`; the library's work pump kept serving                                         | —                         |
| F8  | closed           | The engine neither logs nor counts losing a run lease mid-drive                                                                     | —                         |
| F9  | closed           | Past `maxWorkMs` the lease is gone and refuses the holder's late appends: a step longer than `maxWorkMs` never completes            | —                         |
| F10 | note             | Engine-side decisions are invisible to the worker; seven assertions and five waits re-expressed through `info` and the engine's log | —                         |
| F11 | note             | With one engine instance a run's items are serialised: the library's two-worker races no longer race                                | —                         |
| F12 | library note     | A `fail` terminal that cannot be journaled ends as a fence for ever, in the pure core                                               | (with F1)                 |
| F13 | brief vs suites  | The suites' own JetStream client needs `@nats-io/jetstream`, `kv`, `obj` as devDependencies                                         | —                         |
| F14 | behaviour change | Payloads are encrypted before they leave the process: work items carry no plaintext any more                                        | —                         |
| F15 | brief vs suites  | Python's unit suites: four cannot pass unchanged, two are the shell's, one named in the brief does not exist                        | —                         |
| F16 | brief vs cut     | Python's cut keeps three pure files of `_nats/`, and the suites' own JetStream client is the library's shell under `tests/shell/`   | —                         |
| F17 | **nats-py gap**  | nats-py has no status stream and does not fail a request in flight at close: the SDK polls `stats` and `is_closed`                  | —                         |
| F18 | brief vs cut     | The `durable_agent` example is left out: it needs the fabric's agent SDK                                                            | —                         |
| F19 | note             | `upgrade-compat` is deleted in TypeScript; the library's Python tree never had one                                                  | —                         |
| F20 | brief vs suites  | Eight of the library's 21 Python integration suites test the shell only and are not copied; five tests dropped in place             | —                         |
| F21 | behaviour change | Where the Python SDK differs from the TypeScript one, and why (its metrics ask closed)                                              | —                         |
| F22 | note             | The engine checkout carried someone's uncommitted changes; the suites built the pin from `../wt/durable-engine`                     | —                         |
| F23 | note             | A missing blob is parked by the engine before an item reaches the worker; the offload test reads the engine's log                   | —                         |
| F24 | **API gap**      | `get` of a blob that is gone answers `internal`, the code of any operational fault: the SDK must read `internal` as "gone"          | —                         |
| F25 | brief vs suites  | The chunked upload is a fourth payload-blob route: both `payload-paths` rosters name it                                             | —                         |
| F26 | behaviour        | What the chunked transport decides: who mints the runId, `offload: false`, the lease on a part, the cache                           | —                         |
| F27 | **engine gap**   | `status` lags `result` by the worker's `done`: a run whose `result` is terminal can still read `suspended` for a round trip         | —                         |
| F28 | note             | The suites over the delay proxy, at 150 and 300 ms: what changed, and what an NGS run needs                                         | —                         |
| F29 | note             | The harness's mode for an existing server: one prefix, one engine, a purge by subject, the skips by name, the waits by RTT          | 17 TS + 17 Py in the mode |
| F30 | note             | nats-py reads a creds file only with `nkeys` installed; the SDK declares `nats-py` alone                                            | —                         |

---

### F1 — No chunked upload: values over `max_payload` − 16 KiB cannot be journaled

**Where.** `API.md` "Sizes" ("`request_max` … is `max_payload` less 16 KiB … v1 has no chunked upload (the research's §3.9)"; "The SDK sends every value inline"); the library's `src/journal/blobs.ts` and `src/nats/journal-store.ts` (an entry over the threshold writes its slot to `DE_BLOBS` first and journals a ref, so a value is bounded by the object store, not by `max_payload`).

**What.** Every request carries its value inline, so a value cannot be larger than one NATS message. A step result, a start or child input, a signal's data or a failure message over about `max_payload` − 16 KiB is refused: `too_large` by the engine, or, past `max_payload`, by the SDK before it sends (`PayloadTooLargeError`, deterministic, so the pure core fails the run as the library does with `offload: false`). The library journaled such a value through offload. The library's whole offload feature is therefore not reachable through API v1.

**Blocks.** 16 integration tests, each `it.skip` with this finding cited: 9 of `offload` (all but "small runs never touch the blob store"), 5 of `payload-codec` (the large signal, its control, the large child outcome, the completeness sweep and its control), 1 of `matrix` (OFFLOAD × multi-worker), 1 of `worker-recovery` (the oversized child failure, see F12). In Python, 5 tests: 2 of `test_payload_codec_e2e` (the large signal's blob holds ciphertext, and its control), 1 of `test_matrix` (OFFLOAD × multi-worker), and 1 each of `test_interop` and `test_interop_reverse` (the `payloadEnc` flag on a signal of twice `max_payload`, in each direction).

**Ask.** A chunked upload (the research's §3.9): for example `put {runId, slot, part, last}` into the run's blob namespace under the lease, answered with a ref the following `append`, `start`, `child` or `signal` carries, with the engine verifying the digest. Or raise the per-request bound by putting large requests through the object store. Either way the SDK keeps sending whole values and the chunking stays in the transport.

**Closed** (2026-10-04, engine `1efd2ec`, its F25): `put` and `get`; the SDKs' transports chunk any value over `request_max` and fetch a ref they are handed (`src/engine/blobs.ts`, `python/src/synadia_durable/_engine/blobs.py`, F26). All 16 TypeScript and 5 Python tests run and pass.

### F2 — The engine's offload of an appended entry is unreachable

**Where.** `API.md` "Sizes" (`offload_threshold`, "`max_payload` less 16 KiB by default"); `engine/engine.go:96` (`Options.OffloadThreshold`); `cmd/durable-engine/serve.go` (no flag sets it).

**What.** At the default, `offload_threshold` equals `request_max`. An `append` request wraps its entry in `{runId, lease, entry, msgID, expectLastSeq}`, so an entry over the threshold makes a request over `request_max`, which is refused `too_large` before it is read. The offload code in the engine's append path can only run when the threshold is lower, and `serve` has no flag to lower it. (The engine's offload of what it builds itself, child-done values and paged journals, is unaffected.)

**Blocks (Python).** 6 tests. The library's Python offload suite runs on a 1 KiB threshold (`OffloadOptions(threshold_bytes=1)`, clamped to the 1 KiB floor) with values of about 3 KiB, so its refs never appear: 5 of `test_offload` (all but "offload disabled still fails loud") and `test_interop_reverse` "TS reads a payload Python offloaded". Before the skips every value round-tripped; only the `v:2` and `*Ref` assertions failed. Two of `test_metrics` ("offload traffic is visible") and the hygiene pass are dropped for the same reason and F20's.

**Ask.** A `--offload-threshold` flag on `serve`, and a default some margin below `request_max`, so that an entry between the two is offloaded rather than refused. This does not replace F1; it makes the existing code reachable.

**Closed** (2026-10-04, the engine's F22): `serve --offload-threshold`. Python's harness turns `offload=OffloadOptions(threshold_bytes=…)` into the flag; the 6 tests run and pass.

### F3 — The cut, as the suites need it

**Where.** The brief, "The cut": `src/core/` "not `inbound.ts`"; "`blobs.ts` only for what `entry.ts` and `payload-codec.ts` import from it"; "Leave behind: everything under `src/nats/`"; "Keep the library's `withReconnectDefaults`".

**What.** Four files are needed, unchanged, that the brief left out:

- `src/core/inbound.ts`: `simulate` and `sim-findings` (and `test/harness/sim/world.ts`) import `recordInbound` and `hasResolve`. It is pure. The worker never calls it, because the engine records inbound events.
- All of `src/journal/blobs.ts`: `entry.ts` and `payload-codec.ts` import nothing from it. `store.ts` (the `InMemoryJournal`) imports `offloadEntry` and `resolveEntry`, and `payload-codec`, `payload-paths` and `inbound.ts` import `InMemoryBlobStore`, `storePayloadBlob` and `withBlobRef`. So the offload code is present in `src/`. It is reachable from the in-memory journal and the unit suites, never from the worker. The worker's only uses are `encodePayload` and `resolveEntry` with no blob store (`src/engine/payload.ts`), so a ref would fail loud.
- `src/nats/protocol.ts`: the `payload-codec` unit suite imports `shrinkWorkItem`. It is pure (no `@nats-io` import) and kept under its library path so its diff stays empty.
- `src/nats/connect.ts`: `withReconnectDefaults`, `parseNatsUrl` and `RECONNECT_DEFAULTS` live there. It imports only a type from `@nats-io/nats-core`.

### F4 — Three unit suites cannot pass unchanged

**Where.** The brief: "These must pass unchanged: if one needs an edit, the cut is wrong somewhere; a finding, not a tweak."

**What.** No cut makes these three pass unchanged, because each names something the brief itself removes or changes:

- `call-roster` rosters every broker call **under `src/nats/`**, file by file: 18 of its 22 tests name files that are the engine's now. It is rewritten as the SDK's roster (`test/unit/call-roster.test.ts`, 7 tests). The SDK speaks to the broker only through `engine/transport.ts`'s `nc.request` to `{prefix}.api.v1.<op>`. `package.json` depends on `@nats-io/nats-core` alone, and no file under `src/` imports `jetstream`, `kv`, `obj` or a transport, publishes, subscribes, or names a `$JS`, `$KV`, `$O`, journal, work or timer subject. That roster is also the brief's grep, and it fails when a JetStream import is added (checked once on purpose).
- `payload-paths` names `nats/runs.ts` (the `DE_RUNS` status hint, the engine's now) among the callers of `storePayloadBlob`. One line is removed.
- `version` pins `VERSION === package.json version`. The brief sets the package to `0.7.0-alpha`, and the copied `src/version.ts` says `0.6.1`. `VERSION` is edited to `0.7.0-alpha` (one line), since a stale exported version is exactly what the suite guards against.

The other ten pass byte for byte.

### F5 — A poll cannot be withdrawn

**Where.** `API.md` "poll" (the engine holds the request up to `waitMs` and answers the reply inbox); `engine/ops.go` `opPoll` (an item is handed to whichever waiting poll takes it; nothing checks that its caller is still there).

**What.** A worker that stops, or whose connection drops, while polls are waiting cannot tell the engine. The engine may then hand an item to one of those polls. Nobody receives it, so its lease lapses and the item redelivers at its ack wait: 30 s by default, during which the run makes no progress. The SDK mitigates the clean case. `stop()` returns once the items in flight are done but leaves its waiting polls subscribed, and an item one of them is handed later goes straight back (`done {kind: "retry"}`, which the engine naks with a short delay). A connection that closes, or a reply lost to a disconnect, still costs an ack wait.

**Ask.** A way to withdraw a poll, such as `poll` carrying a `pollId` and an `unpoll {pollId}` op, or the engine dropping a waiting poll when its requester's connection goes away. Either would let `stop()` end its polls at once without losing an item to the lapse.

**Closed** (2026-10-04, the engine's F23): every poll carries the worker's `worker` id and `stop()` sends `poll {cancelPoll}`, both SDKs. 16 waiting polls are released 0.9 to 1.9 ms after `stop()` begins (three rounds each, TypeScript and Python), against 29,979 to 29,990 ms before. A connection that drops still leaves its polls to their wait (the engine's **Not done**).

### F6 — An encrypted signal is journaled as a blob ref, whatever its size

**Where.** `API.md` "Payloads" ("a signal sent as `dataEnc` reach[es] the parent's journal as `v:3` entries"); the library: a client puts the signal's **plaintext** on the work item, and the worker's append encrypts it inline (`{"kind":"signal",…,"v":3,"dataEnc":"…"}`); the library's `WorkItem` (`src/core/work-item.ts`) has `data` or `dataRef` and no inline encrypted form.

**What.** Seen on the wire, for a 10-byte signal sent as `dataEnc`: `{"kind":"signal","seq":-1,"name":"card","signalId":"…","v":3,"ts":…,"dataRef":{"name":"<runId>/sha256:…","digest":"sha256:…","size":40}}`. The engine never holds the key, so it cannot put plaintext on the work item. The library's work-item format has no inline encrypted slot, so the engine stores the ciphertext as a blob. Every encrypted signal then costs a blob write and a blob read, and its entry differs from what the library writes for the same signal. Both read back correctly: the library resolves the ref, and so does the SDK, since the engine inflates refs. The same probably holds for a child-done value crossing from an encrypted child, which was not checked separately.

**Blocks.** `payload-codec` "a SMALL signal is encrypted inline" (`typeof sig.dataEnc` is `"undefined"`; its `v:3` and no-plaintext assertions hold).

**Ask.** An inline encrypted slot on the work item (`signal.dataEnc`, `child.valueEnc` / `errorEnc`) for a payload under the offload threshold, journaled verbatim as the entry's `<slot>Enc`. That is the same bytes as the library's, and a blob only when the payload is large.

**Closed** (2026-10-04, the engine's F20): the test runs unchanged and passes.

### F7 — With `DE_TIMERS` gone, every poll is refused

**Where.** The library's `worker-recovery` suite, "the maintenance pass and the pumps survive DE_TIMERS disappearing" ("a broken timer stream degrades timers, it does not stop the worker"); `engine/ops.go` `opPoll` → `bind` (both durables of every named workflow, at each poll).

**What.** After the timers stream is deleted (the LEARNINGS #40 upgrade case), the engine answers every `poll` `unavailable` ("stream not found", 128 refusals in 20 s in the test). A workflow that never sleeps stops too. The maintenance half behaves as the library's: the engine logs `timer-wake purge failed; will retry next tick`, and the test reads that from the engine's log.

**Blocks.** That test (`it.skip`, F7 cited); its maintenance assertion is re-expressed and would pass. The same in Python: `test_worker_recovery` "DE_TIMERS deleted" (skipped, its maintenance wait re-expressed through the engine's log).

**Ask.** Bind and pull the work durable independently of the timers one, so a poll keeps handing out work items while the timers stream is missing, with the timers half logged loud, as the library's pumps did.

**Closed** (2026-10-04, the engine's F19): both tests run and pass, their maintenance wait still read from the engine's log.

### F8 — A lost run lease is silent

**Where.** The library: `src/nats/lease.ts` warns `run lease lost mid-drive` and emits `lease.gate`; the engine: `store/lease.go` `held` (a renewal that finds another owner stops renewing and keeps driving, with no log line and no counter).

**What.** The behaviour matches the library's: the lease is advisory, the run finishes, and the CAS carries correctness. Nothing records that it happened, though, so an operator cannot see two holders overlapping.

**Blocks.** `worker-recovery` "the run lease being taken away mid-drive" (its first assertion, that the run finishes, holds; the log assertion has nothing to read).

**Ask.** A warning line and a counter (`de_engine_leases_lost_total`) when a renewal finds another owner.

**Closed** (2026-10-04, the engine's F21): the test runs; its log assertion reads the engine's line `run lease lost mid-drive` (`engineLog`), or the worker's, which never logs it now.

### F9 — Past `maxWorkMs` the holder's late appends are refused, so a long step never completes

**Where.** `API.md` "The lease" ("gone: … `maxWorkMs` passed since the item was pulled … everything is `expired`"); `engine/ops.go` `leaseFor` (`!now.Before(h.workDeadline)` → `expired`); the library's `maxWorkMs` (stop extending, and a late append still lands under the CAS: "a stale ack after a takeover is harmless").

**What.** In the library, `maxWorkMs` only stopped the heartbeat. A step that ran past it could still journal its `step-end` if no one had moved the run on, and the run completed. In the engine, a lease past `maxWorkMs` is gone, and `append`, `load` and `done` are all `expired`. A step whose function takes longer than `maxWorkMs` can therefore never journal its result. Every redelivery re-runs it, runs past the bound again and is dropped, so the run never completes. The library's network control (`maxWorkMs: 300`, a 5.5 s step at 500 ms RTT) shows it: under the engine the run does not finish within 30 s. Library code that set `maxWorkMs` below a legitimate step's duration changes from "may be taken over" to "never finishes".

**Blocks.** `network` "…and the control: the same run with renewal cut short IS taken over" (`it.skip`), and its Python twin in `test_network`. Note: the engine checkout `../durable-engine` carried uncommitted changes on 2026-10-03 that read as a fix for this (a lease past `maxWorkMs` lapsed, not gone); the suites ran the `d8c0d73` binary, and `engine.pin` is not moved.

**Ask.** Past `maxWorkMs`, treat the lease as **lapsed** (the item redelivers, and the holder's requests still answer under the CAS), not gone. Or, if the hard bound is intended, document it in `API.md` as a bound on any one step's duration.

**Closed** (2026-10-04, the engine's F18): both controls run and pass; the `lease.gate {outcome: takeover}` assertion reads the engine's log (`taken over`: `run taken over past max work`, or `run lease taken over` across instances).

### F10 — Engine-side decisions are invisible to the worker

**What.** Several of the library's assertions read the worker's own `work.done` outcomes or its log for decisions the engine makes now, without a poll: a terminal re-drive, an item for a run that has already ended, a run with no journal, the maintenance pass. Those items never reach the worker. Seven such assertions and five waits are re-expressed through the engine's `info` counts (`de_engine_acks_total{outcome}`, `de_engine_items_pulled_total{workflow}`) or the engine's own log, each marked `durable-sdk:` in the suite. The engine's counters are per workflow and per instance, never per run, so a per-run claim becomes a per-workflow one. A per-run view (or the run in the ack log line) would make these exact.

### F11 — One engine instance serialises a run's items

**Where.** `API.md` "The lease" ("An instance hands a run's items one at a time: while one is held (live), the run's next items wait behind it").

**What.** The library's multi-worker suites (`matrix`, the `e2e` chaos test, `payload-codec`'s storm) start two workers to race one run on the journal CAS. Through one engine instance they never hold the same run at once; the race moves to the engine, and to its instances (its proofs 3 and 9). These suites still pass and still check the journal's sanity, but on one instance they no longer exercise the CAS race they were written for. Running them against two engine instances per prefix (`--no-lease`) would restore it. The harness has one instance per prefix.

### F12 — A `fail` terminal that cannot be journaled is a fence for ever

**Where.** The pure core, `src/core/ctx.ts` `appendTerminalFail` → `appendTerminalCAS` returns `"fail"` on a `SerializationError` → `reconcileTerminal()` finds no terminal → `"fenced"`.

**What.** When a workflow's failure message is too large to journal (F1), the core reports `fenced`, the engine naks, and every redelivery repeats it, so the run never ends and `result()` waits for ever. The library with `offload: false` has the same shape: there the worker finalised the status as `failed` with no terminal in the journal, and `result()` hung. With offload on, the library never reaches it. The core is frozen and copied unchanged; this is reported, not fixed.

**Ask (library).** When the `fail` entry itself cannot be encoded, journal a `fail` with a truncated message, and say it was truncated, rather than fencing.

### F13 — The suites' own JetStream client

**Where.** The brief: "`package.json` lists no `@nats-io/jetstream`, `kv` or `obj`" and, of the suites, "a read of the stream with the test's own JetStream client in the harness, which is fine in a test".

**What.** The two cannot both hold literally: the suites read and stage the streams (journal reads, KV hints, object listings, forged work items) with the real client libraries. They are **devDependencies** only. `dependencies` is `@nats-io/nats-core` alone, no file under `src/` imports the others, and `test/unit/call-roster.test.ts` asserts both. The staging uses the library's own shell code, copied under `test/harness/shell/` with its imports re-pointed: `journal-store`, `direct`, `blob-store`, `runs` and `names`, plus the one helper of `resources.ts`. A test that writes a journal by hand therefore writes exactly what a library worker would have left behind.

### F14 — No plaintext in flight

**What.** The library's client put a signal's plaintext on the work queue and its worker encrypted it on the way into the journal ("plaintext IN FLIGHT — by design", `payload-codec`). The SDK encrypts in the client (`dataEnc`), and the engine never holds the key, so nothing on `de.work.*` carries plaintext any more (see F6 for how it is stored). One assertion was turned around (`toContain(SECRET)` → `not.toContain(SECRET)`), marked in the suite.

### F15 — Python's unit suites

**Where.** The brief: "the pure unit tests (`test_replay`, `test_fanout`, `test_call_roster` rewritten as TypeScript's was, `test_version`, `test_registry`, `test_codec`, `test_payload_codec`, `test_payload`, `test_payload_paths`, `test_wire_vectors`, `test_sim_findings`, `test_store`, `test_journal_load`, `test_backoff` if pure, `test_token`, `test_connect`) … pass unchanged".

**What.** F4's three, and one more:

- `test_call_roster` (25 tests in the library) rosters every nats-py call under `_nats/` and `client.py`. It is rewritten as the SDK's roster (7 tests, the twin of TypeScript's): `pyproject.toml` depends on `nats-py` alone; no file under `src/` imports `nats.js` or calls `jetstream()`, `jsm()`, `key_value()` or `object_store()`; only `_engine/{client,transport,worker}.py` import `nats` (the client and the worker for a type only); the one call on the connection is `_engine/transport.py`'s `request` to `{prefix}.api.v1.<op>`; nothing publishes, subscribes, names a `$JS`, `$KV`, `$O`, journal, work or timer subject, or reaches a consumer. Adding `from nats.js import api` to `_engine/names.py` fails two of its tests (checked once on purpose).
- `test_payload_paths` names `_nats/runs.py` among the callers of `store_payload_blob`. One line is removed.
- `test_version` pins `VERSION` to `pyproject.toml`'s version: `version.py` says `0.7.0a0` (one line).
- `test_connect` has a third test, "the runbook's provisioning call is importable from the package root", which pins `Names`, `ProvisionOptions`, `provision`, `deprovision` and `resource_streams` in the exports the brief removes. It is dropped with a note; the other two pass unchanged.

Left behind: `test_journal_load` imports `_nats/journal_store.NatsJournal` and `_nats/names.Names` and tests the library's direct-get load and its completeness guard, which are the engine's now (TypeScript left `journal-load.test.ts` behind the same way); `test_names` tests `_nats/names.py`. `test_payload` is named in the brief but does not exist in the library's Python tree (`payload.test.ts` has no Python twin). Two pure suites the brief did not name are copied unchanged and pass: `test_workflow` and `test_blobs`. So 16 unit suites, 12 byte for byte.

### F16 — Python's cut

**Where.** The brief: "Leave behind `_nats/` and everything that creates a consumer, a stream or a bucket. The only NATS dependency is **`nats-py`** core".

**What.** As F3: three files of `_nats/` are pure and kept unchanged under their library paths, so their diff stays empty. `__init__.py` is the package marker. `connect.py` holds `with_reconnect_defaults` and imports nothing from nats-py. `protocol.py` holds `shrink_work_item`, which `test_payload_codec` imports, and imports only `core/work_item.py` and `journal/blobs.py`. Nothing of the shell's I/O is in `src/`.

nats-py is one distribution, and its JetStream client ships inside it, so "nats-py core alone" cannot be a dependency rule: it is the roster's (F15). The suites' own staging and reading of the streams is the library's shell, copied as `tests/shell/{journal_store,direct,blob_store,runs,names}.py` with the one import prefix re-pointed, plus `js_error_code` from `resources.py`, as F13 did for TypeScript.

### F17 — Two things nats-py does not do, which the SDK does for itself

**Where.** nats-py 2.15.0 `nats/aio/client.py`: reconnects are reported only through the `reconnected_cb` given to `connect()`; `_close` leaves `_resp_map` (the requests in flight) untouched. nats.js has `nc.status()` and rejects the requests in flight at close.

**What.** Both matter to the two behaviours TypeScript's suites pinned:

- `result()` re-asks after a reconnect (a reply lost to a disconnect). With no status stream to wait on and the callback the caller's, the client watches `nc.stats["reconnects"]` every 100 ms while a `result` request is in flight, and asks again when it moves. `test_network` "a result waiter rides out a cut" passes.
- A worker on a connection that closes for good must say so at once. A poll in flight would otherwise wait out its whole timeout (40 s), so `test_network` "a worker whose connection closes for good" failed. The transport now races every request against `nc.is_closed` (every 100 ms) and fails it as `ConnectionClosedError` when the connection closes first. The cost is one sleeping task per request in flight.

**Ask (nats-py).** Fail the pending request futures on close; and a status stream (or more than one reconnect listener) a library can subscribe to after `connect()`.

### F18 — The `durable_agent` example is left out

**What.** `examples/durable_agent/frontdoor.py` and `connection.py` import `synadia_ai.agent_service` and `synadia_ai.agents`, the fabric's agent SDK (the library's dev dependency `synadia-ai-agent-service`). The brief keeps it only "if it needs nothing of the fabric SDK". It is not copied, and the dev dependency and its lint entries are gone from `pyproject.toml`.

### F19 — `upgrade-compat`

**What.** Deleted (Mario, 2026-10-03: recent brokers only): `test/integration/upgrade-compat.test.ts` and the `NATS_SERVER_OLD` sentence of the README. The library's Python tree has no twin of it (its integration suites never spawned a second server binary), so there was nothing to delete in `python/`. The TypeScript integration suites are 9 of the library's plus `interop-py`.

### F20 — The library's Python integration suites that test the shell

**What.** The library has 21 Python integration suites. Thirteen are copied and run on the engine. Eight test the library's shell itself and are left behind, as TypeScript left their twins: `test_blob_direct` (3, the direct-get blob reader), `test_cost_budget` (1, the shell's JetStream API requests per run), `test_journal_direct` (4, the direct-get journal reader), `test_journal` (9, `NatsJournal` against the server), `test_lease` (9, `_nats/lease.py`), `test_provision` (13, `_nats/resources.py`), `test_quota` (7, the worker's consumer binding under the account cap) and `test_reconcile` (12, `_nats/parked.py` and the lost-wake repair). Each is the engine's now; its proofs are the engine's to keep.

Within the thirteen, five tests are dropped in place with a note, each a test of something the worker no longer does: `test_chaos` and `test_worker_recovery` "a fenced inbound event is retried, not lost" (both patched the worker's `NatsJournal.append`; recording inbound events is the engine's, as TypeScript dropped them), `test_metrics` "the hygiene pass reports its gauge and its sweep" and "offload traffic is visible" (`maintenance.*` and `blob.*` are the engine's), and `test_network` "an offloaded payload costs one round trip per chunk" (the library's blob writer). `test_network` "LEASE at RTT 500 ms / TTL 2 s" is kept as a skip for the diff, as TypeScript kept it.

### F21 — Where the Python SDK differs from the TypeScript one

- **`RunNotFoundError` is also a `LookupError`.** The library's Python `client.signal` and `client.cancel` raised `LookupError` for an unknown run; a caller's `except LookupError` still holds.
- **`stop(drain_timeout_s)` cancels.** Past the timeout the items still in flight are cancelled, as the library's Python `stop()` did; they send no `done`, so their leases lapse and they redeliver at the ack wait. A promise cannot be cancelled, so TypeScript's `stop()` only returns. `test_chaos` "workers killed mid-run" simulates a crash with `stop(drain_timeout_s=0)` and needs it.
- **`journal.load` and append sizes.** The pure core never calls `Journal.load` in an ordinary run, because the engine hands the journal over with the poll, so a worker built strictly to `API.md`'s seam table reports no `journal.load` at all and `bytes: 0` on each `journal.append`. `test_metrics` asserts both, so the Python worker emits one `journal.load` per item (the poll's journal and its pages) and the encoded entry's size. **Ask (TypeScript SDK):** do the same, so a dashboard reads one number from both.
- **A minted runId** is the engine's UUID (with hyphens). The library's Python client minted `uuid4().hex`.
- **`create_client` gains `logger`**, as TypeScript's has; `metrics`, `stream_max_bytes` and `offload` are accepted and ignored. `create_worker` gains `poll_wait_ms`; the options that became `serve` flags are accepted and ignored.

**Closed** (2026-10-04), the TypeScript ask above: the TypeScript worker emits `journal.load` once per item, its `bytes` the journal as the engine handed it, and `journal.append` with the size of the entry it sends, as Python's does; Python's `journal.load` gained the same `bytes`. `test/integration/metrics.test.ts` is the twin of `test_metrics`'s first test (both assert one load per item with `bytes > 0`); without the change it fails.

### F22 — The engine checkout carried someone else's uncommitted changes

**What.** On 2026-10-04 `../durable-engine` stood at `1efd2ec` with uncommitted changes to `engine/{drive,engine,metrics,ops}.go` and `internal/scriptworker/proof03_test.go` (another session's work, not this one's). The harness refuses a dirty checkout, as it should. Nothing there was touched: every run of this follow-up built the pin from the clean worktree `../wt/durable-engine` at `1efd2ec` (`DE_ENGINE_REPO=../wt/durable-engine`, the binary `.engine-bin/durable-engine-1efd2ec3bc33`), which is where the coordinator verifies too. During the 300 ms run that worktree was moved to `51b0cae` by someone else, and the Python half refused it in 1 s (88 setup errors, not test results); it was re-run, and every later run used the binary already built from the clean `1efd2ec` tree (`DE_ENGINE=.engine-bin/durable-engine-1efd2ec3bc33`). **For the coordinator:** `../wt/durable-engine` is no longer at the pin; verify with `DE_ENGINE` pointing at a build of `1efd2ec`, or check the worktree out at it first.

### F23 — A missing blob is parked by the engine, before the worker sees the item

**Where.** The `offload` suite, "a missing blob PARKS the run … and a restored blob heals it", which waits for the worker's `work.done` outcome `blob-unresolvable`; the engine's `view` (`engine/obligations.go`), which inflates the journal before it hands an item over.

**What.** The engine reads the run's journal, refs inflated, before a poll is handed the item. With the step's blob deleted, that read fails and the engine parks the run itself (`run's journal unreadable; parking it (restore the blob or cancel the run)`, naked with backoff): the item never reaches a worker. The worker's own `blob-unresolvable` path (a `get` that finds the blob gone, `BlobMissingError`) is still there and still labelled as the library labels it, but it is reachable only for a ref the engine hands over as too large for a reply, and only in the window between the engine's read and the worker's `get`. The wait is re-expressed as either the worker's outcome or the engine's log line, marked `durable-sdk:` (the F10 class). The heal half (restore the blob, the run completes) passes unchanged.

### F24 — `get` of a blob that is gone answers `internal`

**Where.** `API.md` `get` ("Errors: … `internal` (the blob is gone)"); `engine/blobs.go` `opGet` (`wire.BlobMissingError` → `toAPIError` → `internal`, the code of every operational fault).

**What.** The SDK has to tell a blob that is gone (park the run: `BlobMissingError`, restorable) from a transient fault (retry). The closed set has no code for the first, so both transports read `internal` from `get` as "gone", which is what `API.md` says it means for that op. Any other `internal` from `get` (a corrupt object, a store fault the engine classes operational) is therefore also read as "gone"; the run parks rather than retries, which heals the same way.

**Ask.** A distinct refusal for a blob that is not there: `not_found` on `get` (the code exists, today only for `signal` and `cancel`), or `internal` with a `missing: true` member.

**Closed** by the engine's F30 at `ee81460` (`blob_gone`, with the blob's name), mapped in F31; `internal` from `get` is no longer read as "gone".

### F25 — The chunked upload is a fourth payload-blob route

**Where.** `test/unit/payload-paths.test.ts` and `python/tests/test_payload_paths.py`, "every caller outside the choke point is a KNOWN payload route": a roster of the callers of `storePayloadBlob` / `store_payload_blob`, which says "whoever adds [a route] has to come and say what it is".

**What.** `fitSlot` / `fit_slot` (`src/engine/payload.ts`, `python/src/synadia_durable/_engine/payload.py`) is that route: a start's input, a signal's data or a child's input too large for one request is `put` through `storePayloadBlob`, so the codec decision stays the choke point's. Each roster gains one line naming it, marked `durable-sdk:`, as F4 and F15 edited the same suite. An appended entry goes through `offloadEntry`, which is already the choke point's.

### F26 — What the chunked transport decides

- **The library's code, the engine as its blob store.** `EngineBlobs` (`src/engine/blobs.ts`, `python/src/synadia_durable/_engine/blobs.py`) implements the library's `BlobStore`: `put(name, bytes)` uploads in parts of `(request_max − 4096) / 4 × 3` raw bytes under the run the name begins with and checks the ref the engine answers names the same blob; `get(name)` downloads in parts. `offloadEntry`, `storePayloadBlob` and `resolveEntry` (unchanged, `src/journal/blobs.ts`) do the rest: the ref's name, `v:2` or `v:3`, the digest check, the codec. So an offloaded entry is the library's bytes, and the pure core never sees a ref.
- **When.** A request whose body would pass `request_max` (from `info` once per client or worker; `max_payload` − 16 KiB until an engine answers): an `append` (its entry's slot), a `start` or a `signal` (`inputRef` / `dataRef`, `payloadEnc` over codec output), a `child` (the child's input, `put` under the **child's** runId). Anything that fits travels inline as before and the engine offloads it past its threshold.
- **A start without a runId** whose input is too large: the client mints the runId (a UUID, the engine's own form), since a `put` needs the run; a start that fits still lets the engine mint it.
- **The lease** goes with a part of the item's own run only; a child's input, under another run, carries none (the engine would answer `expired`).
- **`offload: false`** keeps the library's meaning: nothing goes up in parts, and a value too large for a request fails loud (`PayloadTooLargeError`), as `e2e` and `test_offload` "offload disabled still fails loud" pin. The option's tuning (`thresholdBytes`, `cacheBytes`, `maxBytes`) stays ignored: the threshold is `serve --offload-threshold`.
- **A cache.** The TypeScript worker keeps the library's 64 MiB content-addressed byte cache for refs it fetched; Python's `resolve_entry` takes none, as in the library.
- **Metrics.** `blob.put` and `blob.get` (`name`, `bytes`, `ms`, `outcome`), from the worker and, newly, from the client (`metrics` is no longer ignored by `createClient` / `create_client`), as the library's `offload` suite expects.
- **Abandoned uploads.** A `put` that stops part way leaves its staged parts in the blob store (the engine's F25 says so too); nothing collects them.

### F27 — `status` lags `result` by the worker's `done`

**Where.** `engine/ops.go` `finish` (the obligations, the `DE_RUNS` status hint among them, run at `done`); `result` (answers from the journal's terminal as soon as it is appended). The library's worker wrote the status hint before it acked, and its `result()` resolved on that hint, so a `status()` after `result()` always read the terminal.

**What.** Between the worker's terminal `append` and its `done` (one round trip of the worker's), `result` already answers the terminal while `status` still reads the state before it (`suspended`, `running`). With the worker next to the engine the window is a millisecond and no test sees it. Over the delay proxy (F28) four tests lost the race: at 150 ms TypeScript `interop-py` "a TypeScript cancel hard-stops a parked Python run" and Python `test_worker` "cancel hard-stops a parked python run" (`'suspended'` for `'cancelled'`), at 300 ms TypeScript `e2e` "status() reports 'suspended' … then 'completed'" and "CANCEL: a run parked on an await is cancelled". About 25 assertions in the suites have the same shape (`e2e`, `offload`, `versioning`, `matrix`, `payload-codec`, `worker-recovery`, `cancel-running`); they won the race in these runs. The four (and `interop-py`'s `failed` twin) now wait for the state, bounded at 10 s, marked `durable-sdk:`. A customer who calls `status()` after `result()` sees the same lag.

**Ask.** Make `status` read-your-terminal: answer the terminal's state from the journal when the hint is behind it (`status` could look at the run's last entry, as `result` does), or write the terminal hint when the terminal is appended rather than at `done`.

### F28 — The suites over the delay proxy, at 150 and 300 ms

**How.** `DE_TEST_RTT_MS=150`, then `300`: the library's delay proxy (`test/harness/proxy.ts`, `python/tests/netproxy.py`) between every test's connection and the server; each `durable-engine serve` on the direct URL, next to the server, as the library's harness arranged; the hygiene (leftovers, stream deletion) on the direct URL too. The suites otherwise unchanged, on the `ngs` account profile, against `1efd2ec`.

| Run                     | TypeScript integration                     | Python integration                                                          |
| ----------------------- | ------------------------------------------ | --------------------------------------------------------------------------- |
| direct (0 ms)           | 136 passed, 1 skipped, 289 s               | 88 passed, 1 skipped, 237 s                                                 |
| 150 ms                  | 134 passed, **2 failed**, 1 skipped, 572 s | 87 passed, **1 failed**, 1 skipped, 380 s                                   |
| 300 ms                  | 134 passed, **2 failed**, 1 skipped, 909 s | 88 passed, 1 skipped, 537 s (with the F27 wait; the first attempt is F22's) |
| 300 ms, after the fixes | 136 passed, 1 skipped, 911 s               | 88 passed, 1 skipped, 544 s                                                 |

**Every deviation:**

- **TypeScript `interop-py` "a TypeScript cancel hard-stops a parked Python run"** (150 ms): `expected 'suspended' to be 'cancelled'`, the `status()` read right after `result()` rejected. The engine's logic, F27; the test now waits for the state, bounded (and its `failed` twin likewise).
- **Python `test_worker` "cancel hard-stops a parked python run"** (150 ms): the same, F27, the same wait.
- **TypeScript `e2e` "status() reports 'suspended' … then 'completed'" and "CANCEL: a run parked on an await is cancelled"** (300 ms): the same race, `expected 'suspended' to be 'completed'` / `'cancelled'`; they had won it at 150 ms. F27; bounded waits.
- **TypeScript `offload` "a missing blob PARKS the run … and a restored blob heals it"** (150 ms): `Test timed out in 30000ms`. A timing assumption in the test: the park backs off (the engine naks at 0.25 s doubling), and the 2 × `max_payload` value crosses the proxy twice; fixed by the library's own allowance (`30 s + 1 s per ms of RTT`, as its big-child test). At 300 ms it then passed in 72.6 s.
- Nothing flaked otherwise, and no failure was the SDK's: the chunked transport, `cancelPoll` and the metrics held at both latencies.

**What grows.** The TypeScript suite takes 2.0× at 150 ms and 3.1× at 300 ms, Python 1.6× and 2.3×. The slowest at 300 ms: the missing-blob park (72.6 s), "a run touching EVERY payload path" (50.1 s; 31.4 s at 150), the `network` control (30.6 s), the SIGKILL storm (22.5 s), the large child outcome and the big child input/result (15.8 s, 15.5 s): the offload tests, whose `put`/`get` parts are dependent round trips, and the tests that wait on a redelivery.

**What the harness would need for the dev NGS, from a laptop** (not built):

- **An existing server by URL and creds** (`DE_TEST_NATS_URL`, `DE_TEST_NATS_CREDS`) instead of the spawned `nats-server`, in both `global-setup.ts` / `conftest.py`; no account profile to apply (NGS's own limits apply, and the sizes already derive from the server's `max_payload`). `natsDirectUrl` and `natsUrl` become the same URL, and `DE_TEST_RTT_MS` stays 0: the latency is real.
- **The engine is no longer next to the server.** Here the engine sat on the direct URL; from a laptop it would pay the WAN round trip on every JetStream call, which this dry run did not measure. Either run the engine on the laptop and accept that (then every engine-side wait grows too), or the coordinator places an engine near NGS; the suites only need its prefix.
- **One prefix for the run, not one per test** (about 86 distinct prefixes in the TypeScript suites alone, each seven resources): a run prefix like `de-sdk-<runid>`, with each test's own name below it where the suites need separation, and one engine for it, started once rather than per test (a `serve` provisions at start: seconds over the WAN, per test).
- **The hygiene scoped to that prefix.** `setup-each.ts` deletes **every stream on the server** after each test, and Python's twin likewise: against a shared account that would delete everyone's. It must delete the run prefix's resources only, and the leftover-consumer check must look only at that prefix's streams, still after every test so a leak is named by the test that made it.
- **The tests serial** (TypeScript `fileParallelism: false` already, Python serial already), and timeouts that scale with the measured RTT, as `offload` does now; `testTimeout` is a flat 30 s.
- **Skipped against NGS:** what needs a server of its own or destroys infrastructure: `network`'s SIGKILL and restart of the server, its connection cuts and stalls (the proxy), `worker-recovery`'s deleted `DE_TIMERS` and its engine restarts, every test that stages journals or work items with the suites' own JetStream client unless the creds allow it, and the profile-dependent size tests if NGS's `max_payload` differs from the `ngs` profile's.
- **F27 first**, or the ~25 `status()`-after-`result()` assertions will race at NGS round trips as two did here.

### F29 — The harness's mode for an existing server

**What.** F28's sketch, built in both languages (`test/harness/existing.ts`, `run-engine.ts`, `engine-process.ts`, `skips.ts`; `python/tests/existing.py`, `engine.py`'s `RunEngine`; both `conftest.py`). `DE_NATS_URL` switches it on; `DE_NATS_CREDS`, `DE_PREFIX`, `DE_ENGINE_URL` and `DE_CLEANUP` are the README's. What it decides, beyond the brief's list:

- **Every prefix a test names is the run's.** The suites name about 90 prefixes; the harness maps each, at its own boundary (`createWorker`/`createClient`, `names()`/`Names`, `engineCounts`, the `prefix` fixture, the interop children's `DE_PREFIX`), so no test body changed. Tests are serial, and the purge after each one means a fixed runId (`ghost-no-journal`) never meets a predecessor.
- **One engine, restarted for flags.** The run's engine is the main process's (vitest's per-file workers reach it over a 127.0.0.1 control channel; pytest holds it in the session). A test's first worker or client fixes its flags, as it fixed a spawned engine's; when they differ from the running engine's it is restarted with them (the engine reconciles the durables' ack wait at bind). `streamMaxBytes` is dropped: a test does not resize a prefix it does not own. In the plain TypeScript run the engine started 15 times for 119 tests, Python 18 for 71. Its counters are the test's own (`engineCounts` subtracts what the engine had counted when the test began, or starts at zero if it was restarted since), and so is its log (`engineLog`: the lines since the test began).
- **The purge is a diff.** Before each test the harness snapshots each of the seven streams' subjects and counts (`subjects_filter: ">"`); after it purges every subject that is new or grew, pipelined. A subject that was there and did not change is left alone, so a prefix shared with another client (`de` on tenant-a-dev, console-de's) keeps the other client's runs. A subject the run did grow is purged whole (a work subject `de.work.<wf>` shared with another client's workflow of the same name would lose that client's items too; the suites' names end in `_e2e`, `_wf` and the like).
- **The test's durables are deleted.** The engine makes two durables per workflow name (`de-worker-<wf>`, `de-timer-<wf>`), and the suites use about 150 names: undeleted, they pass an account's per-stream consumer cap (250 on the `ngs` profile) within one TypeScript run. The harness deletes the durables that were not there before the test; a later test reusing the name gets them back at its first poll (the engine re-binds after its 0.5 s backoff).
- **The leftovers check counts what the test made**: consumers without a durable name on the prefix's streams, less those there before the test (before the run, at the end), so another client's KV watch on a shared prefix is not this run's leak.
- **The engine runs where the harness runs.** Under `DE_TEST_RTT_MS` the run's engine connects through the proxy too, so every round trip in the run is the proxy's, the shape of a laptop against NGS. F28's runs had the engine on the direct URL; this is slower and closer to the real thing.
- **The factor** is 1 up to 20 ms, then 1 + RTT/100 of the median of five round trips to where the tests connect, printed once; 2.52 at the proxy's 150 ms. It scales the default test and hook timeouts, a handle's `result()`, the engine's start, the harness's own waits, and the suites' own wait helpers (TypeScript `waitUntil` and `waitForState` in eight files; Python `_wait_until` and `_wait_for_state` in three). A test's own timeout is fixed by vitest when it collects the test, out of the harness's reach, so the 35 tests that give one now give `N * WAN` (`test/harness/mode.ts`), each under a `// prettier-ignore` that keeps prettier from re-indenting its body. F28's offload allowance already reads `natsRttMs`, which is the measured RTT in the mode.
- **Creds** reach the clients (`connect` in `existing.ts`, `existing.connect`), the engine (`--creds`) and the interop children (`NATS_CREDS`, which `conformance/interop/ts-worker.ts`, `ts-client.ts` and `python/conformance/worker.py` now read). Proven against a local server in operator mode with a throwaway `nsc` operator and user, which refuses an unauthenticated connection: TypeScript e2e, offload, metrics and interop-py 71 passed but for interop-py's 10, which failed on F30 and then passed; Python interop, interop-reverse and worker 34 passed.

**What the 150 ms run changed in the tests** (each marked `durable-sdk:`):

- TypeScript `e2e` "a work item for a run with NO journal …" read the work durable right after `worker.start()`. On `57ae3c9` the engine binds a workflow's durables when the first poll for it arrives (pull on demand), which can come after `start()` returns, so the read could answer `consumer not found`. It failed so in the spawned run at 0 ms too, once; absent now counts as not yet drained.
- TypeScript `interop-py` "runs a Python workflow and reads its result and status" lost F27's race (`'running'` for `'completed'`) and waits for the state, bounded, as its twins have since F28.
- Python `test_stop_drains_the_work_it_already_dispatched` and `test_health_reports_what_the_worker_is_doing` slept 0.4 s and 0.5 s for "the step is executing now"; with every round trip 150 ms the item had not arrived. They wait for the step to start and for the item to be held.
- TypeScript `working` and Python `test_working` "ESCAPE HATCH across WORKERS" (the takeover) ran out of their own 40 s and 25 s; the factor now reaches both.
- Python `test_interop` `test_status_reports_the_terminal_state` lost F27's race in the next run (`'running'` for `'completed'`) and waits for the state too. F27 is the engine's, and timing: each run at 150 ms lost it in a different place (TypeScript `interop-py` in the first, Python `test_interop` in the second). The NGS run may meet it in another of the ~25 assertions of that shape; such a failure is F27, not the mode.

The first 150 ms run, before these: TypeScript 116 passed, 3 failed; Python 68 passed, 3 failed. The second: TypeScript 119 passed; Python 70 passed, 1 failed (`test_status_reports_the_terminal_state`).

**`DE_CLEANUP` stays opt-in, as the brief has it.** With a generated prefix every run leaves seven streams reserving 1 GiB each; after about ten runs the local server's store was full and the next run's engine refused to start (`the account's storage quota refused it (1073741824 bytes asked; JetStream 10047)`), which the harness reported as the run's failure in the engine's own words, the behaviour the brief asks for on a tier too small. Local runs pass `DE_CLEANUP=delete` (the README says so). Making it the default for a generated prefix, which only the run can have made, would be the obvious next step.

**The skips, by name** (each title or reason says "existing server (DE_NATS_URL)" and why):

- TypeScript `network.test.ts`, all of it but its already-skipped "LEASE at RTT 500 ms", 16: "the proxy itself" (5), "latency" (3 and the lease control), "disconnects" (5), "a connection that closes for good" (1): a proxy of their own between them and the server. They pass against a plain local server, but a server the run does not own can be TLS, which a byte proxy cannot carry, and their cuts are what the brief excludes. "a broker crash mid-storm" (1): it kills a server of its own.
- TypeScript `worker-recovery.test.ts` "the maintenance pass and the pumps survive DE_TIMERS disappearing, and heal on restart": it deletes one of the prefix's streams.
- Python `test_network.py`, its 16 that run (the twins; `test_sigkill_and_restart_under_a_two_worker_signal_storm` as a server of its own), and `test_worker_recovery.py::test_the_maintenance_pass_and_pumps_survive_a_deleted_timers_stream`.
- The account-limits profile assertions: none of this repo's suites asserts on the profile (the library's limits suite was not copied), so none is skipped for it; in the mode no profile is applied.

**The runs** (engine `57ae3c9`, a plain `nats-server -js` 2.14.6 on 127.0.0.1:14222 started by hand and given by URL):

| Run                                    | TypeScript integration         | Python integration                                                             |
| -------------------------------------- | ------------------------------ | ------------------------------------------------------------------------------ |
| spawned (as before)                    | 136 passed, 1 skipped, 309 s   | 88 passed, 1 skipped (the whole suite 468 passed, 1 skipped, 2 xfailed), 234 s |
| the mode, plain (RTT 0.0 ms, × 1)      | 119 passed, 18 skipped, 72.8 s | 71 passed, 18 skipped, 71.8 s                                                  |
| the mode, the proxy at 150 ms (× 2.53) | 119 passed, 18 skipped, 1295 s | 71 passed, 18 skipped, 738 s                                                   |

The counts are the spawned ones less the 17 named skips in each language (each language's 18th skip is the "LEASE at RTT 500 ms" test dropped since F9, inside a skipped module or suite in the mode). The mode's runs went against a fresh server with `DE_CLEANUP=delete`: each left the prefix's seven empty (no messages, no consumers) before deleting them, and the server held no stream afterwards, so nothing was written outside the prefix. The time grows with the round trip about linearly: TypeScript ~8 s and Python ~4.4 s for each ms of RTT, as every round trip of the engine's is paid too (F28's 150 ms run, with the engine on the direct URL, took 572 s and 380 s).

**For the NGS run.** F27's `status()`-after-`result()` race is still the engine's (about 25 assertions; five wait for the state since F28), and every round trip of the engine's now crosses the WAN as well. tenant-a-dev's `de` is console-de's too: the run leaves its subjects and durables alone (above), but its own engine and the tests share the prefix's API subjects with whatever else serves there, so the run is best done while nothing else drives `de`.

### F30 — nats-py reads a creds file only with `nkeys` installed

**What.** `nats.connect(url, user_credentials=…)` imports `nkeys` to sign the server's nonce. `nats-py` does not require it (it is the `nats-py[nkeys]` extra), so a client of the Python SDK on NGS, or any server that wants creds, fails with `ModuleNotFoundError: No module named 'nkeys'`. The harness's mode found it at the first creds run: every Python test errored at setup, and the TypeScript interop suite's Python worker died before `READY`. `nkeys` is now a dev dependency (`python/pyproject.toml`). The SDK still declares `nats-py` alone; `python/README.md` says to install `nats-py[nkeys]` for creds. A `creds` extra on `synadia-durable` would make it one word; that is the SDK's choice, not made here.

### F31 — `blob_gone` is the library's missing-blob error

**Where.** `src/engine/transport.ts` `blobGone`, `python/src/synadia_durable/_engine/transport.py` `_blob_gone`; the blob stores' `get`; the workers' park; the engine's F30 and `docs/API.md` "Errors".

**What.** On `ee81460` the engine answers a blob that is gone, or whose bytes are not its digest's, with `blob_gone` and its `blob` name, from `get`, `load`, `result` and (rarely) `poll`. At `main` both transports read the unknown code as `unavailable`: transient, retried. Now:

- **The mapping.** `blob_gone` becomes the library's `BlobMissingError` (`runId` the blob's run, `seq` -1, `reason: "absent"`), or `BlobIntegrityError` when the engine's message names a digest (`digest mismatch: …`, `… not the digest its name carries`); the engine's two messages are its `wire.BlobMissingError` and `wire.BlobIntegrityError`. A missing blob is the default when the message says neither.
- **Inside an execution** the worker parks the run as the library did: `blob-unresolvable`, the library's loud line, `done retry`, never a terminal. That held for an error thrown out of the replay already; it now also holds when the pure core halts with `retry` on one (a step-end `load` that meets a gone blob is an infrastructure error to the core, as in the library). A `get` inside the execution answers `null` to `resolveEntry` (TypeScript) or raises (Python) as before, so the library's own message names the entry.
- **A `poll` that answers `blob_gone`** (the engine parked the item's run at the hand-off) is logged loud and polled again at once, not counted as a failed poll.
- **A client's `result`** raises it (the engine's "outside, report it").
- **`internal`** from `get` is no longer read as "gone" (F24): it is the operator's, transient, as on every other op.

**The tests.** The acceptance the brief names, the library's missing-blob test, is TypeScript `offload` "a missing blob PARKS the run (loud, never a terminal) and a restored blob heals it" (the library's Python has no integration twin; its `test_blobs` unit tests are the pure core's and unchanged, 4 of them on a missing blob). It no longer reads the engine's log: it waits for the worker's `blob-unresolvable` or the engine's `de_engine_acks_total{outcome="parked"}`, which `info` counts and any client can read. **The worker's outcome is not reachable in it**, F23 still: the engine inflates the journal before it hands an item over, so the item of a run whose blob is gone never reaches a worker (run with `DE_SDK_LOG=1`, the worker logs nothing; the count is what passes it). New, in both languages:

- `offload` "blob_gone: a gone blob is BlobMissingError and a corrupt one BlobIntegrityError, at get and at result, and a restored one heals" and Python `test_blob_gone_is_blob_missing_or_integrity_at_get_and_at_result_and_heals`: `get` of a name never put and of a forged object, through the transport and the blob store; then `result` of a run whose terminal is a ref (`serve --offload-threshold 1024`, a harness flag TypeScript gains here as Python had it) with its blob deleted (`BlobMissingError` naming it), corrupt (`BlobIntegrityError`), and restored (the value). The engine caches a blob it read when the terminal landed (its status hint inflates it, F29), so the test restarts the engine after deleting it.
- Python `test_a_missing_blob_parks_the_run_and_a_restored_blob_heals_it`, the twin of the TypeScript park test.
- `test/unit/engine-transport.test.ts` and `python/tests/test_engine_transport.py`: the mapping, both messages, the blob store's `null` / raise, `internal` no longer "gone".

### F32 — nats-py's object delete is unreadable to nats.go

**Where.** `nats.js.object_store.ObjectStore.delete` (nats-py 2.15.0); the engine's object store reader (nats.go).

**What.** nats-py marks a deleted object with a meta record whose `mtime` is `""`. nats.go cannot parse that as a time and answers `nats: object-store meta information invalid`, which the engine classes as transient: a blob deleted with nats-py is read as `unavailable`, retried, never `blob_gone`; the run is not parked, and its work item redelivers with the engine's backoff. Found writing F31's Python park test: the engine answered `result` with `unavailable: … meta information invalid` for a blob the test had deleted. A blob deleted with nats.js or the `nats` CLI is read as gone. The Python tests delete an object by purging its meta and chunk subjects (`_delete_object` in `test_offload.py`). The engine's to judge (an unreadable meta might be `blob_gone` too); nats-py's to fix (an empty `mtime`); neither is this repo's.

**Handed on** (2026-10-04, F39): to the engine as an ask from here, for its next brief (the coordinator carries it): read a meta whose `mtime` is empty as a deleted object, so a blob nats-py deleted is `blob_gone`, not `unavailable`.

### F33 — The dev NGS run of 2026-10-04, and what the harness and the transport take from it

The coordinator ran the existing-server mode (F29) against `tenant-a-dev` at 51 ms, the engine `57ae3c9` on the laptop: TypeScript 109 passed, 10 failed, 18 skipped in 28.4 min; Python 66 passed, 5 failed, 18 skipped in 15.6 min. Large values over the WAN were the engine's (its brief `2026-10-04-durable-engine-wan-findings.md`), as was Python `test_six_runs_of_a_slow_step_finish_in_parallel` (5.4 s for 3 s; its threshold stays). The rest:

**The `put` timeout.** `put: the engine did not answer in time` on the last part: every op that does not wait had the flat 10 s margin, and the last part is answered only once the engine has read every staged part back from the blob store, checked the digest, written the blob and deleted the parts, each a run of 128 KiB object-store chunks, one acked publish each. On NGS (`max_payload` 8 MiB) the values were 12 MiB, 96 chunks per pass. Now each part's timeout is `putTimeoutMs(stored, rtt)` / `put_timeout_ms`: the 10 s margin, plus 4 round trips per 128 KiB chunk of what the engine moves for the part, plus 1 s per MiB of it (a floor on the store's throughput); `stored` is the part's own bytes, and on the last part also the whole value twice. The round trip is `nc.rtt()`, measured once per upload. On NGS a 12 MiB value goes up as two parts of 6,276,096 bytes and a last of 30,720; the last part's wait is 73 s at 51 ms (34 s at 0 ms), each of the others' 26 s (16 s). The engine's `docs/API.md` `put` does not yet say what the last part's reply waits for; this follows its implementation (`engine/blobs.go` `assemble`). A `put` the engine does not answer even then is `PayloadTooLargeError` ("the blob store is too slow for a value this size"), and one it refuses as `invalid` a `SerializationError`: deterministic for the value, so the pure core fails the run (its `SerializationError` path) instead of a redelivery that re-runs the step and puts the same value again, for ever; a client's call raises it. No responders, the engine's own `unavailable` and `expired` keep their meaning. Unit tests in both languages pin the formula, the timeouts sent part by part, and the mapping. Not run against the WAN from here.

**Run-scoped assertions.** "small runs never touch the blob store" counted every blob in the bucket and found two that earlier failed tests had left on the shared prefix; Python's `test_a_big_result_does_not_wedge_the_status_hint` found 3 blobs for 1 and `test_the_same_value_in_two_runs_is_stored_per_run` 4 for 2, the TypeScript run's leftovers. In the mode an assertion over a store's contents now counts the test's own runs: the blobs whose names begin with its run ids (`offload` "small runs", both Python tests through `_object_names(…, *run_ids)`), and in `payload-codec`'s sweep and its control the journal, timer and KV messages whose subject or body names the run (its children and generations begin with its id). Against a spawned server the prefix is the test's own and the assertions count the whole store, as the library's did. The other store reads were already per run (`listObjects(…).filter(runId)`, Python `_run_blobs`).

**Cleanup after a failed or timed-out test.** The run left 17 blobs (1.7 MiB) in `OBJ_DE_BLOBS` and `de-worker-resume_big`, `de-timer-resume_big`. The engine's log has the shape: from 08:03:49 to 08:09:22 the engine re-bound `resume_big`'s two durables after every test's cleanup deleted them (`pull failed; re-binding … consumer deleted`), ten times. `offload` "crash mid-run" had failed before its `w1.stop()`, so `w1` kept polling for `resume_big` for the rest of the file, and its item, retried, put its value again and again: a `put` that stops part way leaves its staged parts (`{runId}/upload/<id>/<n>`, F26), which no diff of a later test owned. Now, after every test in the mode, failed and timed out included (vitest's `afterEach` and pytest's fixture teardown run after both; everything after the leftover check is in its `finally`):

- the workers the test started and did not stop are stopped, bounded at 5 s × the factor (`endTestWorkers` / `end_test_workers`, the harness's `createWorker` keeping count);
- every waiting poll for each workflow set the test's workers named is withdrawn (`poll {cancelPoll, workflows}`), before the durables are deleted, so the engine holds no pending fetch that re-creates them;
- the test's journal subjects give its run ids, and every blob of every run id the file (Python: the session) has touched is deleted by name, meta and chunks (`deleteRunBlobs` / `delete_run_blobs`), staged parts included: a blob a straggler writes after its test's cleanup goes at the next one;
- at the run's end, with the engine stopped (nothing re-creates a durable or writes a blob then), the blobs of every run the run touched, and every durable on the work and timers streams that was not there when the run began (`sweepRun` / `sweep_run`). The engine's `/metrics` is read before it stops, and the report written after the sweep.

The engine will stop re-creating a durable it is pulling when it is deleted (its brief); until then, the `cancelPoll` comes first.

**Proven against a local server** (`nats-server -js` 2.14.6 on 127.0.0.1:14322, by hand) with `DE_INJECT_FAILURE=1`, which runs `test/integration/injected-failure.test.ts` and `python/tests/integration/test_injected_failure.py`: a test that throws with its worker running and putting 1.5 × `max_payload` values, one that times out (TypeScript: the test's 3 s; Python: `result()`'s) while its worker still puts them, and (TypeScript) a passing test after them. Under `DE_PREFIX` (kept at the end): TypeScript 120 passed, 2 failed (the two), 18 skipped, in 88 s; Python 73 passed, 2 failed, 18 skipped, in 79 s; each prefix's seven afterwards held **0 messages, 0 bytes, 0 consumers**, and the end's sweep found nothing left to delete (0 blobs of 180 and 113 runs, 0 durables). Through the delay proxy at 150 ms the injected file alone left the same: nothing. **What this does not prove:** the old harness, at `main`, run the same way on the same injected file, left nothing either, at 0 and at 150 ms. The NGS leftovers need minutes of a stray worker over a WAN round trip, which this machine did not reproduce; the new cleanup removes that worker, its polls, its runs' blobs by name and the run's durables by construction, and the runs above show it leaves nothing.

**The estimate.** The README's "84 s + 8 s per ms" (F29) predicted 8 min at 51 ms; the run took 28. It was fitted to the proxy, where every round trip costs the same; on the WAN the offload suites dominate, as each value's object-store chunks are dependent round trips of the engine's and its bytes cross the laptop's uplink. TypeScript `offload` and `payload-codec` took 9.7 s of the plain run's 76 s, and 743 s of NGS's 1,704 s (`e2e` 463 s). The README now has the measured table instead (the proxy run is this branch's, generated prefix, 120 passed and 21 skipped, 73 passed and 20 skipped; the NGS run is the coordinator's; Python's per-file time comes from `--durations=0`, not taken at NGS):

| Run                                                        | Round trip | TypeScript (of it `offload` + `payload-codec`) | Python (of it `test_offload` + `test_payload_codec_e2e`) |
| ---------------------------------------------------------- | ---------- | ---------------------------------------------- | -------------------------------------------------------- |
| plain, a local server (`ee81460`, this branch)             | 0.1 ms     | 76 s (9.7 s)                                   | 80 s                                                     |
| the delay proxy, a local server (`ee81460`)                | 155 ms     | 22.8 min (8.2 min)                             | 12.6 min (2.7 min)                                       |
| dev NGS, `tenant-a-dev`, from a laptop (`57ae3c9`, `main`) | 51 ms      | 28.4 min (12.4 min), 10 failed                 | 15.6 min, 5 failed                                       |

### F34 — A generated prefix is deleted at the end by default

**Where.** `test/harness/existing.ts` `existingFromEnv`, `python/tests/existing.py` `_from_env`; F29's note.

**What.** F29 kept `DE_CLEANUP=delete` opt-in and noted that a generated prefix, which only the run can have made, should be deleted by default: after about ten local runs the store was full. Now `DE_CLEANUP` unset deletes a generated prefix's seven at the run's end and keeps a `DE_PREFIX` one; `DE_CLEANUP=keep` keeps a generated one, `DE_CLEANUP=delete` deletes a given one. The line at the end says which (`DE_CLEANUP=delete (the default for a generated prefix): deleted …`). Proven: TypeScript (120 passed, 21 skipped, 76 s) and Python (73 passed, 20 skipped, 80 s) against the local server with no `DE_PREFIX` and no `DE_CLEANUP`: the server held 42 streams before each run and 42 after (the six given prefixes of F33's runs, kept by design), so each generated prefix left **0 streams**.

### F35 — The engine's F29 and F31 at this pin

- **`status` after `result`** (the engine's F29; this repo's F27). The five waits F28 and F29 added (TypeScript `e2e` 2, `interop-py` 2; Python `test_interop`, `test_worker`) stay, harmless; each now says the engine guarantees the state since `ee81460`, so a failure there is the engine's again.
- **A refusal is an `error` member with a string `code`** (the engine's F31: a failed run's `result` and `status` carry its `{name, message}` in `error`). Both transports already checked for the string `code` (`isRefusal`; `isinstance(refusal.get("code"), str)`), as the Go client now does. A test each: a failed run's `result` and `status` replies are answered, not raised, and an `error` with a code is the refusal (`test/unit/engine-transport.test.ts`, `python/tests/test_engine_transport.py`).

### F36 — `engine.pin` is the tenancy merge, `d3e741d`

`engine.pin` moves from `ee81460` to `d3e741d`, the engine's PR #7 (tenancy, P2-20). Standalone is unchanged on the wire, and nothing in `src/` or `python/src/` changed. On it, at this branch's head, against a local `nats-server` 2.14.6: spawned, TypeScript **137 passed, 4 skipped** (288 s) and Python **484 passed, 3 skipped, 2 xfailed** (233 s); in the existing-server mode (a hand-started server, a generated prefix, the harness's engine), TypeScript **120 passed, 21 skipped** (70 s) and Python **73 passed, 20 skipped** (56 s). These are the coordinator's counts for `main` on the tenancy binary. The binary was `.engine-bin/durable-engine-d3e741d57c99` (`go version -m`: `vcs.revision=d3e741d…`, `vcs.modified=false`), passed as `DE_ENGINE`: midway through this session someone else's uncommitted changes appeared in `../durable-engine` (`api/api.go`, `go.mod`, `store/direct.go`, from 22:14; the binary is from 22:09, clean). The harness refused that checkout as it should (F22), and it was left as found.

### F37 — The large-value budgets learn bandwidth

**Where.** `test/harness/existing.ts` `measureThroughput`, `test/harness/budget.ts`, `test/harness/mode.ts` `moving` / `budget`, `global-setup.ts`; `python/tests/existing.py` `measure_throughput`, `moving`; the tally in `test/harness/engine.ts` and `setup-each.ts`; `offload.test.ts`, `payload-codec.test.ts`, Python `test_payload_codec_e2e.py`.

**What the NGS rerun showed.** The coordinator ran `offload` alone against the dev NGS on 2026-10-04 at 51 ms, with the engine's blob path fixed: 10 of 11 passed in 7 minutes. "a big continue-as-new INPUT crosses generations as refs" hit its 44.7 s budget, while "a big child INPUT and big child RESULT flow as refs" moved its bytes in 80.9 s inside its own (that test alone had a per-ms-of-RTT term). The budgets scaled with the round trip only, and the large-value tests' time there is their bytes.

**The probe.** In the existing-server mode, once at the start, 4 MiB of random bytes go up through the run's engine's `put` and back down through its `get`, on the tests' own link (the proxy's under `DE_TEST_RTT_MS`), as a run `probe-<hex>` whose blob is deleted at once and again at the end. Throughput = 8 MiB over the two. It is printed next to the round trip: `round trip 0.0 ms, throughput 60.55 MiB/s (a 4.00 MiB put 99 ms and get 33 ms through the engine), max_payload 1.00 MiB → …`. Locally it read 59–72 MiB/s in TypeScript and 115–142 MiB/s in Python (8 MiB `max_payload` on the tenancy server, 1 MiB on the plain one).

**The budget.** A test that moves values over the inline limit declares n, the `max_payload`s it moves through `put` and `get` in all, and adds `moving(n)` = n × `max_payload` / throughput × 2 to its `result()` waits. Its own timeout is `budget(ms, n)` = (ms + `moving(n)`) × the round-trip factor (vitest fixes a test's timeout at collection, past the harness's reach). n comes from a tally, not from reading the code: the harness's `createClient` / `createWorker` count every `blob.put` / `blob.get` byte of a test and write `test, up, down, max_payload` to `.de-run/<prefix>/blob-bytes.tsv` in the mode. The test's own bucket reads and writes are added by hand, where it has any. Measured, in `max_payload`s:

| Test                                               | Up + down (tally)                             | n      |
| -------------------------------------------------- | --------------------------------------------- | ------ |
| `offload` multi-MiB input and step values          | 3.5 + 1.5                                     | 5      |
| `offload` crash mid-run                            | 2 + 2                                         | 4      |
| `offload` a missing blob parks the run             | 2 + 2, and the test's own read and write of 2 | 8      |
| `offload` a multi-MiB SIGNAL                       | 2 + 2                                         | 4      |
| `offload` a big child INPUT and RESULT             | 7 + 5                                         | 12     |
| `offload` a big continue-as-new INPUT              | 3 + 4.5                                       | 8      |
| `offload` a multi-MiB failure MESSAGE              | 1.5 + 1.5                                     | 3      |
| `offload` a multi-MiB RESULT                       | 4 + 2                                         | 6      |
| `offload` cancelling a run with offloaded entries  | 1.5 + 1.5                                     | 3      |
| `payload-codec` a LARGE signal's BLOB, its CONTROL | 2 + 2, and the test's read                    | 6 each |
| `payload-codec` a LARGE child outcome              | 4 + 2, and the test's read                    | 8      |
| `payload-codec` the completeness sweep             | 9 + 9, and the test's reads                   | 24     |
| `payload-codec` the sweep's CONTROL                | 3 + 1.5, and the test's reads                 | 8      |

Python's `test_offload` values are a few KiB under a 1 KiB `--offload-threshold`, none over the inline limit. Its two large-signal `test_payload_codec_e2e` cases (2 × `max_payload`) take `existing.moving(6)`. Spawned, the throughput is not measured and the term is 0, so those budgets are as before.

**At the dev NGS's numbers** (from the rerun, not run from here: that run predates the probe). The big child test moved 12 × 8 MiB = 96 MiB in 80.9 s, so ≈ 1.19 MiB/s is what a probe would see at most. The whole suite moved 48.5 × 8 MiB = 388 MiB in 7 min, ≈ 0.92 MiB/s with everything else in it. The continue-as-new test's 60 MiB needed ≈ 51 s at the first rate and had 44.7 s, which is why it failed. With the factor 1.51 at 51 ms:

| Throughput | `moving(8)` | its test's budget, (30 s + `moving(8)`) × 1.51 | its `result()` wait | the big child test's budget       |
| ---------- | ----------- | ---------------------------------------------- | ------------------- | --------------------------------- |
| 1.19 MiB/s | 108 s       | **208 s** (was 45 s)                           | 201 s (was 38 s)    | 367 s (was 81 s, by its RTT term) |
| 0.92 MiB/s | 139 s       | 255 s                                          | 247 s               | 436 s                             |

So about four times what it needed at the observed rate. The headroom 2 is because the probe and the tests take the same path through the engine, but a test's values also cross the engine's own hops to the store more than once (it inflates what fits a reply). A budget only matters when a test is slow: a passing test takes its own time.

**The engine's position.** On the laptop the engine's own hop to the store is the WAN as well: every 128 KiB chunk it writes or reads crosses it, a dependent round trip each, so the bytes cross the uplink about twice per upload and the round trips add up per chunk. An engine next to the broker has a short hop, and the same suite's bytes cross the WAN once. The probe measures whichever it is, since it goes through the run's engine. The README's table now has the rerun's row.

### F38 — The harness as a tenant: an admin connection, and what a tenant cannot see

**Where.** `test/harness/existing.ts` (`Scope`, `scopeOf`, `connectAdmin`, `userInfo`, the scoped `snapshot` / `purgeSince` / `runIdsSince` / `deleteRunBlobs` / `sweepRun` / `prefixEphemeralConsumers`), `setup-each.ts`, `global-setup.ts`, `shell/names.ts` `skipAsTenant`; `python/tests/existing.py`, `integration/conftest.py`, `shell/names.py`; `conformance/interop/ts-worker.ts`, `ts-client.ts`.

**What.** In the engine's tenancy mode (its PR #7, `docs/API.md` "Tenancy") the seven live in the engine's account, `DE`, shared by every tenant, and a tenant's SDK reaches the engine through a service import (`de.api.>` → `de.A.api.>`). As tenant A the tests' connection has no JetStream at all (`10039 JetStream not enabled for account`), so the leftovers check, the purge between tests and the end's sweep cannot run on it. `DE_TENANT=<token>` turns the scope on, and `DE_ADMIN_URL` / `DE_ADMIN_CREDS` (default: the tests' own) is the connection those run on. The tests' identity keeps the engine's API, because the engine refuses a caller from its own account (`invalid`, "no caller identity", its F39), so `info` and `cancelPoll` stay on the tests' connection. The scope:

- **subjects:** the three streams under `{prefix}.{tenant}.>`, the buckets under `$KV.<bucket>.{tenant}.>`, the run ids from `{prefix}.{tenant}.journal.<runId>`;
- **objects:** the metas whose name begins `{tenant}/` and the chunks they name. An object store's subjects carry no tenant, so a staged upload the tenant left half-way is the engine's to drop (`--upload-ttl`);
- **durables:** `{prefix}~{tenant}~…` only. An ephemeral consumer counts only if it is filtered on the tenant's subjects;
- **never:** another tenant's subject, object or durable, or a stream. `DE_TENANT` needs `DE_PREFIX` (the import's) and `DE_ENGINE_URL` (the engine runs in the other account), and refuses `DE_CLEANUP=delete`.

**A test that reads the stores itself** (a raw journal entry, a blob's bytes, a KV record, a work-subject publish) cannot as a tenant. The shell's `names()` / `Names()` is how every such read names a resource, so as a tenant it skips the test, saying so, with an explicit `skipAsTenant()` where a test opens its JetStream manager first. TypeScript skips 46 (by suite: `e2e` 11, `payload-codec` 14, `worker-recovery` 8, `offload` 6, `interop-py` 4, `cancel-running` 2, `matrix` 1), Python 17 (`test_payload_codec_e2e` 5, `test_worker_recovery` 3, `test_cancel_running` 2, `test_interop` 2, `test_interop_reverse` 2, `test_concurrency` 1, `test_matrix` 1, `test_worker` 1).

**Three things the proof found.**

- **A test skipped part-way never reaches vitest's `afterEach`** (`@vitest/runner` returns before the hooks when `context.skip()` throws). So what it wrote before the skip was never purged: the first tenant run left 193 journal messages and 482 MB of objects, and the four `interop-py` Python workers it had spawned kept polling. The same was true at `main` for the mode's own `DE_ENGINE_URL` skips that come after writes (`restartEngine`, a second worker's flags). Now the next test's `beforeEach` cleans a test whose cleanup never ran, before its snapshot, and the file's `afterAll` cleans the last one. `interop-py` stops a leftover worker in `beforeEach` and `afterAll` too. pytest runs fixture teardown after a skip, so Python needed neither.
- **Under `DE_ENGINE_URL` the end's sweep is not after the engine stops.** A poll a late-stopped worker had in flight bound four of A's durables after the sweep. The sweep now runs again after a 2 s × factor pause, up to three times, until a round finds nothing. And a test that reads the engine's log (`engineLog`) is skipped under `DE_ENGINE_URL`, saying so: that log is the operator's, and it used to read an empty one and fail (TypeScript `worker-recovery` "a work item for a run with NO journal …", seen in the mode with `DE_ENGINE_URL`; Python's `engine_log` is the same, by construction, not seen failing).
- **nats.js ignores `user:password@` in a server URL** (`Authorization Violation`), and the config-file server's users are names and passwords. The harness's `connect` / `connectAdmin` and the two interop scripts make a URL's user and password the connection's. The engine's Go client and nats-py read them anyway.

**Proven locally** on a config-file `nats-server` 2.14.6 with the engine's `TestTenancy*` setup (`internal/testserver.TenancyConfig(port, dir, NGS, "de", "A", "B")` at `d3e741d`, port 14322; config below), `durable-engine serve --tenancy --nats nats://de:de@127.0.0.1:14322 --prefix de --metrics-addr 127.0.0.1:19391 --tenant-max-bytes 0` as `de`. Before the run, tenant B had a parked run of its own (`b-seed-1`, a 12 MiB input: 4 journal messages, a RUNS and a LEASES key, a 12 MiB object, `de~B~worker-bparked` and `de~B~timer-bparked`). Then, as tenant A (`DE_NATS_URL=nats://a-client:a-client@…`, `DE_ADMIN_URL=nats://de:de@…`, `DE_TENANT=A`, `DE_PREFIX=de`, `DE_ENGINE_URL=http://127.0.0.1:19391`):

| Run                                                                        | TypeScript                        | Python                          |
| -------------------------------------------------------------------------- | --------------------------------- | ------------------------------- |
| as tenant A, the tenancy engine                                            | **61 passed, 80 skipped**, 52.5 s | **34 passed, 59 skipped**, 35 s |
| the plain mode with `DE_ENGINE_URL` (a standalone engine, prefix `plainx`) | 106 passed, 35 skipped, 42 s      | 50 passed, 43 skipped, 37 s     |
| the plain mode, the harness's engine                                       | 120 passed, 21 skipped, 70 s      | 73 passed, 20 skipped, 56 s     |

The tenant's counts are the `DE_ENGINE_URL` ones less the named skips: 106 − 61 = 45 of TypeScript's 46 (the 46th, the engine-log test, is a `DE_ENGINE_URL` skip as well) and 50 − 34 = 16 of Python's 17 (likewise). Afterwards B's footprint read the same as before, byte for byte (`nats stream subjects`, `nats consumer ls`, `nats object ls`, as `de`). The DE account held exactly B's (`DE_JOURNAL` 4 messages, the two keys, `OBJ_DE_BLOBS` 4 messages and 12,584,562 bytes, no durable of A's), and no worker process was left. The engine counted `de_engine_tenant_bytes{tenant="A"}` 562,146,147 for the two runs and `de_engine_tenant_runs_in_flight{tenant="A"}` 11: a purge never lowers the counters (the engine's F40). So at the default `--tenant-max-bytes` (1 GiB) a tenant would be refused `tenant_limit` (`bytes`) by its second or third full run, which is why the byte limit is 0 here. **For the dev NGS tenancy run:** the tenant's limit record (`KV_DE_TENANTS`) needs `maxBytes` raised or 0, or the counters reset between runs.

The config (the engine's, with its `NGS` limits, the port fixed):

```
host: 127.0.0.1
port: 14322
max_payload: 8388608
jetstream { store_dir: "<dir>/js", max_file_store: 1099511627776, max_mem_store: 268435456 }
accounts {
  DE {
    jetstream: { max_mem: 0, max_file: 53687091200, max_streams: 16, max_consumers: 250, max_bytes_required: true }
    users: [ { user: de, password: de } ]
    exports: [
      { service: "de.A.api.>", accounts: [A] }
      { service: "de.B.api.>", accounts: [B] }
    ]
  }
  A {
    users: [ { user: a-client, password: a-client }, { user: a-worker, password: a-worker } ]
    imports: [ { service: { account: DE, subject: "de.A.api.>" }, to: "de.api.>", share: true } ]
  }
  B {
    users: [ { user: b-client, password: b-client }, { user: b-worker, password: b-worker } ]
    imports: [ { service: { account: DE, subject: "de.B.api.>" }, to: "de.api.>", share: true } ]
  }
}
```

**What this does not prove:** a tenant on NGS, where the import is token-gated and the users are creds (`DE_NATS_CREDS` the tenant's, `DE_ADMIN_CREDS` the engine account's), and the harness starting a tenancy engine of its own (not built: `DE_TENANT` takes `DE_ENGINE_URL`'s, so the 13 flag tests skip as they do under any `DE_ENGINE_URL`).

### F39 — F32 handed on to the engine

F32 (nats-py's object-store delete writes a meta with an empty `mtime`, which the engine's nats.go reader cannot parse, so a blob deleted from Python reads `unavailable`, not `blob_gone`) goes to the engine as an ask from here: read such a meta as a deleted object. The coordinator carries it into the engine's next brief. Nothing changed here: the Python tests still delete an object by purging its subjects.

### F40 — The limit codes are refusals, `LimitError` (the engine's F46)

**Where.** `src/engine/errors.ts`, `src/engine/transport.ts`, `src/engine/worker.ts` and their twins in `python/src/synadia_durable/_engine/`; the engine's `docs/API.md` "Errors", "The licence", "Tenancy" at `a7c5010`; the brief `2026-10-05-durable-sdk-limit-codes.md`.

**What.** The engine's closed set has ten codes; both SDKs mapped eight and read `tenant_limit` and `licence_limit` as `unavailable`, so a client's caller retried what will not clear and a refused worker polled again as after a broker blip. Now both are **`LimitError`** (`code` the one the engine said, `limit` its `limit` member verbatim, the engine's message kept), an `EngineError` and not an `EngineUnavailableError`, so nothing that retries the transient retries it:

- **A client's call** (`start`, `signal`, `cancel`, `result`, the `put` parts) rejects with it after one request; `result()` no longer asks again as it does on `unavailable`.
- **A worker's `start()`** sends `info` and then one poll that checks once (`waitMs: 0`), and rejects with the `LimitError` of either (logged at error, naming the limit), so a process does not run without work silently. An item that first poll is handed goes to the first slot. `rate` is the exception: it clears within a second, so `start()` goes on and the slots meet it.
- **A refused poll** after that is logged at **error** with the code, the limit, the workflows and how many refusals the line stands for, once per limit per back-off however many slots it refuses, and that slot polls again after **30 s** (`rate`: 1 s). The API carries no retry advice, so the back-off is the API's prose ("`rate` clears within a second, the others when runs end or an operator acts"). `stop()` wakes a slot from its back-off at once.
- **Inside an execution** (a `child` at `runs`, an `append` at `bytes`, any op at `rate`) the pure core halts with `retry`, as on any infrastructure error, and the worker hands the item back (`done retry`), logged at error instead of `infra error during run`; `work.done`'s outcome is `limit`.
- **A code neither SDK knows** stays `EngineUnavailableError` (an older SDK on a newer engine keeps working), now logged at warn naming the code.

**The mirror.** `ENGINE_ERROR_CODES` in both languages is the codes table, code → class; `test/unit/engine-errors.test.ts` and `python/tests/test_engine_errors.py` read the table from the engine checkout at `engine.pin` (`git show <pin>:docs/API.md`, skipped without a checkout) and fail when the two differ, then check each code maps to its class. Before the pin moved, the test failed on the missing `licence_limit`, as meant.

**Brief vs API.** The brief names the limits `workflows`, `workers`, `runs_in_flight`, `runs_per_day`, `tenants`, `rate`, `bytes`; those are `durable-licence mint`'s flags. The API's `limit` member says `runs` and `runsPerDay` (the licence's) and `bytes`, `runs`, `workflows`, `polls`, `rate` (the tenant's), plus `expired`; the SDKs pass the engine's word through and match none of them but `rate`. The brief's integration test is "a tenant limit of 0 workflows"; the engine reads 0 or less as **no limit** (`engine/tenancy.go` `limited`), so the test sets the tenant's `maxWorkflows` to 1, binds one name and is refused the second, as the licence test does with `-workflows 1`.

**Tests.** Unit (no server), each language: every code its class; the table at the pin; both codes `LimitError` with their `limit`; no client call retried; `start()` rejected on a refused first poll and on a refused `info`, not on `rate`; a later refused poll one error line and one poll per slot per back-off; `stop()` prompt. Integration, each language (`limits`, `test_limits`): a standalone engine of the test's own on a licence `-workflows 1 -runs-in-flight 1` refuses a second workflow's worker at `start()` (`licence_limit` `workflows`) and a second run's `start` (`licence_limit` `runs`, the engine's refusal count up by exactly one), and the run in flight still finishes; an engine in the fabric mode on a three-account server of the test's own, tenant A's record `{"maxWorkflows": 1}`, refuses A's second workflow (`tenant_limit` `workflows`) while B binds two. Both skip against an existing server (they need a server and an engine of their own).

### F41 — The idle poll's warning, once a minute with a count

**Where.** `src/engine/worker.ts` `pollLoop`, `python/src/synadia_durable/_engine/worker.py` `_poll_loop`; the staging run the brief cites (four `unavailable: context deadline exceeded` refusals at a tenant's first polls, the engine's to fix in its next brief).

**What.** Each slot warned `poll failed; retrying` at its first failure and every twentieth, so a worker of 16 slots over a slow link logged 16 lines at once and more for as long as it lasted. The retry and its back-off (250 ms doubling to 5 s) stay; the warning is now the **worker's**, one line a minute at most (`EveryLog`, `POLL_WARN_EVERY_MS` / `POLL_WARN_EVERY_S`), carrying `failures`, the count since the line before, and `everyMs` / `every_s`. A refused poll at a limit (F40) and an `invalid` one log at error as before, and are not counted here. Unit-tested in both languages: four slots failing for most of a second are one warning.

### F42 — `engine.pin` is the licence merge, `a7c5010`, and the harness mints a test licence

**Where.** `engine.pin`; `test/harness/engine-bin.ts`, `engine-process.ts`, `global-setup.ts`, `engine.ts`; `python/tests/engine.py`, `python/tests/integration/conftest.py`; the engine's `docs/API.md` "The licence", `docs/QUICKSTART.md`, `Makefile`.

**What.** At `a7c5010` (the engine's PR #8, P2-22) a standalone `serve` refuses to start without a valid licence (exit 2). The harness now:

- **builds** what `make build-dev` builds, with the same two `go build`s but into this repo's `.engine-bin/`, so the engine checkout is read and built from, never written: `durable-engine-dev-<commit>` (`-tags devlicence`, which trusts the test issuer) and `durable-licence-<commit>`;
- **mints one test licence per run** (`durable-licence mint -test -name durable-sdk-tests`, the tool's defaults: 100 workflows, 100 workers, 10,000 runs in flight, a million a day, one tenant, 30 days) into a temporary directory, removed at the end, and starts every standalone engine with `DE_LICENCE` set to it: the per-test engines, and the existing-server mode's one engine. `DE_LICENCE` in the caller's environment wins (nothing is minted). With `DE_ENGINE` the tool is `durable-licence` beside it (`make build-dev`'s `bin/`), or `DE_LICENCE_TOOL`;
- starts an engine in **the fabric mode** (`limits`' tenancy test) with no `DE_LICENCE` at all: it needs none, and a licence would bound its tenants (the test licence's one tenant would refuse tenant B).

The existing-server mode is otherwise unchanged. `DE_ENGINE_URL`'s engine is its operator's, licence and all.

**The checkout.** `../durable-engine` was on another branch with local changes (`mario/engine-fabric-next` at `e379e90`), which the harness refuses (as in F22). Nothing was done to it: every run here built from a local clone of it at `a7c5010` (`DE_ENGINE_REPO=<scratch>/durable-engine`).

**Runs** (2026-10-05, a local nats-server 2.14.6, the engine `a7c5010` built by the harness with the minted licence, sequentially): TypeScript unit 345 passed (15 files); integration 139 passed, 4 skipped (137 and 4 before `limits`), 296 s. Python 509 passed, 3 skipped, 2 xfailed (the unit suites 417 passed, 2 xfailed; 484 passed before this work's 25 tests), 237 s. Against a local server (`DE_NATS_URL=nats://127.0.0.1:14222`): TypeScript 120 passed, 23 skipped, 75 s; Python 73 passed, 22 skipped, 56 s (the 2 new skips each are `limits`).

### F43 — The engine's fabric build, and the pin at `97a1778`

**Where.** `engine.pin`, `test/harness/engine-bin.ts`, `test/harness/engine-process.ts`, `python/tests/engine.py`; the engine's #9 (`97a1778`), its F44.

**What.** Since the engine's #9 a release build has no tenancy mode: `--tenancy` and the other fabric flags exist only in a binary built with `-tags fabric`, which needs no licence, and a build without the tag refuses the flag as unknown, exit 2. The harness built one `-tags devlicence` binary and used it for every spawn, the tenancy-mode ones included, so on this pin every tenancy test would have failed at the engine's start.

**SDK.** The harness resolves two builds from the pinned checkout into `.engine-bin/`: `durable-engine-dev-<commit>` (`-tags devlicence`, with the licence tool beside it) and `durable-engine-fabric-<commit>` (`-tags fabric`); a spawn whose arguments carry `--tenancy` runs the fabric build, in both languages. A caller's `DE_ENGINE` stays the standalone binary; `DE_ENGINE_FABRIC` names the fabric one beside it, else the checkout at the pin is built. Nothing else of the pin's changes reaches the suites: the engine's F48 makes a tenant's first polls answer `none` while its durables are created (the SDK's retry path unchanged), F49 sizes resources at creation only, F50 adds an optional `onBehalfOf` the SDKs do not send.

**Where it belongs.** The fabric's pins round (2026-10-05); closed here.

### F44 — `stop()` waits for its polls, and the examples stop their worker in a `finally` (the engine's F51)

**Where.** `src/engine/worker.ts` `stop()`, `python/src/synadia_durable/_engine/worker.py` `Worker.stop()`; `examples/*.ts`, `python/examples/*.py`, the READMEs' and `docs/FABRIC.md`'s snippets. Staging, 2026-10-05: the `charge` example's run stalled two minutes (the engine's F51, which has the interleaving and the reproduction).

**What.** Two things of the SDK's turned the engine's stall (a signal journaled by the instance not driving the run, deferred to the lease's TTL; fixed in the engine's #11) into a worker's lifetime. (1) **The examples exited without `stop()`**: `handle.result(20_000)` timed out, `main().catch` called `process.exit(1)`, and `worker.stop()` and `nc.drain()` never ran, so the engine never heard `cancelPoll` and kept the process's sixteen polls for their 30 s; the item that became ready went to one of them, and waited out its ack wait for a process that was gone. (2) **`stop()` returned before its polls did**: it withdrew them (F5's close) and waited for the items in flight, but not for the slots' loops. A poll answered with an item in the instant before the withdrawal reaches the worker after the `cancelPoll`'s reply; the loop handed it back (`done retry`) only if the process was still alive, and a caller that exits right after `stop()` (as every example does) lost that `done`: the item waited its ack wait (30 s by default).

**SDK.** `stop()` now returns once every slot's loop has ended: each slot's item in flight has finished and its poll has returned, an item it was handed then handed back (`done retry`) before `stop()` resolves. Both languages; bounded as before (TypeScript 30 s, then it returns and warns; Python `drain_timeout_s`, then the items and the slots still running are cancelled and redeliver at the ack wait). A poll no engine withdrew (the `cancelPoll` itself failed, the broker away) ends at its own wait, within that bound. **A poll that reaches the engine after the withdrawal is withdrawn again**: the engine's `cancelPoll` withdraws the polls registered at that moment and remembers nothing (`engine/ops.go` `withdraw`), so a poll sent just before it but served just after waits its whole wait (30 s). The full suite's first run showed it: `network` "a worker's start costs a pinned number of DEPENDENT round trips" starts and stops a worker thirteen times, each `stop()` right after `start()`, and timed out at 30 s once `stop()` waited for the polls. So while a slot's poll is still out, `stop()` sends `cancelPoll` again every 250 ms (`WITHDRAW_AGAIN_MS`, `WITHDRAW_AGAIN_S`); that test is back to 3 s. Every example in both languages (`charge`, `cancel`, `crash-replay`, `fan-out`, `saga`) and the snippets in both READMEs and `docs/FABRIC.md` stop the worker and drain the connection in a `finally` (`try { … } finally { await worker.stop(); await nc.drain(); }`, Python `try`/`finally`), so a timeout or any error leaves no registered poll behind; `main().catch` prints and exits non-zero after the `finally`. `crash-replay`'s simulated crash (`process.exit` / `os._exit` inside the workflow) skips it on purpose, and says so. `charge`'s comment says the signal may land before the `await` over a slow link, and that the engine journals it and the run takes it on the next replay (F51).

**Ask (the engine).** A `cancelPoll` that names a worker could withdraw, for a few seconds, a poll of that worker arriving after it (a tombstone per worker id), so one withdrawal is enough; the SDK's repeat then costs nothing more than it does now.

**Tests.** `worker-recovery` "stop() hands back an item its poll was handed as it stopped" and `test_worker_recovery` `test_stop_hands_back_an_item_its_poll_was_handed_as_it_stopped`: a worker (concurrency 1) on a connection of its own, wrapped so that a poll answered with an item is held until the worker's `cancelPoll` has been answered and 200 ms more, as the wire would; the run is started, the item reaches the poll, `stop()` is called; the test asserts the hand-back (`done retry`) was answered before `stop()` resolved, closes the connection at once (the process exits), and a second worker then completes the run within 15 s (the ack wait is 30 s). Against the previous `stop()` both fail: no `done` before `stop()` resolved, and, with that assertion removed, the run not done in 15 s (TypeScript checked both; Python the first). On this pin the suites need nothing of the engine's #11: the test exercises the SDK's stop alone, so `engine.pin` stays `97a1778`.

### F45 — The engine's floor: `start()` refuses an engine without `withdrawn-remembered`, and `stop()` withdraws once (F44's close)

**Where.** `src/engine/errors.ts` (`UnsupportedEngineError`, `ENGINE_FLOOR`, `checkEngineFloor`), `src/engine/worker.ts` `start()`, `stop()`, `pollLoop`; `python/src/synadia_durable/_engine/errors.py`, `_engine/worker.py`; `engine.pin`; the engine's #13 (`16cb3da`), its F53 and F57.

**What.** Since the engine's #12 one `cancelPoll` is enough: a worker it names is remembered 5 s on every instance, and a poll of it that arrives in that window answers `none` at once (its F53). `stop()` still sent `cancelPoll` again every 250 ms while a poll was out (F44), because it could not tell whether the engine it spoke to had F53. Since the engine's #13 `info` says: `commit`, the build's commit, and `capabilities`, a list of names, the first `withdrawn-remembered` (its F57). **The floor is a capability, not a version:** `info.engine` is the build's `git describe`, the engine repo has no tag, so every build so far says `0.2.0-dev` (staging's image included), and a floor read from it would have nothing to read.

**SDK.** Both languages.

- **`start()` checks the floor.** Its `info` probe now reads the reply: no `capabilities`, or a list without `withdrawn-remembered`, rejects (raises) with `UnsupportedEngineError`, an `EngineError` with `code` `unsupported` (the SDK's own, not one of the engine's codes, so `ENGINE_ERROR_CODES` stays the engine's table), `engine`, `commit`, `capabilities` and `missing`. Its message names the prefix, the engine's `engine` and `commit` as `info` gave them, and what this SDK needs (the engine at or after its #13, `16cb3da`, which announces `withdrawn-remembered`). It is logged at error once. A `LimitError` at `info` and a closed connection behave as before.
- **No engine at start: the check is deferred, not skipped.** When nothing answers `info` (no responders, a timeout: not fatal, the slots poll until an engine does), the first slot whose poll is answered sends one `info` and applies the same check. Slots answered meanwhile wait on that one request. A failed `info` there is left to the next answered poll. A refusal there is logged at error (`…; this worker stops`) and stops the worker: an item the poll was handed goes back (`done retry`), and since `start()` has already resolved, the caller sees `health().running` false.
- **`start()` sends its `waitMs` 0 poll whether or not `info` answered** (before, only when it did). A failure there is swallowed as before, and the slots meet it again. A `LimitError` there still rejects `start()`.
- **One `cancelPoll` in `stop()`.** `WITHDRAW_AGAIN_MS` / `WITHDRAW_AGAIN_S` and the re-withdraw in the drain loop are gone: `stop()` withdraws once, then waits for the slots' loops within the drain bound as before (TypeScript 30 s, Python `drain_timeout_s`). What `stop()` guarantees is unchanged: every in-flight poll returns, an item handed in the instant before goes back (`done retry`), then `stop()` resolves. Both workers' head comments say the new shape.

**F53's restart gap, the SDK's half.** The engine's F53 left one case: a restart within the 5 s window whose `start()` found no engine sent no `waitMs` 0 poll, so the restarted worker's polls were answered `none` until the window ended. `start()` now always sends that poll. When it reaches an engine, that closes the gap: the engine treats a `waitMs` 0 poll as a start before its tombstone check, ends the window on its instance and broadcasts that to the others (the engine's F53, "the restart gap, closed from the SDK's side"). When it reaches no engine (no responders), there is nothing to end, since an engine that comes up later remembers no withdrawal. What remains is the engine's own bound and not the SDK's: an instance that misses the repoll broadcast keeps its tombstone for the rest of its 5 s.

**Tests.** Unit, each language (`engine-errors`, `test_engine_errors`, 5 new each, against a fake connection): no `capabilities` rejects with the stub's `engine` and `commit` in the message, and nothing but `info` is sent; capabilities without the name reject; an engine at the floor starts, with one `info`; a start that found no engine still sends its `waitMs` 0 poll, and the first answered polls send one more `info`, hand their items back, stop the worker and log one error; `stop()` with four polls out sends one `cancelPoll` and none afterwards. Integration, each language (`engine-floor`, `test_engine_floor`, 4 each):

- **the floor**, on stub responders on a prefix of the test's own (`{prefix}.api.v1.info`, `.poll`, `.done`): an `info` without `capabilities` makes `start()` reject with `UnsupportedEngineError` carrying the stub's `engine` and `commit`; with `capabilities: ["withdrawn-remembered"]` `start()` goes through; **the deferred check**: the stub subscribes `info` only after `start()` has resolved, and the worker stops within 5 s, one error line naming the stub's commit.
- **one withdrawal, on the pinned engine**: a subscription on `{prefix}.api.v1.poll` (outside the engine's queue group, so it sees every request) counts exactly one `cancelPoll` from the worker in each of seven `stop()`s of one worker (concurrency 4): one with its polls waiting, five right after their `start()`, and one whose polls the worker's connection holds back 600 ms, so they reach the engine after the withdrawal. Every `stop()` resolves under 5 s (a poll's wait is 30 s). The worker started again afterwards sends no more than its start's poll and one per slot in a second, so its polls wait instead of being answered `none` in a loop. **Against the previous `stop()`** (the worker of `main` put back) the count is 3 in both languages, in the held-back case: on an engine with F53 the other cases' polls return before 250 ms, and the repeat never fires there.

**Not done.** The deferred check's refusal stops the worker through its own `stop()`, which withdraws once. Against an engine below the floor (no F53), a poll that arrives just after that withdrawal waits its own wait, within `stop()`'s bound. This applies only to an engine already being refused.

**`test_network`'s lost ack, under load** (the brief's item 6). `test_a_lost_ack_the_append_landed_its_reply_did_not` failed twice for the coordinator on 2026-10-05 with `result timed out` after 30 s, under load, and passed alone and quiet. The test passes no engine flags: the `ack_wait_ms` / `lease_ttl_ms` 2 000 the brief cites at line 644 belong to the test before it (`test_control_with_reconnect_off_…`), so this one's engine runs the defaults, ack wait and lease TTL 30 s. **What the 30 s bound protects** is the worker's own recovery. Its step-end append lands and the reply is lost, so the request times out at the transport's margin (`REQUEST_MARGIN_MS`, 10 s). The pure core halts `retry`, the worker sends `done retry`, and the redelivery replays and finishes. Measured in the engine's log: the append sent at :02.55, timed out at :12.554, run done by :12.825, the test 11.5 s in all. That was 3 of 3 quiet, 3 of 3 under 30 busy processes (load 21), and 3 of 3 beside the engine's `go test -race ./...`, all at 11.5–11.7 s. **The slow side is the engine's lapse, not the test's wait.** The worker's path is a 10 s timer plus about 0.3 s, which load does not stretch, so a run that reaches 30 s has left that path. Its `done` did not land, and the item waited out the 30 s ack wait (and lease). Not reproduced here, so nothing is widened. The bound stays 30 s: that is what separates the worker's retry from the lapse, and widening it would hide the case. The test now fails saying which side it was: elapsed time, the `work.done` outcomes, the appends and the engine's last warnings. The engine's code between `5047d79` and `16cb3da` changes nothing on this path (`info`'s two members, a debug line, a gauge).

**Runs** (2026-10-06, a local nats-server 2.14.6, the engine at `16cb3da` built by the harness from a clone, `DE_ENGINE_REPO`, since `../durable-engine` is on another branch; sequentially): TypeScript `npm run check` (typecheck, lint, format, unit 350 passed, 345 before); `npm run test:integration` 144 passed, 4 skipped (140 and 4 before), 295 s. Python `uv run pytest` 519 passed, 3 skipped, 2 xfailed (510 before), 243 s; `ruff check`, `ruff format --check`, `mypy` clean. Against a local server (`DE_NATS_URL=nats://127.0.0.1:14222`): TypeScript 125 passed, 23 skipped, 69 s; Python 78 passed, 22 skipped, 60 s. The pure core's diff against `7456c75` is empty in both languages.

### F46 — `engine.pin` `91ac210`: one pair, no journal dropped by age, eight resources, and the signal `id`

**Where.** `engine.pin`; `test/harness/` (`engine-process.ts`, `engine.ts`, `run-engine.ts`, `engine-bin.ts`, `existing.ts`, `global-setup.ts`), `python/tests/` (`engine.py`, `existing.py`, `integration/conftest.py`); `src/engine/client.ts`, `python/src/synadia_durable/_engine/client.py`; the engine's #18 to #21, its F69, F70, F72 to F77.

**What.** The engine at `91ac210` (on staging since 2026-10-16) changed three things the harness and three tests assumed:

- **One durable pair per prefix.** Standalone binds `{prefix}~worker` on `{P}_WORK` and `{prefix}~timer` on `{P}_TIMERS` for every workflow name (the engine's F77); the fabric mode's pair is `{prefix}~{tenant}~worker` / `~timer` (F69). The per-name `de-worker-{wf}` / `de-timer-{wf}` are gone.
- **No journal is dropped by age, not by count**, in both modes (F75, F77): a work item whose run has no journal is nak'd with the backoff (250 ms doubling) while younger than `--no-journal-max-age` (default 10 min), then termed `dropped-no-journal`.
- **Eight resources per prefix.** Standalone provisions `KV_{P}_TENANTS` too. Its key `workflows` holds the set of workflow names (7 days' retention), which the licence's `maxWorkflows` counts at every poll. At `a6fa5a0` the licence counted the work stream's durables, which the per-name cleanup deleted after each test; the set now persists across tests.

**The three failures** (found by the engine's PR 2b worker in a scratch copy, 2026-10-16; each a harness or test assumption, none the engine's): TypeScript `e2e` "a work item for a run with NO journal …" polled `de-worker-ghost_wf`, which no longer exists; TypeScript `worker-recovery` and Python `test_worker_recovery` "no journal" expected the term on the fifth delivery and four retries, and at the default age the item is retried for 10 min.

**The fixes.**

- The harness gains `noJournalMaxAgeMs` / `no_journal_max_age_ms` → `serve --no-journal-max-age <n>ms`, copied to the existing-server engine (`runFlags`), and in Python dropped before the SDK's keyword-only `create_worker` / `create_client`. The ghost tests run at 3 s: e2e polls `${prefix}~worker` until drained; both `worker-recovery` tests assert at least one retry (`retry-no-journal`) and the drop (`dropped-no-journal`); the order is not observable through counts, since the outcomes are built from counters in label order (the backoff gives about four retries in 3 s).
- **The run's licence is minted with no name limit** (`-workflows 0`; the engine's own cap of 1000 names stays): the existing-server run puts every test's names on one prefix, and the set persists, so the tool's default of 100 would refuse polls mid-run. A test's own mint (`limits`) is unchanged.
- **The existing-server mode deletes no durable.** The per-test and end-of-run durable loops existed for the consumer cap over ~150 per-name durables (F29, F33). The one pair is bound by the run's engine at its first poll, so it was absent from the run's first snapshot and the loop would have deleted it after the first test, making the engine re-bind under its long pull (the engine's `TestDurablesDeletedUnderTheEngine`: the 10012 store race and a 0.5 s backoff). The pair goes with the prefix's streams at the end. `durableOf` / `durable_of` count the pair alone standalone, for the end's report. `KV_{P}_TENANTS` is the engine's record (it caches the set and writes it by CAS), so the per-test purge skips it.
- **Eight everywhere:** `prefixStreams` / `prefix_streams` (`KV_{P}_TENANTS` last, so the existing indexes still name the streams they meant), `DE_CLEANUP=delete` deletes the eight, `ProvisionQuotaError`'s shipped text ("three streams, four KV buckets, one object store; ~5.2 GiB at the 1 GiB default"), and every comment and doc that said seven. The spawned profile's stream cap stays 16, two prefixes of eight.

**The signal `id`** (the engine's #19 journals a signal at send and deduplicates it by `id`): every `signal` now carries one, the caller's (`{ id }`, Python `id=`) or a generated UUID, checked with the token rule before any request. Before, the SDKs sent none and the engine minted one, so a caller retrying after `EngineUnavailableError` sent a second signal. New: `test/integration/signal-id.test.ts` (4) and `python/tests/integration/test_signal_id.py` (8, the invalid id parametrised five ways): the same id twice journals one entry (`signalId`) and the run takes it once; two without an id journal two, each a UUID; an invalid id sends nothing on the signal subject; a retry with the same id after the run continued is answered `ok` and journals nothing in `_g1`. Unit tests in `engine-errors` / `test_engine_errors` (6 TypeScript, 6 Python): the id sent as given or generated, and refused before any request.

**`ctx.attempt`** is documented as the delivery count (the engine's F70): Python `test_working`'s `attempts == [1]`, the watch item, passed in every run here.

**Runs** (2026-10-16, nats-server 2.14.6, the engine built by the harness from a clean checkout at `91ac210`, `DE_ENGINE_REPO`; one suite at a time):

| Run                                                                                    | TypeScript                                                                                | Python                                                   |
| -------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| `npm run check` (unit) / the Python gate (`ruff format --check`, `ruff check`, `mypy`) | 356 passed, 8 s                                                                           | clean                                                    |
| integration, spawned                                                                   | 148 passed, 4 skipped, 402 s                                                              | `uv run pytest`: 533 passed, 3 skipped, 2 xfailed, 303 s |
| existing server (`DE_NATS_URL` local, `DE_CLEANUP=delete`)                             | 129 passed, 23 skipped, 101 s                                                             | 86 passed, 22 skipped, 84 s                              |
| the ghost and signal tests, five times each                                            | e2e "NO journal" 5 of 5; `worker-recovery` 11 of 11, 5 times; `signal-id` 4 of 4, 5 times | 9 of 9, 5 times                                          |

In the existing-server runs the pair was on the server in every sample (every 3 s) from the first poll to the run's end, the engine logged `pair bound` once per start (TypeScript 21 for 22 starts, Python 20 for 21: the engine restarts for a test's flags) and no `re-bind`, `10012` or `10100`; the end's report showed the pair and the eight empty but for `KV_{P}_TENANTS`' one record, and `DE_CLEANUP=delete` left no stream on the server.
