# Findings

What building the thin TypeScript SDK found against the engine's API (`../durable-engine/docs/API.md` at `d8c0d73`, which `engine.pin` names), the frozen library (`../synadia-durable-execution` at `7456c75`) and the brief (`synadia-agent-fabric/local/briefs/2026-10-03-durable-sdk-ts.md`). Where the brief and `API.md` differ, `API.md` won and the difference is here. Nothing in the engine or the library was changed; each gap in them is an **ask**.

**Date:** 2026-10-03 · **Engine:** `d8c0d73` (`0.2.0-dev`) · **Library:** `7456c75` · **Server:** nats-server 2.14.6 · **Node:** 25.9 · **Python:** 3.14.7, nats-py 2.15.0
**Follow-up:** 2026-10-04 · **Engine:** `1efd2ec` (its PR #3, which answered F1, F2, F5 to F9 as its own F18 to F25) · the brief `2026-10-04-durable-sdk-followup.md`

F1 to F14 came from the TypeScript SDK (P2-19a). F15 to F21 came from the Python SDK (P2-19b, the brief `2026-10-03-durable-sdk-py.md`); where a TypeScript finding also blocks a Python test, its **Blocks** line says so. F22 onwards came from the follow-up. A finding the engine answered keeps its text and gains a **Closed** line.

| #   | Kind             | One line                                                                                                                            | Blocks                 |
| --- | ---------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ---------------------- |
| F1  | closed           | API v1 has no chunked upload: a value over `max_payload` − 16 KiB cannot reach the engine, where the library offloaded it           | — (was 16 TS + 5 Py)   |
| F2  | closed           | `serve` has no `--offload-threshold`, and at the default threshold the engine never offloads an appended entry                      | — (was 6 Python tests) |
| F3  | brief vs cut     | The cut needs four files the brief left behind or trimmed: `inbound.ts`, all of `blobs.ts`, `nats/protocol.ts`, `nats/connect.ts`   | —                      |
| F4  | brief vs suites  | Three of the 13 unit suites cannot pass unchanged: `call-roster`, `payload-paths`, `version`                                        | —                      |
| F5  | closed           | A poll cannot be withdrawn: the engine hands an item to a poll whose caller is gone, and the item waits out its ack wait            | —                      |
| F6  | closed           | A signal sent as `dataEnc` is journaled as a blob ref whatever its size, where the library journals a small one inline              | —                      |
| F7  | closed           | With `DE_TIMERS` gone the engine refuses every `poll`; the library's work pump kept serving                                         | —                      |
| F8  | closed           | The engine neither logs nor counts losing a run lease mid-drive                                                                     | —                      |
| F9  | closed           | Past `maxWorkMs` the lease is gone and refuses the holder's late appends: a step longer than `maxWorkMs` never completes            | —                      |
| F10 | note             | Engine-side decisions are invisible to the worker; seven assertions and five waits re-expressed through `info` and the engine's log | —                      |
| F11 | note             | With one engine instance a run's items are serialised: the library's two-worker races no longer race                                | —                      |
| F12 | library note     | A `fail` terminal that cannot be journaled ends as a fence for ever, in the pure core                                               | (with F1)              |
| F13 | brief vs suites  | The suites' own JetStream client needs `@nats-io/jetstream`, `kv`, `obj` as devDependencies                                         | —                      |
| F14 | behaviour change | Payloads are encrypted before they leave the process: work items carry no plaintext any more                                        | —                      |
| F15 | brief vs suites  | Python's unit suites: four cannot pass unchanged, two are the shell's, one named in the brief does not exist                        | —                      |
| F16 | brief vs cut     | Python's cut keeps three pure files of `_nats/`, and the suites' own JetStream client is the library's shell under `tests/shell/`   | —                      |
| F17 | **nats-py gap**  | nats-py has no status stream and does not fail a request in flight at close: the SDK polls `stats` and `is_closed`                  | —                      |
| F18 | brief vs cut     | The `durable_agent` example is left out: it needs the fabric's agent SDK                                                            | —                      |
| F19 | note             | `upgrade-compat` is deleted in TypeScript; the library's Python tree never had one                                                  | —                      |
| F20 | brief vs suites  | Eight of the library's 21 Python integration suites test the shell only and are not copied; five tests dropped in place             | —                      |
| F21 | behaviour change | Where the Python SDK differs from the TypeScript one, and why (its metrics ask closed)                                              | —                      |
| F22 | note             | The engine checkout carried someone's uncommitted changes; the suites built the pin from `../wt/durable-engine`                     | —                      |
| F23 | note             | A missing blob is parked by the engine before an item reaches the worker; the offload test reads the engine's log                   | —                      |
| F24 | **API gap**      | `get` of a blob that is gone answers `internal`, the code of any operational fault: the SDK must read `internal` as "gone"          | —                      |
| F25 | brief vs suites  | The chunked upload is a fourth payload-blob route: both `payload-paths` rosters name it                                             | —                      |
| F26 | behaviour        | What the chunked transport decides: who mints the runId, `offload: false`, the lease on a part, the cache                           | —                      |
| F27 | **engine gap**   | `status` lags `result` by the worker's `done`: a run whose `result` is terminal can still read `suspended` for a round trip         | —                      |
| F28 | note             | The suites over the delay proxy, at 150 and 300 ms: what changed, and what an NGS run needs                                         | —                      |

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
