# durable-sdk

The thin durable execution SDKs, TypeScript and Python. Each keeps the replay core of the library `@synadia-ai/durable` (`synadia-ai/durable-ts`, frozen at `7456c75`): workflows as ordinary functions over a `ctx`, every side-effecting or nondeterministic step journaled and replayed after a crash, the determinism guards, the out-of-band halts. Behind the core's three seams (`Journal`, `Scheduler`, `Children`) and the client's five calls sits a request/reply client for [`durable-engine`](https://github.com/synadia-ai/durable-engine), which owns the streams. A customer's workflow functions run in the customer's process; nothing runs without the engine.

Private until published. Created 2026-10-03 on the Synadia Agent Fabric's decision 61 (`synadia-agent-fabric-docs`, `DECISIONS.md`; the research `docs/research/2026-10-03--durable-execution-as-a-product.md`).

- **The public API** is the library's: `defineWorkflow`, `createClient`, `createWorker`, `ctx.run/sleep/await/call/now/random/uuid/continueAsNew/onCancel`; the library's integration suites are this repo's acceptance suites, run against the engine.
- **Documentation** is this README and the examples, kept minimal on purpose; the library's website and guides are not carried over.

Status: empty; the TypeScript SDK comes first, once the engine's API is fixed (the fabric's board row P2-19).
