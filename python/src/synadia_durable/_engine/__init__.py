"""synadia-durable — the engine client: the only package that speaks to the broker at all.

One request to ``{prefix}.api.v1.<op>`` per call (``durable-engine`` ``docs/API.md``), through
``nats-py``'s core request/reply. The streams, buckets and consumers behind the API are the
engine's. Mirrors ``src/engine/`` of the TypeScript SDK, module for module.
"""
