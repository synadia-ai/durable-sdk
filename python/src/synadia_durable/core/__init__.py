"""synadia-durable — the pure core.

Nothing in this package may import ``nats``: the replay engine depends only on the seams
(``Journal``, ``Scheduler``, ``Children``), which is what makes the whole crash / replay /
concurrency story testable without a server. A lint rule enforces it.
"""
