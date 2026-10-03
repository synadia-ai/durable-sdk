"""synadia-durable — the thin durable execution SDK: the replay core in your process,
``durable-engine`` behind it.

A workflow is an ordinary ``async`` function whose every nondeterministic or side-effecting
operation goes through ``ctx`` and is journaled; on a crash the function re-runs from the top,
journaled operations replay, and the first un-journaled one is the live frontier. The replay core is
the library's (``synadia-durable`` at ``7456c75``), unchanged; behind its three seams and the
client's five calls sits a request/reply client for ``durable-engine``, which owns the streams.

The wire contract is the library's ``docs/PROTOCOL.md`` and the engine's ``docs/API.md``, the same
as the TypeScript SDK's, so runs, signals and sub-workflow calls cross the language boundary.
"""

from synadia_durable._engine.client import Client, RunHandle, create_client
from synadia_durable._engine.errors import (
    EngineError,
    EngineFencedError,
    EngineUnavailableError,
    InvalidRequestError,
    LeaseExpiredError,
    RunNotFoundError,
)
from synadia_durable._engine.worker import PumpHealth, Worker, WorkerHealth, create_worker
from synadia_durable._internal.logger import Logger, StdlibLogger
from synadia_durable._internal.metrics import Metrics, SilentMetrics
from synadia_durable._nats.connect import RECONNECT_DEFAULTS, with_reconnect_defaults
from synadia_durable.core.workflow import define_workflow, workflow
from synadia_durable.errors import (
    BlobIntegrityError,
    BlobMissingError,
    CancelledError,
    ConnectionClosedError,
    ConsumerLimitError,
    DurableError,
    DurableTimeoutError,
    FencedError,
    NonDeterminismError,
    NotImplementedFeatureError,
    PayloadCodecError,
    PayloadTooLargeError,
    ProvisionQuotaError,
    SerializationError,
    StepNestingError,
    WorkflowFailure,
)
from synadia_durable.journal.blobs import OffloadOptions
from synadia_durable.journal.payload_codec import PayloadCodec, aes_gcm_payload_codec
from synadia_durable.types import (
    Ctx,
    DurableFuture,
    Handle,
    RetryPolicy,
    RunState,
    RunStatus,
    StartOptions,
    Workflow,
)
from synadia_durable.version import (
    VERSION,
    WIRE_VERSION,
    WIRE_VERSION_BLOBS,
    WIRE_VERSION_ENCODED,
)

__all__ = [
    "RECONNECT_DEFAULTS",
    "VERSION",
    "WIRE_VERSION",
    "WIRE_VERSION_BLOBS",
    "WIRE_VERSION_ENCODED",
    "BlobIntegrityError",
    "BlobMissingError",
    "CancelledError",
    "Client",
    "ConnectionClosedError",
    "ConsumerLimitError",
    "Ctx",
    "DurableError",
    "DurableFuture",
    "DurableTimeoutError",
    "EngineError",
    "EngineFencedError",
    "EngineUnavailableError",
    "FencedError",
    "Handle",
    "InvalidRequestError",
    "LeaseExpiredError",
    "Logger",
    "Metrics",
    "NonDeterminismError",
    "NotImplementedFeatureError",
    "OffloadOptions",
    "PayloadCodec",
    "PayloadCodecError",
    "PayloadTooLargeError",
    "ProvisionQuotaError",
    "PumpHealth",
    "RetryPolicy",
    "RunHandle",
    "RunNotFoundError",
    "RunState",
    "RunStatus",
    "SerializationError",
    "SilentMetrics",
    "StartOptions",
    "StdlibLogger",
    "StepNestingError",
    "Worker",
    "WorkerHealth",
    "Workflow",
    "WorkflowFailure",
    "aes_gcm_payload_codec",
    "create_client",
    "create_worker",
    "define_workflow",
    "with_reconnect_defaults",
    "workflow",
]
