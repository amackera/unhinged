"""Unhinged: Absurd's durable-execution schema, unmoored from Postgres and run on SQLite."""

__version__ = "0.2.0"

from .aio import AsyncTaskContext, AsyncUnhinged
from .engine import (
    Engine,
    EnginePool,
    InvalidRetryStrategy,
    RunAlreadyFailed,
    TaskCancelled,
    UnhingedError,
    retry_delay_seconds,
    uuidv7,
)
from .schema import SCHEMA_VERSION
from .sdk import (
    StepHandle,
    SuspendTask,
    TaskContext,
    TaskResultSnapshot,
    TimeoutError,
    Unhinged,
    get_current_context,
)

__all__ = [
    "__version__",
    "AsyncTaskContext",
    "AsyncUnhinged",
    "Engine",
    "EnginePool",
    "InvalidRetryStrategy",
    "RunAlreadyFailed",
    "SCHEMA_VERSION",
    "StepHandle",
    "SuspendTask",
    "TaskCancelled",
    "TaskContext",
    "TaskResultSnapshot",
    "TimeoutError",
    "Unhinged",
    "UnhingedError",
    "get_current_context",
    "retry_delay_seconds",
    "uuidv7",
]
