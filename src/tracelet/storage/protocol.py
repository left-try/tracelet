from __future__ import annotations

import inspect
import asyncio
from typing import Any, Protocol


class StorageAdapter(Protocol):
    """Application-owned persistence contract; operations may be sync or async."""

    def enqueue(self, job_id: str, payload: dict[str, Any]) -> Any: ...
    def get(self, job_id: str) -> Any: ...
    def list_pending(self) -> Any: ...
    def claim(self, job_id: str, *, worker_id: str) -> Any: ...
    def checkpoint(self, job_id: str, *, result: Any) -> Any: ...
    def release(self, job_id: str) -> Any: ...
    def complete(self, job_id: str, *, result: Any = None) -> Any: ...
    def retry(self, job_id: str, *, error: str) -> Any: ...
    def fail(
        self,
        job_id: str,
        *,
        error: str,
        retryable: bool = False,
        increment_attempt: bool = True,
    ) -> Any: ...


async def call_storage(adapter: Any, method: str, *args, **kwargs):
    """Call async adapters directly and isolate sync I/O from the event loop.

    A synchronous adapter can set ``sync_on_event_loop = True`` for APIs whose
    objects are thread-affine. Such adapters should generally provide async
    methods instead, since their I/O will block the caller's loop.
    """
    operation = getattr(adapter, method)
    if inspect.iscoroutinefunction(operation):
        return await operation(*args, **kwargs)
    if getattr(adapter, "sync_on_event_loop", False):
        result = operation(*args, **kwargs)
    else:
        result = await asyncio.to_thread(operation, *args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


def validate_storage_adapter(adapter: Any) -> bool:
    required = ("enqueue", "get", "list_pending", "claim", "checkpoint", "release", "complete", "retry", "fail")
    return all(callable(getattr(adapter, name, None)) for name in required)
