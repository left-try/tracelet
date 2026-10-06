from __future__ import annotations

import inspect
import json
import asyncio
import uuid

from ..redaction import RedactionPolicy
from .protocol import call_storage


class S3Sink:
    """Write evaluation records to stable S3-compatible object keys."""

    def __init__(self, *, client, bucket: str, prefix: str = "", redaction: RedactionPolicy | None = None):
        self.client = client
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.redaction = redaction or RedactionPolicy()

    def object_key(self, job_id: str) -> str:
        name = f"{job_id}.json"
        return f"{self.prefix}/{name}" if self.prefix else name

    async def write(self, *, job_id: str, record: dict) -> None:
        sanitized = self.redaction.sanitize(record)
        body = json.dumps(sanitized, ensure_ascii=False, allow_nan=False).encode("utf-8")
        call = self.client.put_object
        kwargs = {"Bucket": self.bucket, "Key": self.object_key(job_id),
                  "Body": body, "ContentType": "application/json"}
        if inspect.iscoroutinefunction(call):
            result = call(**kwargs)
        else:
            result = await asyncio.to_thread(call, **kwargs)
        if inspect.isawaitable(result):
            await result

    async def deliver_from_outbox(self, store, job_id: str) -> None:
        job = await call_storage(store, "get", job_id)
        if job is None:
            raise KeyError(job_id)
        try:
            await self.write(job_id=job_id, record=job["payload"])
        except Exception as exc:
            try:
                error = self.redaction.sanitize(str(exc))
            except Exception:
                error = "S3 delivery failed; redaction unavailable"
            if hasattr(store, "defer"):
                await call_storage(store, "defer", job_id, error=error)
            else:
                await call_storage(store, "retry", job_id, error=error)
            raise
        await call_storage(store, "complete", job_id, result={"object_key": self.object_key(job_id)})


class S3DrainWorker:
    """Poll a storage outbox and upload pending jobs to an S3Sink."""

    def __init__(self, *, store, sink: S3Sink, poll_interval: float = 1.0, batch_size: int = 100, max_attempts: int = 5):
        if poll_interval <= 0 or batch_size < 1 or max_attempts < 1:
            raise ValueError("poll_interval, batch_size, and max_attempts must be positive")
        self.store = store
        self.sink = sink
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.max_attempts = max_attempts
        self.worker_id = str(uuid.uuid4())
        self._runner = None
        self._stopping = False
        self._wake = asyncio.Event()
        self.last_error: Exception | None = None

    async def drain_once(self) -> int:
        completed = 0
        pending = await call_storage(self.store, "list_pending")
        for job in pending[: self.batch_size]:
            job_id = job["job_id"]
            if int(job.get("attempts", 0)) >= self.max_attempts:
                await call_storage(
                    self.store,
                    "fail",
                    job_id,
                    error=job.get("error", "S3 retry limit reached"),
                    retryable=False,
                    increment_attempt=False,
                )
                continue
            claimed = await call_storage(self.store, "claim", job_id, worker_id=self.worker_id)
            if not claimed:
                continue
            try:
                await self.sink.deliver_from_outbox(self.store, job_id)
                completed += 1
            except asyncio.CancelledError:
                await call_storage(self.store, "release", job_id)
                raise
            except Exception:
                current = await call_storage(self.store, "get", job_id) or {}
                if current.get("status") == "pending" and int(current.get("attempts", 0)) >= self.max_attempts:
                    await call_storage(
                        self.store,
                        "fail",
                        job_id,
                        error=current.get("error", "S3 retry limit reached"),
                        retryable=False,
                        increment_attempt=False,
                    )
        return completed

    async def start(self) -> None:
        if self._runner is not None:
            return
        self._stopping = False
        self._runner = asyncio.create_task(self._run(), name="tracelet-s3-drain")

    async def _run(self) -> None:
        while not self._stopping:
            try:
                await self.drain_once()
                self.last_error = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = exc
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.poll_interval)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()

    async def stop(self) -> None:
        self._stopping = True
        self._wake.set()
        if self._runner is not None:
            self._runner.cancel()
            await asyncio.gather(self._runner, return_exceptions=True)
            self._runner = None
