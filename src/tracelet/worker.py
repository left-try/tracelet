from __future__ import annotations

import asyncio
import inspect
import time
import uuid
from dataclasses import asdict, is_dataclass
from types import SimpleNamespace
from typing import Any, Callable

from .errors import WorkerClosedError
from .event import Event
from .evaluator import evaluate
from .storage.protocol import call_storage


class RetryPolicy:
    def __init__(self, *, max_attempts: int = 3, initial_delay: float = 0.1, max_delay: float = 5.0):
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.max_delay = max_delay

    async def run(self, operation: Callable[[], Any]):
        delay = self.initial_delay
        for attempt in range(1, self.max_attempts + 1):
            try:
                result = operation()
                return await result if inspect.isawaitable(result) else result
            except Exception:
                if attempt == self.max_attempts:
                    raise
                if delay > 0:
                    await asyncio.sleep(delay)
                delay = min(max(delay * 2, self.initial_delay), self.max_delay)


class EvaluationWorker:
    def __init__(
        self,
        *,
        store,
        evaluator=None,
        evaluators: dict[str, Any] | None = None,
        candidate_runner=None,
        pairwise_evaluator=None,
        archive_store=None,
        max_concurrency: int = 4,
        max_attempts: int = 3,
        poll_interval: float = 0.05,
        timeout: float | None = None,
    ):
        if evaluator is None and not evaluators:
            raise ValueError("provide evaluator or a non-empty evaluator registry")
        if archive_store is not None:
            store_root = getattr(store, "root", None)
            archive_root = getattr(archive_store, "root", None)
            if store is archive_store or (store_root is not None and store_root == archive_root):
                raise ValueError("archive_store must be separate from the evaluation store")
            if not callable(getattr(store, "checkpoint", None)):
                raise TypeError("evaluation store must implement checkpoint() when archive_store is configured")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be at least one")
        self.store = store
        self.evaluator = evaluator
        self.evaluators = dict(evaluators or {})
        self.candidate_runner = candidate_runner
        self.pairwise_evaluator = pairwise_evaluator
        self.archive_store = archive_store
        self.max_concurrency = max_concurrency
        self.max_attempts = max_attempts
        self.poll_interval = max(0.001, poll_interval)
        self.timeout = timeout
        self.worker_id = str(uuid.uuid4())
        self._runner: asyncio.Task | None = None
        self._tasks: set[asyncio.Task] = set()
        self._stopping = False
        self._started = False
        self._wake = asyncio.Event()
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._scheduling = False

    async def start(self) -> None:
        if self._started:
            return
        self._stopping = False
        self._started = True
        self._runner = asyncio.create_task(self._run_loop(), name="tracelet-worker")

    async def enqueue(self, event: Event | dict[str, Any], *, job_id: str | None = None) -> str:
        if self._stopping:
            raise WorkerClosedError("worker has stopped")
        if isinstance(event, Event):
            payload = {"event": event.to_dict()}
            job_id = job_id or event.request_id
        else:
            payload = event
            job_id = job_id or event.get("request_id") or str(uuid.uuid4())
        await call_storage(self.store, "enqueue", job_id=job_id, payload=payload)
        self._wake.set()
        return job_id

    async def _run_loop(self) -> None:
        while not self._stopping:
            scheduled = False
            if len(self._tasks) < self.max_concurrency:
                self._scheduling = True
                try:
                    pending = await call_storage(self.store, "list_pending")
                    for job in pending:
                        if len(self._tasks) >= self.max_concurrency:
                            break
                        job_id = job["job_id"]
                        if not await call_storage(self.store, "claim", job_id, worker_id=self.worker_id):
                            continue
                        task = asyncio.create_task(self._process(job_id), name=f"tracelet:{job_id}")
                        self._tasks.add(task)
                        task.add_done_callback(self._tasks.discard)
                        scheduled = True
                finally:
                    self._scheduling = False
            if not scheduled:
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=self.poll_interval)
                except asyncio.TimeoutError:
                    pass

    async def _process(self, job_id: str) -> None:
        async with self._semaphore:
            try:
                job = await call_storage(self.store, "get", job_id)
                payload = job["payload"]
                raw_event = payload.get("event", payload) if isinstance(payload, dict) else payload
                event = self._event(raw_event)
                if "result_checkpoint" in job:
                    serialized_result = job["result_checkpoint"]
                else:
                    if "evaluators" in payload:
                        selected = []
                        for name in payload["evaluators"]:
                            try:
                                selected.append(self.evaluators[name])
                            except KeyError as exc:
                                raise ValueError(f"evaluator {name!r} is not registered") from exc
                        call = evaluate(event, selected)
                    elif self.evaluator is None:
                        call = evaluate(event, self.evaluators.values())
                    else:
                        call = self.evaluator(event)
                    if self.timeout is not None:
                        result = await asyncio.wait_for(call, timeout=self.timeout) if inspect.isawaitable(call) else call
                    else:
                        result = await call if inspect.isawaitable(call) else call
                    if self.candidate_runner is not None:
                        comparison = await self.candidate_runner.run(event)
                        pairwise = []
                        if self.pairwise_evaluator is not None:
                            for candidate in comparison.candidates:
                                if candidate.status != "completed":
                                    continue
                                verdict = await self.pairwise_evaluator.compare(
                                    input=event.input,
                                    candidate_a=comparison.baseline.output,
                                    candidate_b=candidate.output,
                                )
                                pairwise.append({"candidate": candidate.model, **asdict(verdict)})
                        result = {
                            "evaluations": result,
                            "comparison": comparison,
                            "pairwise": pairwise,
                        }
                    serialized_result = _serialize_result(result)
                if self.archive_store is not None:
                    if "result_checkpoint" not in job:
                        await call_storage(self.store, "checkpoint", job_id, result=serialized_result)
                    await call_storage(
                        self.archive_store,
                        "enqueue",
                        job_id=f"tracelet-eval:{job_id}",
                        payload={
                            "schema_version": 1,
                            "source_job_id": job_id,
                            "request_id": getattr(event, "request_id", job_id),
                            "event": _serialize_result(raw_event),
                            "result": serialized_result,
                        },
                    )
                await call_storage(self.store, "complete", job_id, result=serialized_result)
            except asyncio.CancelledError:
                await call_storage(self.store, "release", job_id)
                raise
            except Exception as exc:
                job = await call_storage(self.store, "get", job_id) or {}
                attempts = int(job.get("attempts", 0)) + 1
                if attempts < self.max_attempts:
                    await call_storage(self.store, "retry", job_id, error=str(exc))
                    await asyncio.sleep(min(0.01 * (2 ** max(0, attempts - 1)), 0.5))
                else:
                    await call_storage(self.store, "fail", job_id, error=str(exc), retryable=False)

    @staticmethod
    def _event(raw):
        if isinstance(raw, Event):
            return raw
        if isinstance(raw, dict) and {"request_id", "input", "output"}.issubset(raw):
            return Event.from_dict(raw)
        if isinstance(raw, dict):
            return SimpleNamespace(**raw)
        return raw

    async def stop(self, *, drain_timeout: float = 5.0) -> None:
        if not self._started:
            self._stopping = True
            return
        self._stopping = True
        self._wake.set()
        if self._runner:
            self._runner.cancel()
            await asyncio.gather(self._runner, return_exceptions=True)
            self._runner = None
        tasks = list(self._tasks)
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=max(0.0, drain_timeout))
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            completed = [task for task in tasks if task not in pending]
            if completed:
                await asyncio.gather(*completed, return_exceptions=True)
        self._started = False

    async def wait_idle(self, *, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not self._scheduling and not self._tasks and not await call_storage(self.store, "list_pending"):
                return
            await asyncio.sleep(self.poll_interval)
        raise TimeoutError("worker did not become idle")


def _serialize_result(value):
    """Convert Tracelet results nested in common containers to JSON records."""
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _serialize_result(value.to_dict())
    if isinstance(value, dict):
        return {key: _serialize_result(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_result(item) for item in value]
    if is_dataclass(value):
        return _serialize_result(asdict(value))
    return value
