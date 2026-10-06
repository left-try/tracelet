from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import time
import uuid
from dataclasses import asdict, is_dataclass
from types import SimpleNamespace
from typing import Any, Callable

from .errors import WorkerClosedError
from .context import EvaluationContext
from .event import Event
from .evaluator import evaluate, evaluate_with_context
from .redaction import RedactionPolicy
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
        redaction: RedactionPolicy | None = None,
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
        self.redaction = redaction or RedactionPolicy()
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
            payload = {"event": event.to_dict(redaction=self.redaction)}
            job_id = job_id or event.request_id
        else:
            payload = self.redaction.sanitize(event)
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
                raw_event = self.redaction.sanitize(raw_event)
                event = self._event(raw_event)
                if "result_checkpoint" in job:
                    serialized_result = job["result_checkpoint"]
                else:
                    contextual_target = self._contextual_target(self.evaluator)
                    context = None
                    cached_context_result = None
                    if "evaluators" in payload:
                        selected = []
                        for name in payload["evaluators"]:
                            try:
                                selected.append(self.evaluators[name])
                            except KeyError as exc:
                                raise ValueError(f"evaluator {name!r} is not registered") from exc
                        self._require_evaluator_checkpoint_store()
                        call = evaluate_with_context(event, selected, self._evaluation_context(job_id, "registry"))
                    elif self.evaluator is None:
                        self._require_evaluator_checkpoint_store()
                        call = evaluate_with_context(
                            event, self.evaluators.values(), self._evaluation_context(job_id, "registry")
                        )
                    elif contextual_target is not None:
                        self._require_evaluator_checkpoint_store()
                        context = self._evaluation_context(job_id, "root")
                        cached_context_result = await context.load_checkpoint()
                        call = None if cached_context_result is not None else contextual_target(event, context=context)
                    else:
                        call = self.evaluator(event)
                    if cached_context_result is not None:
                        result = cached_context_result
                    elif self.timeout is not None:
                        result = await asyncio.wait_for(call, timeout=self.timeout) if inspect.isawaitable(call) else call
                    else:
                        result = await call if inspect.isawaitable(call) else call
                    if context is not None and cached_context_result is None:
                        await context.save_checkpoint(_serialize_result(result))
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
                    serialized_result = self.redaction.sanitize(_serialize_result(result))
                    await call_storage(self.store, "checkpoint", job_id, result=serialized_result)
                if self.archive_store is not None:
                    archive_payload = self.redaction.sanitize({
                        "schema_version": 1,
                        "source_job_id": job_id,
                        "request_id": getattr(event, "request_id", job_id),
                        "event": _serialize_result(raw_event),
                        "result": serialized_result,
                    })
                    await call_storage(
                        self.archive_store,
                        "enqueue",
                        job_id=f"tracelet-eval:{job_id}",
                        payload=archive_payload,
                    )
                await call_storage(self.store, "complete", job_id, result=serialized_result)
            except asyncio.CancelledError:
                await call_storage(self.store, "release", job_id)
                raise
            except Exception as exc:
                try:
                    error = self.redaction.sanitize(str(exc))
                except Exception:
                    error = "evaluation failed; redaction unavailable"
                job = await call_storage(self.store, "get", job_id) or {}
                attempts = int(job.get("attempts", 0)) + 1
                if attempts < self.max_attempts:
                    await call_storage(self.store, "retry", job_id, error=error)
                    await asyncio.sleep(min(0.01 * (2 ** max(0, attempts - 1)), 0.5))
                else:
                    await call_storage(self.store, "fail", job_id, error=error, retryable=False)

    @staticmethod
    def _contextual_target(evaluator):
        target = evaluator
        method = getattr(target, "evaluate_with_context", None)
        if callable(method):
            return method
        owner = getattr(evaluator, "__self__", None)
        method = getattr(owner, "evaluate_with_context", None)
        return method if callable(method) else None

    def _require_evaluator_checkpoint_store(self):
        missing = [
            name for name in ("get_evaluator_checkpoints", "checkpoint_evaluator")
            if not callable(getattr(self.store, name, None))
        ]
        if missing:
            raise TypeError(
                "context-aware evaluation requires storage methods: " + ", ".join(missing)
            )

    def _evaluation_context(self, job_id: str, slot_path: str) -> EvaluationContext:
        key = hashlib.sha256(
            json.dumps(["tracelet-evaluator-v1", job_id, slot_path], ensure_ascii=False,
                       separators=(",", ":")).encode("utf-8")
        ).hexdigest()

        async def load_checkpoint():
            checkpoints = await call_storage(self.store, "get_evaluator_checkpoints", job_id)
            return checkpoints.get(key)

        async def save_checkpoint(result):
            await call_storage(
                self.store, "checkpoint_evaluator", job_id, key,
                result=self.redaction.sanitize(_serialize_result(result)),
            )

        return EvaluationContext(
            key,
            load_checkpoint=load_checkpoint,
            save_checkpoint=save_checkpoint,
            slot_factory=lambda child: self._evaluation_context(job_id, f"{slot_path}/{child}"),
        )

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
