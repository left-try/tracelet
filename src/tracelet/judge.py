from __future__ import annotations

import inspect
import time
import asyncio
import math
import hashlib

from .evaluator import _call_evaluator, _restore_result
from .result import EvaluationResult


class LLMJudge:
    """Run a rubric against a user-supplied model callable."""

    def __init__(self, *, call_model, rubric: str, model: str | None = None, version: str = "1"):
        self.call_model = call_model
        self.rubric = rubric
        self.model = model
        self.name = "llm-judge"
        self.version = version

    async def evaluate(self, event):
        return await self._evaluate(event)

    async def evaluate_with_context(self, event, *, context):
        return await self._evaluate(event, idempotency_key=context.idempotency_key)

    async def _evaluate(self, event, *, idempotency_key=None):
        started = time.perf_counter()
        request = {
            "rubric": self.rubric,
            "input": event.input,
            "output": event.output,
            "context": event.context,
            "request_id": event.request_id,
        }
        if idempotency_key is not None:
            request["idempotency_key"] = idempotency_key
        try:
            response = self.call_model(request)
            if inspect.isawaitable(response):
                response = await response
            if not isinstance(response, dict):
                raise ValueError("judge response must be an object")
            if response.get("refusal"):
                raise ValueError(f"judge refusal: {response['refusal']}")
            if "score" not in response:
                raise ValueError("judge response is missing score")
            score = response["score"]
            if type(score) is bool:
                score_type = "boolean"
            elif type(score) in (int, float):
                score_type = "numeric"
            elif isinstance(score, str):
                score_type = "categorical" if response.get("label") else "text"
            else:
                raise ValueError("judge score must be boolean, numeric, or text")
            return EvaluationResult(
                evaluator=self.name,
                evaluator_version=self.version,
                score_type=score_type,
                score=score,
                status="completed",
                details=response.get("reason") or response.get("details"),
                label=response.get("label"),
                model=self.model or response.get("model"),
                usage=response.get("usage", {}),
                latency_ms=(time.perf_counter() - started) * 1000,
            )
        except Exception as exc:
            return EvaluationResult.error_result(self.name, self.version, exc)


class EvaluationPipeline:
    """Run deterministic checks, then optional cheap and strong judges."""

    def __init__(
        self,
        *,
        deterministic=(),
        cheap_judge=None,
        judge=None,
        short_circuit: bool = False,
        escalate_on=("uncertain", "unknown"),
        sample_rate: float = 1.0,
        max_judge_calls: int | None = None,
        max_judge_cost: float | None = None,
        max_concurrent_judges: int = 4,
    ):
        if not 0 <= sample_rate <= 1:
            raise ValueError("sample_rate must be between zero and one")
        if max_judge_calls is not None and max_judge_calls < 0:
            raise ValueError("max_judge_calls cannot be negative")
        if max_judge_cost is not None and (not math.isfinite(max_judge_cost) or max_judge_cost < 0):
            raise ValueError("max_judge_cost must be finite and non-negative")
        if max_concurrent_judges < 1:
            raise ValueError("max_concurrent_judges must be positive")
        self.deterministic = list(deterministic)
        self.cheap_judge = cheap_judge
        self.judge = judge
        self.short_circuit = short_circuit
        self.escalate_on = set(escalate_on)
        self.sample_rate = sample_rate
        self.max_judge_calls = max_judge_calls
        self.max_judge_cost = max_judge_cost
        self._judge_calls = 0
        self._judge_cost = 0.0
        self._judge_lock = asyncio.Lock()
        self._judge_semaphore = asyncio.Semaphore(max_concurrent_judges)

    async def _evaluate(self, evaluator, event):
        return await _call_evaluator(evaluator, event)

    async def _evaluate_slot(self, evaluator, event, context, slot_id, *, errors_as_results=False):
        if context is None:
            try:
                return await _call_evaluator(evaluator, event)
            except Exception as exc:
                if errors_as_results:
                    name = getattr(evaluator, "name", getattr(evaluator, "__name__", type(evaluator).__name__))
                    version = str(getattr(evaluator, "version", "1"))
                    return EvaluationResult.error_result(name, version, exc)
                raise
        slot = context.for_slot(slot_id)
        cached = await slot.load_checkpoint()
        if cached is not None:
            return _restore_result(cached)
        try:
            result = await _call_evaluator(evaluator, event, context=slot)
        except Exception as exc:
            if not errors_as_results:
                raise
            name = getattr(evaluator, "name", getattr(evaluator, "__name__", type(evaluator).__name__))
            version = str(getattr(evaluator, "version", "1"))
            result = EvaluationResult.error_result(name, version, exc)
        await slot.save_checkpoint(result.to_dict() if isinstance(result, EvaluationResult) else result)
        return result

    async def _judge_result(self, evaluator, event, *, context=None, slot_id="judge"):
        name = getattr(evaluator, "name", "judge")
        version = str(getattr(evaluator, "version", "1"))
        slot = context.for_slot(slot_id) if context is not None else None
        if slot is not None:
            cached = await slot.load_checkpoint()
            if cached is not None:
                return _restore_result(cached)
        async with self._judge_semaphore:
            # When a cost ceiling is active, serialize judge calls so a burst
            # cannot pass the same remaining-budget check concurrently.
            lock = self._judge_lock if self.max_judge_cost is not None else _NullAsyncLock()
            async with lock:
                if self.max_judge_calls is not None and self._judge_calls >= self.max_judge_calls:
                    result = EvaluationResult.skipped_result(name, version, "max_judge_calls reached")
                    if slot is not None:
                        await slot.save_checkpoint(result.to_dict())
                    return result
                if self.max_judge_cost is not None and self._judge_cost >= self.max_judge_cost:
                    result = EvaluationResult.skipped_result(name, version, "max_judge_cost reached")
                    if slot is not None:
                        await slot.save_checkpoint(result.to_dict())
                    return result
                self._judge_calls += 1
                try:
                    result = await (_call_evaluator(evaluator, event, context=slot)
                                   if slot is not None else self._evaluate(evaluator, event))
                except Exception as exc:
                    result = EvaluationResult.error_result(name, version, exc)
                if slot is not None:
                    await slot.save_checkpoint(result.to_dict())
                cost = result.usage.get("cost_usd", result.usage.get("cost", 0)) if result.usage else 0
                if isinstance(cost, (int, float)) and not isinstance(cost, bool) and math.isfinite(cost) and cost > 0:
                    self._judge_cost += float(cost)
                return result

    async def run(self, event):
        return await self._run(event, context=None)

    async def evaluate_with_context(self, event, *, context):
        return await self._run(event, context=context)

    async def _run(self, event, *, context):
        sample_identity = (context.idempotency_key if context is not None else
                           str(getattr(event, "request_id", repr(event))))
        sample_value = int(hashlib.sha256(sample_identity.encode("utf-8")).hexdigest()[:16], 16) / 2**64
        if self.sample_rate < 1 and sample_value >= self.sample_rate:
            return [EvaluationResult.skipped_result("evaluation-pipeline", "1", "excluded by sampling")]
        results = []
        for index, evaluator in enumerate(self.deterministic):
            name = getattr(evaluator, "name", getattr(evaluator, "__name__", type(evaluator).__name__))
            version = str(getattr(evaluator, "version", "1"))
            result = await self._evaluate_slot(
                evaluator, event, context, f"deterministic:{index}:{name}:{version}"
            )
            results.append(result)
            if self.short_circuit and result.status == "completed" and result.score is False:
                return results
        if self.cheap_judge is not None:
            name = getattr(self.cheap_judge, "name", getattr(self.cheap_judge, "__name__", "cheap-judge"))
            version = str(getattr(self.cheap_judge, "version", "1"))
            cheap_result = await self._judge_result(
                self.cheap_judge, event, context=context,
                slot_id=f"cheap-judge:{name}:{version}",
            )
            results.append(cheap_result)
            if cheap_result.status != "completed" or cheap_result.score not in self.escalate_on:
                return results
        if self.judge is not None:
            name = getattr(self.judge, "name", getattr(self.judge, "__name__", "judge"))
            version = str(getattr(self.judge, "version", "1"))
            results.append(await self._judge_result(
                self.judge, event, context=context, slot_id=f"judge:{name}:{version}"
            ))
        return results


class _NullAsyncLock:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False
