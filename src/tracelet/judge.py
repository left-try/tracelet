from __future__ import annotations

import inspect
import time
import asyncio
import math

from .evaluator import _call_evaluator
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
        started = time.perf_counter()
        request = {
            "rubric": self.rubric,
            "input": event.input,
            "output": event.output,
            "context": event.context,
            "request_id": event.request_id,
        }
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

    async def _judge_result(self, evaluator, event):
        name = getattr(evaluator, "name", "judge")
        version = str(getattr(evaluator, "version", "1"))
        async with self._judge_semaphore:
            # When a cost ceiling is active, serialize judge calls so a burst
            # cannot pass the same remaining-budget check concurrently.
            lock = self._judge_lock if self.max_judge_cost is not None else _NullAsyncLock()
            async with lock:
                if self.max_judge_calls is not None and self._judge_calls >= self.max_judge_calls:
                    return EvaluationResult.skipped_result(name, version, "max_judge_calls reached")
                if self.max_judge_cost is not None and self._judge_cost >= self.max_judge_cost:
                    return EvaluationResult.skipped_result(name, version, "max_judge_cost reached")
                self._judge_calls += 1
                try:
                    result = await self._evaluate(evaluator, event)
                except Exception as exc:
                    result = EvaluationResult.error_result(name, version, exc)
                cost = result.usage.get("cost_usd", result.usage.get("cost", 0)) if result.usage else 0
                if isinstance(cost, (int, float)) and not isinstance(cost, bool) and math.isfinite(cost) and cost > 0:
                    self._judge_cost += float(cost)
                return result

    async def run(self, event):
        import random

        if self.sample_rate < 1 and random.random() >= self.sample_rate:
            return [EvaluationResult.skipped_result("evaluation-pipeline", "1", "excluded by sampling")]
        results = []
        for evaluator in self.deterministic:
            result = await self._evaluate(evaluator, event)
            results.append(result)
            if self.short_circuit and result.status == "completed" and result.score is False:
                return results
        if self.cheap_judge is not None:
            cheap_result = await self._judge_result(self.cheap_judge, event)
            results.append(cheap_result)
            if cheap_result.status != "completed" or cheap_result.score not in self.escalate_on:
                return results
        if self.judge is not None:
            results.append(await self._judge_result(self.judge, event))
        return results


class _NullAsyncLock:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False
