from __future__ import annotations

import inspect
from typing import Any, Iterable

from .result import EvaluationResult


async def _call_evaluator(evaluator, event):
    target = getattr(evaluator, "evaluate", evaluator)
    value = target(event)
    if inspect.isawaitable(value):
        value = await value
    name = getattr(evaluator, "name", getattr(evaluator, "__name__", type(evaluator).__name__))
    version = str(getattr(evaluator, "version", "1"))
    if isinstance(value, EvaluationResult):
        return value
    if not isinstance(value, dict):
        raise TypeError("evaluator must return EvaluationResult or a score dictionary")
    return EvaluationResult(
        evaluator=name,
        evaluator_version=version,
        score_type=value["score_type"],
        score=value.get("score"),
        status=value.get("status", "completed"),
        details=value.get("details"),
        error=value.get("error"),
        label=value.get("label"),
        model=value.get("model"),
        usage=value.get("usage", {}),
        latency_ms=value.get("latency_ms"),
        entailment=value.get("entailment"),
    )


async def evaluate(event, evaluators: Iterable[Any]) -> list[EvaluationResult]:
    """Run independent evaluators, converting failures into error results."""
    results = []
    for evaluator in evaluators:
        name = getattr(evaluator, "name", getattr(evaluator, "__name__", type(evaluator).__name__))
        version = str(getattr(evaluator, "version", "1"))
        try:
            results.append(await _call_evaluator(evaluator, event))
        except Exception as exc:
            results.append(EvaluationResult.error_result(name, version, exc))
    return results


class Evaluator:
    name = "custom"
    version = "1"

    def evaluate(self, event):
        raise NotImplementedError
