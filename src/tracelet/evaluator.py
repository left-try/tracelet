from __future__ import annotations

import inspect
from typing import Any, Iterable

from .result import EvaluationResult


async def _call_evaluator(evaluator, event, *, context=None):
    contextual = getattr(evaluator, "evaluate_with_context", None)
    if context is not None and callable(contextual):
        value = contextual(event, context=context)
    else:
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


async def evaluate_with_context(event, evaluators: Iterable[Any], context) -> list[EvaluationResult]:
    """Run registry evaluators with per-slot restore and checkpoint behavior."""
    results = []
    for index, evaluator in enumerate(evaluators):
        name = getattr(evaluator, "name", getattr(evaluator, "__name__", type(evaluator).__name__))
        version = str(getattr(evaluator, "version", "1"))
        slot = context.for_slot(f"evaluator:{index}:{name}:{version}")
        cached = await slot.load_checkpoint()
        if cached is not None:
            results.append(_restore_result(cached))
            continue
        try:
            result = await _call_evaluator(evaluator, event, context=slot)
        except Exception as exc:
            result = EvaluationResult.error_result(name, version, exc)
        await slot.save_checkpoint(result.to_dict())
        results.append(result)
    return results


def _restore_result(value):
    if isinstance(value, EvaluationResult):
        return value
    if isinstance(value, dict) and {"evaluator", "evaluator_version", "score_type", "score"}.issubset(value):
        return EvaluationResult(**value)
    return value


class Evaluator:
    name = "custom"
    version = "1"

    def evaluate(self, event):
        raise NotImplementedError
