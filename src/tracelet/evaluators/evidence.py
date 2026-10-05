from __future__ import annotations

import inspect
import time

from ..result import EvaluationResult


class EvidenceEvaluator:
    """Adapter for a user-supplied entailment/NLI callable."""

    name = "evidence-entailment"
    version = "1"

    def __init__(self, *, classify):
        self.classify = classify

    async def evaluate(self, event=None, *, answer=None, evidence=None):
        if event is not None:
            answer = event.output
            evidence = event.context
        if answer is None or evidence is None:
            raise ValueError("evidence evaluation requires answer and evidence")
        started = time.perf_counter()
        verdict = self.classify(answer, evidence)
        if inspect.isawaitable(verdict):
            verdict = await verdict
        if verdict not in {"entailment", "contradiction", "unknown"}:
            raise ValueError("evidence classifier must return entailment, contradiction, or unknown")
        return EvaluationResult(
            evaluator=self.name,
            evaluator_version=self.version,
            score_type="categorical",
            score=verdict,
            label=verdict,
            entailment=verdict,
            latency_ms=(time.perf_counter() - started) * 1000,
        )
