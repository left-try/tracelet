from __future__ import annotations

import inspect
import random
import asyncio
from copy import deepcopy
from dataclasses import dataclass


@dataclass
class CandidateOutput:
    model: str
    output: str | None
    status: str = "completed"
    error: str | None = None


@dataclass
class Comparison:
    request_id: str
    baseline: CandidateOutput
    candidates: list[CandidateOutput]


class CandidateRunner:
    def __init__(self, *, candidate=None, candidates=None, sample_rate: float = 1.0, timeout: float | None = None):
        if not 0 <= sample_rate <= 1:
            raise ValueError("sample_rate must be between zero and one")
        self.candidates = candidates or ({"candidate": candidate} if candidate is not None else {})
        self.sample_rate = sample_rate
        self.timeout = timeout

    async def run(self, event):
        request_id = event.request_id
        baseline_model = getattr(event, "model", None) or "baseline"
        baseline = CandidateOutput(model=baseline_model, output=event.output)
        if self.sample_rate < 1 and random.random() >= self.sample_rate:
            return Comparison(
                request_id=request_id,
                baseline=baseline,
                candidates=[CandidateOutput(model=name, output=None, status="skipped") for name in self.candidates],
            )
        results = []
        for name, callback in self.candidates.items():
            snapshot = deepcopy(event)
            try:
                output = callback(snapshot)
                if inspect.isawaitable(output):
                    output = await asyncio.wait_for(output, timeout=self.timeout) if self.timeout else await output
                results.append(CandidateOutput(model=name, output=output))
            except Exception as exc:
                results.append(CandidateOutput(model=name, output=None, status="error", error=str(exc)))
        return Comparison(request_id=request_id, baseline=baseline, candidates=results)


@dataclass
class PairwiseResult:
    winner: str
    rationale: str | None
    verdicts: list[str]
    model: str | None = None


class PairwiseEvaluator:
    def __init__(self, *, judge, swap_order: bool = False, model: str | None = None):
        self.judge = judge
        self.swap_order = swap_order
        self.model = model

    async def compare(self, *, input, candidate_a, candidate_b):
        async def call(a, b):
            response = self.judge({"input": input, "candidate_a": a, "candidate_b": b})
            if inspect.isawaitable(response):
                response = await response
            winner = response.get("winner")
            if winner not in {"A", "B", "tie"}:
                raise ValueError("pairwise judge must return winner A, B, or tie")
            return winner, response.get("rationale")

        first, rationale = await call(candidate_a, candidate_b)
        verdicts = [first]
        normalized = [first]
        if self.swap_order:
            second, _ = await call(candidate_b, candidate_a)
            verdicts.append(second)
            normalized.append({"A": "B", "B": "A", "tie": "tie"}[second])
        winner = normalized[0] if all(value == normalized[0] for value in normalized) else "tie"
        return PairwiseResult(winner=winner, rationale=rationale, verdicts=verdicts, model=self.model)
