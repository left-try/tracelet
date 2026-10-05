from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from .errors import ValidationError

ScoreType = Literal["boolean", "numeric", "categorical", "text"]
ResultStatus = Literal["completed", "error", "skipped"]


@dataclass
class EvaluationResult:
    evaluator: str
    evaluator_version: str
    score_type: ScoreType
    score: bool | float | int | str | None
    status: ResultStatus = "completed"
    details: str | None = None
    error: str | None = None
    label: str | None = None
    model: str | None = None
    usage: dict[str, Any] = field(default_factory=dict)
    latency_ms: float | None = None
    entailment: str | None = None

    def __post_init__(self) -> None:
        if not self.evaluator or not self.evaluator_version:
            raise ValidationError("evaluator and evaluator_version are required")
        if self.score_type not in {"boolean", "numeric", "categorical", "text"}:
            raise ValidationError("unsupported score_type")
        if self.status not in {"completed", "error", "skipped"}:
            raise ValidationError("unsupported result status")
        if self.status == "completed":
            valid = {
                "boolean": lambda v: type(v) is bool,
                "numeric": lambda v: type(v) in (int, float) and v == v,
                "categorical": lambda v: isinstance(v, str),
                "text": lambda v: isinstance(v, str),
            }[self.score_type]
            if not valid(self.score):
                raise ValidationError(f"score must match score_type={self.score_type}")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def error_result(cls, evaluator: str, version: str, error: BaseException | str) -> "EvaluationResult":
        return cls(
            evaluator=evaluator,
            evaluator_version=version,
            score_type="text",
            score=None,
            status="error",
            error=str(error),
        )

    @classmethod
    def skipped_result(cls, evaluator: str, version: str, reason: str) -> "EvaluationResult":
        return cls(
            evaluator=evaluator,
            evaluator_version=version,
            score_type="text",
            score=None,
            status="skipped",
            details=reason,
        )
