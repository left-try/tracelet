from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from ..result import EvaluationResult


class DeterministicEvaluator:
    version = "1"
    name = "deterministic"

    def evaluate_output(self, output: Any, **kwargs) -> EvaluationResult:
        raise NotImplementedError

    def evaluate(self, event) -> EvaluationResult:
        return self.evaluate_output(event.output, context=event.context, metadata=event.metadata)

    def result(self, passed: bool, *, details: str | None = None, label: str | None = None):
        return EvaluationResult(
            evaluator=self.name,
            evaluator_version=self.version,
            score_type="boolean",
            score=passed,
            details=details,
            label=label,
        )


@dataclass
class JsonSchemaEvaluator(DeterministicEvaluator):
    schema: dict[str, Any]
    name = "json-schema"

    def evaluate_output(self, output, **kwargs):
        try:
            value = json.loads(output) if isinstance(output, str) else output
        except (TypeError, json.JSONDecodeError) as exc:
            return self.result(False, details=f"invalid JSON: {exc}")
        valid, details = _matches_schema(value, self.schema)
        return self.result(valid, details=details)


def _matches_schema(value, schema):
    if not isinstance(schema, dict):
        return False, "schema must be an object"
    expected = schema.get("type")
    checks = {
        "object": lambda v: isinstance(v, dict),
        "array": lambda v: isinstance(v, list),
        "string": lambda v: isinstance(v, str),
        "number": lambda v: type(v) in (int, float),
        "integer": lambda v: type(v) is int,
        "boolean": lambda v: type(v) is bool,
        "null": lambda v: v is None,
    }
    if expected in checks and not checks[expected](value):
        return False, f"expected {expected}"
    if expected == "object":
        missing = [key for key in schema.get("required", []) if key not in value]
        if missing:
            return False, f"missing required keys: {', '.join(missing)}"
        for key, child_schema in schema.get("properties", {}).items():
            if key in value:
                valid, reason = _matches_schema(value[key], child_schema)
                if not valid:
                    return False, f"{key}: {reason}"
    return True, "matches schema"


@dataclass
class RequiredTextEvaluator(DeterministicEvaluator):
    required: list[str]
    name = "required-text"

    def evaluate_output(self, output, **kwargs):
        text = str(output)
        normalized_text = text.casefold()
        missing = [item for item in self.required if item.casefold() not in normalized_text]
        return self.result(not missing, details="missing: " + ", ".join(missing) if missing else "all required text found")


@dataclass
class ForbiddenTextEvaluator(DeterministicEvaluator):
    forbidden: list[str]
    name = "forbidden-text"

    def evaluate_output(self, output, **kwargs):
        normalized_text = str(output).casefold()
        found = [item for item in self.forbidden if item.casefold() in normalized_text]
        return self.result(not found, details="forbidden text found: " + ", ".join(found) if found else "no forbidden text")


@dataclass
class RegexEvaluator(DeterministicEvaluator):
    pattern: str
    flags: int = 0
    name = "regex"

    def evaluate_output(self, output, **kwargs):
        return self.result(bool(re.search(self.pattern, str(output), self.flags)), details=f"pattern: {self.pattern}")


@dataclass
class LengthEvaluator(DeterministicEvaluator):
    min_chars: int = 0
    max_chars: int | None = None
    name = "length"

    def evaluate_output(self, output, **kwargs):
        size = len(str(output))
        valid = size >= self.min_chars and (self.max_chars is None or size <= self.max_chars)
        return self.result(valid, details=f"length={size}")


@dataclass
class RangeEvaluator(DeterministicEvaluator):
    minimum: float
    maximum: float
    name = "range"

    def evaluate_output(self, output, **kwargs):
        try:
            value = float(output)
        except (TypeError, ValueError):
            return self.result(False, details="output is not numeric")
        return self.result(self.minimum <= value <= self.maximum, details=f"value={value}")


@dataclass
class ExactMatchEvaluator(DeterministicEvaluator):
    expected: str
    name = "exact-match"

    def evaluate_output(self, output, **kwargs):
        return self.result(output == self.expected, details="exact comparison")


@dataclass
class EvidenceReferenceEvaluator(DeterministicEvaluator):
    name = "evidence-reference"
    _reference_pattern = re.compile(r"\[([^\]]+)\]")

    def evaluate_output(self, output, *, evidence_ids=None, **kwargs):
        if evidence_ids is None:
            context = kwargs.get("context") or []
            evidence_ids = {
                entry.get("source") or entry.get("id")
                for entry in context
                if isinstance(entry, dict) and (entry.get("source") or entry.get("id"))
            }
        citations = set(self._reference_pattern.findall(str(output)))
        valid = bool(citations) and citations.issubset(set(evidence_ids or set()))
        return self.result(valid, details="citation reference presence only; semantic entailment is not checked", label="reference_present" if valid else "reference_missing")
