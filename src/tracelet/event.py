from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .errors import ValidationError


def _json_value(value: Any) -> Any:
    """Validate JSON compatibility without coercing arbitrary objects."""
    try:
        json.dumps(value, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"value is not JSON serializable: {exc}") from exc
    return value


@dataclass(frozen=True)
class Event:
    request_id: str
    input: Any
    output: Any
    context: Any = None
    model: str | None = None
    prompt_version: str | None = None
    timestamp: str | None = None
    tags: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
    schema_version: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.request_id, str) or not self.request_id.strip():
            raise ValidationError("request_id must be a non-empty string")
        if self.input is None:
            raise ValidationError("input is required")
        if self.output is None:
            raise ValidationError("output is required")
        if self.schema_version != 1:
            raise ValidationError("unsupported event schema_version")
        if not isinstance(self.metadata, dict):
            raise ValidationError("metadata must be a dictionary")
        if not self.timestamp:
            object.__setattr__(self, "timestamp", datetime.now(timezone.utc).isoformat())

    def to_dict(self, *, redaction=None) -> dict[str, Any]:
        record = {
            "schema_version": self.schema_version,
            "request_id": self.request_id,
            "timestamp": self.timestamp,
            "input": self.input,
            "output": self.output,
            "context": self.context,
            "model": self.model,
            "prompt_version": self.prompt_version,
            "tags": list(self.tags),
            "metadata": self.metadata,
        }
        if redaction is not None:
            record = redaction.apply(record)
        _json_value(record)
        return record

    def to_json(self, *, redaction=None) -> str:
        return json.dumps(self.to_dict(redaction=redaction), ensure_ascii=False, allow_nan=False)

    @classmethod
    def from_dict(cls, record: dict[str, Any]) -> "Event":
        return cls(
            request_id=record["request_id"],
            input=record["input"],
            output=record["output"],
            context=record.get("context"),
            model=record.get("model"),
            prompt_version=record.get("prompt_version"),
            timestamp=record.get("timestamp"),
            tags=tuple(record.get("tags", ())),
            metadata=record.get("metadata", {}),
            schema_version=record.get("schema_version", 1),
        )

    @classmethod
    def from_json(cls, value: str) -> "Event":
        try:
            record = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValidationError("event JSON is invalid") from exc
        if not isinstance(record, dict):
            raise ValidationError("event JSON must contain an object")
        return cls.from_dict(record)
