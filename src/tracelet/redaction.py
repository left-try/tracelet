from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, is_dataclass, asdict
from typing import Any


def _path_parent(record: dict[str, Any], dotted_path: str):
    parts = dotted_path.split(".")
    current: Any = record
    for part in parts[:-1]:
        if not isinstance(current, dict) or part not in current:
            return None, None
        current = current[part]
    return (current, parts[-1]) if isinstance(current, dict) else (None, None)


@dataclass(frozen=True)
class RedactionPolicy:
    redact_fields: set[str] = frozenset()
    exclude_fields: set[str] = frozenset()
    replacement: str = "[REDACTED]"

    def apply(self, value: Any) -> dict[str, Any]:
        if is_dataclass(value):
            record = asdict(value)
        elif hasattr(value, "to_dict"):
            record = value.to_dict()
        else:
            record = deepcopy(value)
        if not isinstance(record, dict):
            raise TypeError("redaction requires an object record")
        record = deepcopy(record)
        for dotted_path in self.redact_fields:
            parent, key = _path_parent(record, dotted_path)
            if parent is not None and key in parent:
                parent[key] = self.replacement
        for dotted_path in self.exclude_fields:
            parent, key = _path_parent(record, dotted_path)
            if parent is not None:
                parent.pop(key, None)
        return record
