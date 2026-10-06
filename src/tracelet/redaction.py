from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, is_dataclass, asdict
import re
from typing import Any


_BUILTIN_PATTERNS = (
    # Private key material is unambiguous and may span multiple lines.
    re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z0-9 ]*PRIVATE KEY-----"),
    # Provider formats with recognizable prefixes and sufficient token length.
    re.compile(r"\b(?:sk-(?:proj|svcacct)-|sk-)[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b"),
    re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b"),
    re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{12,}"),
    # Labelled assignments are strong credential signals; keep ordinary prose intact.
    re.compile(
        r"(?i)\b(?:api[_-]?key|access[_-]?token|auth[_-]?token|client[_-]?secret|password|secret)"
        r"\s*[:=]\s*(?:['\"])?[^,\s'\"]{8,}(?:['\"])?"
    ),
)


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
    detect_secrets: bool = True
    custom_patterns: tuple[str, ...] = ()

    def _patterns(self):
        patterns = list(_BUILTIN_PATTERNS) if self.detect_secrets else []
        patterns.extend(re.compile(pattern) for pattern in self.custom_patterns)
        return patterns

    def _sanitize(self, value: Any, patterns) -> Any:
        if isinstance(value, str):
            for pattern in patterns:
                value = pattern.sub(self.replacement, value)
            return value
        if isinstance(value, dict):
            return {key: self._sanitize(item, patterns) for key, item in value.items()}
        if isinstance(value, list):
            return [self._sanitize(item, patterns) for item in value]
        if isinstance(value, tuple):
            return tuple(self._sanitize(item, patterns) for item in value)
        return deepcopy(value)

    def sanitize(self, value: Any) -> Any:
        """Return a sanitized copy of a JSON-compatible value."""
        try:
            return self._sanitize(value, self._patterns())
        except Exception:
            raise RuntimeError("secret redaction failed; value was not persisted") from None

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
        return self.sanitize(record)
