"""Context supplied to evaluators that support idempotent execution."""
from __future__ import annotations

import inspect
from typing import Any, Callable


class EvaluationContext:
    """Stable idempotency key and per-slot result checkpoint access."""

    def __init__(
        self,
        idempotency_key: str,
        *,
        load_checkpoint: Callable[[], Any] | None = None,
        save_checkpoint: Callable[[Any], Any] | None = None,
        slot_factory: Callable[[str], "EvaluationContext"] | None = None,
    ):
        if not isinstance(idempotency_key, str) or not idempotency_key:
            raise ValueError("idempotency_key must be a non-empty string")
        self.idempotency_key = idempotency_key
        self._load = load_checkpoint
        self._save = save_checkpoint
        self._slot_factory = slot_factory

    async def load_checkpoint(self) -> Any | None:
        if self._load is None:
            return None
        result = self._load()
        return await result if inspect.isawaitable(result) else result

    async def save_checkpoint(self, result: Any) -> None:
        if self._save is None:
            raise RuntimeError("this evaluation context cannot persist checkpoints")
        value = self._save(result)
        if inspect.isawaitable(value):
            await value

    def for_slot(self, slot: str) -> "EvaluationContext":
        if self._slot_factory is None:
            raise RuntimeError("this evaluation context cannot create evaluator slots")
        return self._slot_factory(slot)
