from __future__ import annotations

from pathlib import Path
from typing import Iterable

from .event import Event
from .redaction import RedactionPolicy
from .storage.files import FileStore
from .storage.protocol import call_storage


class Tracelet:
    """Capture production events and queue configured evaluations."""

    def __init__(self, *, storage="file://./.tracelet", redaction: RedactionPolicy | None = None):
        if isinstance(storage, (str, Path)):
            raw = str(storage)
            if raw.startswith("file://"):
                raw = raw[7:]
            storage = FileStore(raw)
        self.storage = storage
        self.redaction = redaction

    def record_nowait(self, event: Event, *, job_id: str | None = None) -> str:
        if not isinstance(event, Event):
            raise TypeError("event must be an Event")
        identifier = job_id or event.request_id
        record = event.to_dict(redaction=self.redaction)
        self.storage.enqueue(job_id=identifier, payload=record)
        return identifier

    async def record(self, event: Event, *, evaluators: Iterable | None = None, job_id: str | None = None) -> str:
        if not isinstance(event, Event):
            raise TypeError("event must be an Event")
        identifier = job_id or event.request_id
        record = event.to_dict(redaction=self.redaction)
        payload = {"event": record}
        if evaluators is not None:
            payload["evaluators"] = [
                item if isinstance(item, str) else getattr(item, "name", getattr(item, "__name__", type(item).__name__))
                for item in evaluators
            ]
        await call_storage(self.storage, "enqueue", job_id=identifier, payload=payload)
        return identifier
