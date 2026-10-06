from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import hashlib
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote

from ..errors import StorageError


class FileStore:
    """Atomic file-backed outbox for a single host and persistent volume."""

    _states = ("pending", "claimed", "completed", "failed")

    def __init__(self, path: str | Path):
        self.root = Path(path).expanduser().resolve()
        self._lock = threading.RLock()
        for state in self._states:
            (self.root / state).mkdir(parents=True, exist_ok=True)
        self._recover_claims()

    def _recover_claims(self) -> None:
        for source in (self.root / "claimed").glob("*.json"):
            destination = self.root / "pending" / source.name
            try:
                os.replace(source, destination)
            except FileNotFoundError:
                pass

    def _path(self, state: str, job_id: str) -> Path:
        raw_id = str(job_id)
        safe_id = quote(raw_id, safe="-_.")
        if len(safe_id) > 180:
            suffix = hashlib.sha256(raw_id.encode("utf-8")).hexdigest()
            safe_id = f"{safe_id[:110]}-{suffix}"
        return self.root / state / f"{safe_id}.json"

    def _write(self, path: Path, record: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(record, ensure_ascii=False, allow_nan=False).encode("utf-8")
        fd, temp_path = tempfile.mkstemp(prefix=".tracelet-", suffix=".tmp", dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_path, path)
        except Exception as exc:
            try:
                os.unlink(temp_path)
            except FileNotFoundError:
                pass
            raise StorageError(f"could not write outbox record: {exc}") from exc

    def _load(self, path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as stream:
            return json.load(stream)

    def enqueue(self, job_id: str, payload: dict[str, Any]) -> None:
        if not job_id:
            raise ValueError("job_id is required")
        with self._lock:
            existing = self.get(job_id)
            if existing is not None:
                return
            record = {
                "schema_version": 1,
                "job_id": job_id,
                "status": "pending",
                "attempts": 0,
                "payload": payload,
            }
            self._write(self._path("pending", job_id), record)

    def list_pending(self) -> list[dict[str, Any]]:
        jobs = []
        for path in sorted((self.root / "pending").glob("*.json")):
            try:
                record = self._load(path)
                record.setdefault("job_id", path.stem)
                jobs.append(record)
            except (OSError, json.JSONDecodeError, ValueError):
                self._quarantine(path)
        return jobs

    def _quarantine(self, path: Path) -> None:
        target = self.root / "failed" / path.name
        try:
            os.replace(path, target)
            record = self._load(target)
        except Exception:
            record = {"schema_version": 1, "job_id": unquote(path.stem), "status": "failed", "error": "corrupt job record", "attempts": 0}
            self._write(target, record)
        else:
            record.update(status="failed", error="corrupt job record")
            self._write(target, record)

    def get(self, job_id: str) -> dict[str, Any] | None:
        for state in self._states:
            path = self._path(state, job_id)
            if path.exists():
                try:
                    return self._load(path)
                except (OSError, json.JSONDecodeError):
                    return None
        return None

    def claim(self, job_id: str, *, worker_id: str) -> bool:
        with self._lock:
            source = self._path("pending", job_id)
            destination = self._path("claimed", job_id)
            if not source.exists():
                return False
            try:
                os.replace(source, destination)
            except FileNotFoundError:
                return False
            record = self._load(destination)
            record.update(status="claimed", worker_id=worker_id)
            self._write(destination, record)
            return True

    def release(self, job_id: str) -> None:
        with self._lock:
            source = self._path("claimed", job_id)
            if not source.exists():
                return
            record = self._load(source)
            record.update(status="pending")
            record.pop("worker_id", None)
            target = self._path("pending", job_id)
            self._write(target, record)
            source.unlink(missing_ok=True)

    def checkpoint(self, job_id: str, *, result: Any) -> None:
        """Persist a computed result while claimed, before cross-store delivery."""
        with self._lock:
            path = self._path("claimed", job_id)
            if not path.exists():
                raise KeyError(job_id)
            record = self._load(path)
            record["result_checkpoint"] = result
            self._write(path, record)

    def get_evaluator_checkpoints(self, job_id: str) -> dict[str, Any]:
        with self._lock:
            record = self.get(job_id)
            return dict(record.get("evaluator_checkpoints", {})) if record else {}

    def checkpoint_evaluator(self, job_id: str, evaluator_key: str, *, result: Any) -> None:
        with self._lock:
            path = self._path("claimed", job_id)
            if not path.exists():
                raise KeyError(job_id)
            record = self._load(path)
            checkpoints = record.setdefault("evaluator_checkpoints", {})
            checkpoints[evaluator_key] = result
            self._write(path, record)

    def complete(self, job_id: str, *, result: Any = None) -> None:
        self._transition(job_id, "completed", result=result)

    def fail(
        self,
        job_id: str,
        *,
        error: str,
        retryable: bool = False,
        increment_attempt: bool = True,
    ) -> None:
        self._transition(job_id, "failed", error=error, increment_attempt=increment_attempt)

    def defer(self, job_id: str, *, error: str) -> None:
        with self._lock:
            record = self.get(job_id)
            if record is None:
                raise KeyError(job_id)
            record.update(status="pending", error=error, attempts=record.get("attempts", 0) + 1)
            self._write(self._path("pending", job_id), record)
            self._path("claimed", job_id).unlink(missing_ok=True)

    def retry(self, job_id: str, *, error: str) -> None:
        self.defer(job_id, error=error)

    def _transition(self, job_id: str, state: str, **updates) -> None:
        with self._lock:
            record = self.get(job_id)
            if record is None:
                raise KeyError(job_id)
            record.update(status=state)
            if state in {"completed", "failed"}:
                record.pop("evaluator_checkpoints", None)
            if updates.pop("increment_attempt", False):
                record["attempts"] = record.get("attempts", 0) + 1
            record.update(updates)
            if state == "completed":
                record.pop("result_checkpoint", None)
            self._write(self._path(state, job_id), record)
            for previous in self._states:
                if previous != state:
                    self._path(previous, job_id).unlink(missing_ok=True)

    def prune(self, *, older_than_seconds: float, states=("completed", "failed"), now: float | None = None) -> int:
        """Delete terminal records older than the requested age; return count."""
        if older_than_seconds < 0:
            raise ValueError("older_than_seconds cannot be negative")
        selected = set(states)
        if not selected.issubset({"completed", "failed"}):
            raise ValueError("only completed and failed records can be pruned")
        cutoff = (time.time() if now is None else now) - older_than_seconds
        removed = 0
        with self._lock:
            for state in selected:
                for path in (self.root / state).glob("*.json"):
                    try:
                        if path.stat().st_mtime < cutoff:
                            path.unlink()
                            removed += 1
                    except FileNotFoundError:
                        continue
        return removed
