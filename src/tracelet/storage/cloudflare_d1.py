"""Async storage adapter for Cloudflare D1's parameterized query API."""
from __future__ import annotations

import asyncio
import inspect
import json
import re
import time
from typing import Any


class CloudflareD1Store:
    """Store Tracelet outbox jobs in D1 through an async HTTP client.

    ``query_url`` is the full D1 ``/query`` endpoint, or a compatible Worker
    proxy URL. Pass an application-owned HTTP client to manage its own
    authentication and lifecycle. Async clients are recommended; sync client
    requests are moved to a worker thread. When omitted, an owned
    ``httpx.AsyncClient`` is created and must be closed with ``await store.close()``.

    ``api_style="cloudflare"`` sends the Cloudflare REST shape (``sql`` field)
    and reads its standard response envelope. ``api_style="worker-proxy"``
    sends ``query`` and accepts the D1 Worker binding result shape.

    Call ``await initialize()`` once during application startup to create the
    table and pending-job index. D1 stores JSON fields as text and uses
    parameterized SQLite queries for all job data. ``pending_limit`` bounds
    the result set fetched during each worker poll.
    """

    def __init__(
        self,
        *,
        query_url: str,
        client=None,
        api_token: str | None = None,
        api_style: str = "cloudflare",
        table_name: str = "tracelet_jobs",
        pending_limit: int = 100,
        timeout: float = 10.0,
    ):
        if not query_url:
            raise ValueError("query_url is required")
        if api_style not in {"cloudflare", "worker-proxy"}:
            raise ValueError("api_style must be 'cloudflare' or 'worker-proxy'")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
            raise ValueError("table_name must contain only letters, digits, and underscores")
        if pending_limit < 1:
            raise ValueError("pending_limit must be positive")
        self.query_url = query_url
        self.table_name = table_name
        self._quoted_table = f'"{table_name}"'
        self.checkpoint_table_name = f"{table_name}_evaluator_checkpoints"
        self._quoted_checkpoint_table = f'"{self.checkpoint_table_name}"'
        self.api_token = api_token
        self.api_style = api_style
        self.pending_limit = pending_limit
        self._owns_client = client is None
        if client is None:
            try:
                import httpx
            except ImportError as exc:  # pragma: no cover - optional dependency
                raise ImportError("CloudflareD1Store requires `pip install tracelet-evals[d1]`") from exc
            client = httpx.AsyncClient(timeout=timeout)
        self.client = client

    async def initialize(self) -> None:
        """Create the outbox table and index if they do not already exist."""
        await self._query(
            f"CREATE TABLE IF NOT EXISTS {self._quoted_table} ("
            "job_id TEXT PRIMARY KEY, status TEXT NOT NULL, payload TEXT NOT NULL, "
            "result TEXT, result_checkpoint TEXT, attempts INTEGER NOT NULL DEFAULT 0, "
            "error TEXT, worker_id TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL)"
        )
        await self._query(
            f"CREATE INDEX IF NOT EXISTS \"{self.table_name}_pending_created\" "
            f"ON {self._quoted_table} (status, created_at, job_id)"
        )
        await self._query(
            f"CREATE TABLE IF NOT EXISTS {self._quoted_checkpoint_table} ("
            "job_id TEXT NOT NULL, evaluator_key TEXT NOT NULL, result TEXT NOT NULL, "
            "PRIMARY KEY (job_id, evaluator_key))"
        )
        await self._query(
            f"UPDATE {self._quoted_table} SET status = 'pending', worker_id = NULL, updated_at = ? "
            "WHERE status = 'claimed'",
            [str(time.time())],
        )

    async def _query(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        headers = {"Authorization": f"Bearer {self.api_token}"} if self.api_token else None
        query_field = "query" if self.api_style == "worker-proxy" else "sql"
        kwargs = {"json": {query_field: sql, "params": [str(value) for value in (params or [])]}}
        if headers:
            kwargs["headers"] = headers
        post = self.client.post
        if inspect.iscoroutinefunction(post):
            response = await post(self.query_url, **kwargs)
        else:
            response = await asyncio.to_thread(post, self.query_url, **kwargs)
            if inspect.isawaitable(response):
                response = await response
        response.raise_for_status()
        payload = response.json()
        if inspect.isawaitable(payload):
            payload = await payload
        if not isinstance(payload, dict):
            raise RuntimeError("D1 query response must be an object")
        if payload.get("error"):
            raise RuntimeError(str(payload["error"]))
        if payload.get("success") is False:
            errors = payload.get("errors") or []
            message = "; ".join(str(item.get("message", item)) if isinstance(item, dict) else str(item)
                                 for item in errors)
            raise RuntimeError(message or "D1 query failed")
        result = payload.get("result")
        if result is None and "results" in payload:
            return payload.get("results") or []
        result = result or []
        if isinstance(result, dict):
            result = [result]
        if not result:
            return []
        query_result = result[0]
        if not isinstance(query_result, dict):
            raise RuntimeError("D1 query result must be an object")
        if query_result.get("success") is False:
            raise RuntimeError("D1 query failed")
        rows = query_result.get("results") or []
        if not isinstance(rows, list):
            raise RuntimeError("D1 query results must be a list")
        return rows

    @staticmethod
    def _decode_record(row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        decoded = dict(row)
        if decoded.get("result_checkpoint") is None:
            decoded.pop("result_checkpoint", None)
        for field in ("payload", "result", "result_checkpoint"):
            value = decoded.get(field)
            if value is not None and isinstance(value, str):
                decoded[field] = json.loads(value)
        return decoded

    async def enqueue(self, job_id: str, payload: dict[str, Any]) -> None:
        if not job_id:
            raise ValueError("job_id is required")
        now = time.time()
        await self._query(
            f"INSERT OR IGNORE INTO {self._quoted_table} "
            "(job_id, status, payload, attempts, created_at, updated_at) "
            "VALUES (?, 'pending', ?, 0, ?, ?)",
            [job_id, json.dumps(payload, ensure_ascii=False, allow_nan=False), str(now), str(now)],
        )

    async def get(self, job_id: str) -> dict[str, Any] | None:
        rows = await self._query(
            f"SELECT job_id, status, payload, result, result_checkpoint, attempts, error, worker_id "
            f"FROM {self._quoted_table} WHERE job_id = ? LIMIT 1",
            [job_id],
        )
        return self._decode_record(rows[0] if rows else None)

    async def list_pending(self) -> list[dict[str, Any]]:
        rows = await self._query(
            f"SELECT job_id, status, payload, result, result_checkpoint, attempts, error, worker_id "
            f"FROM {self._quoted_table} WHERE status = 'pending' ORDER BY created_at, job_id LIMIT ?",
            [str(self.pending_limit)],
        )
        return [self._decode_record(row) for row in rows]

    async def claim(self, job_id: str, *, worker_id: str) -> bool:
        rows = await self._query(
            f"UPDATE {self._quoted_table} SET status = 'claimed', worker_id = ?, updated_at = ? "
            "WHERE job_id = ? AND status = 'pending' RETURNING job_id",
            [worker_id, str(time.time()), job_id],
        )
        return bool(rows)

    async def checkpoint(self, job_id: str, *, result: Any) -> None:
        rows = await self._query(
            f"UPDATE {self._quoted_table} SET result_checkpoint = ?, updated_at = ? "
            "WHERE job_id = ? AND status = 'claimed' RETURNING job_id",
            [json.dumps(result, ensure_ascii=False, allow_nan=False), str(time.time()), job_id],
        )
        if not rows:
            raise KeyError(job_id)

    async def get_evaluator_checkpoints(self, job_id: str) -> dict[str, Any]:
        rows = await self._query(
            f"SELECT evaluator_key, result FROM {self._quoted_checkpoint_table} "
            "WHERE job_id = ? ORDER BY evaluator_key",
            [job_id],
        )
        return {row["evaluator_key"]: json.loads(row["result"]) for row in rows}

    async def checkpoint_evaluator(self, job_id: str, evaluator_key: str, *, result: Any) -> None:
        rows = await self._query(
            f"INSERT INTO {self._quoted_checkpoint_table} (job_id, evaluator_key, result) "
            f"SELECT ?, ?, ? WHERE EXISTS (SELECT 1 FROM {self._quoted_table} "
            "WHERE job_id = ? AND status = 'claimed') "
            "ON CONFLICT(job_id, evaluator_key) DO UPDATE SET result = excluded.result "
            "RETURNING evaluator_key",
            [job_id, evaluator_key, json.dumps(result, ensure_ascii=False, allow_nan=False), job_id],
        )
        if not rows:
            raise KeyError(job_id)

    async def release(self, job_id: str) -> None:
        await self._query(
            f"UPDATE {self._quoted_table} SET status = 'pending', worker_id = NULL, updated_at = ? "
            "WHERE job_id = ? AND status = 'claimed'",
            [str(time.time()), job_id],
        )

    async def complete(self, job_id: str, *, result: Any = None) -> None:
        rows = await self._query(
            f"UPDATE {self._quoted_table} SET status = 'completed', result = ?, "
            "result_checkpoint = NULL, worker_id = NULL, updated_at = ? "
            "WHERE job_id = ? RETURNING job_id",
            [json.dumps(result, ensure_ascii=False, allow_nan=False) if result is not None else None,
             str(time.time()), job_id],
        )
        if not rows:
            raise KeyError(job_id)
        await self._delete_evaluator_checkpoints(job_id)

    async def retry(self, job_id: str, *, error: str) -> None:
        rows = await self._query(
            f"UPDATE {self._quoted_table} SET status = 'pending', attempts = attempts + 1, "
            "error = ?, worker_id = NULL, updated_at = ? WHERE job_id = ? RETURNING job_id",
            [error, str(time.time()), job_id],
        )
        if not rows:
            raise KeyError(job_id)

    async def defer(self, job_id: str, *, error: str) -> None:
        await self.retry(job_id, error=error)

    async def fail(
        self,
        job_id: str,
        *,
        error: str,
        retryable: bool = False,
        increment_attempt: bool = True,
    ) -> None:
        del retryable  # The worker decides whether a failed job is retried.
        attempt_update = "attempts = attempts + 1, " if increment_attempt else ""
        rows = await self._query(
            f"UPDATE {self._quoted_table} SET status = 'failed', {attempt_update}"
            "error = ?, worker_id = NULL, updated_at = ? WHERE job_id = ? RETURNING job_id",
            [error, str(time.time()), job_id],
        )
        if not rows:
            raise KeyError(job_id)
        await self._delete_evaluator_checkpoints(job_id)

    async def _delete_evaluator_checkpoints(self, job_id: str) -> None:
        await self._query(
            f"DELETE FROM {self._quoted_checkpoint_table} WHERE job_id = ?",
            [job_id],
        )

    async def close(self) -> None:
        """Close the internally created HTTP client, leaving supplied clients alone."""
        if self._owns_client:
            await self.client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        await self.close()
