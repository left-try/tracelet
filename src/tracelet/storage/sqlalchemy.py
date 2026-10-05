"""Optional SQLAlchemy Core storage adapter (install ``tracelet-evals[sql]``)."""
from __future__ import annotations

from typing import Any


class SQLAlchemyStore:
    """Persist the Tracelet outbox using an application-owned SQLAlchemy Engine.

    The adapter owns short transactions on the supplied Engine, not the
    application's ORM session. Use the same database/credentials as the app;
    no driver is bundled, so install the driver matching the Engine URL.
    """

    def __init__(self, engine, *, table_name: str = "tracelet_jobs", create_table: bool = True):
        try:
            import sqlalchemy as sa
        except ImportError as exc:  # pragma: no cover - depends on optional extra
            raise ImportError("SQLAlchemyStore requires `pip install tracelet-evals[sql]`") from exc
        if not table_name.isidentifier():
            raise ValueError("table_name must be a valid SQL identifier")
        self.engine = engine
        self.sa = sa
        metadata = sa.MetaData()
        self.table = sa.Table(
            table_name, metadata,
            sa.Column("job_id", sa.String(512), primary_key=True),
            sa.Column("status", sa.String(16), nullable=False, index=True),
            sa.Column("payload", sa.JSON, nullable=False),
            sa.Column("result", sa.JSON),
            sa.Column("result_checkpoint", sa.JSON),
            sa.Column("attempts", sa.Integer, nullable=False, default=0),
            sa.Column("error", sa.Text),
            sa.Column("worker_id", sa.String(256)),
            sa.Column("created_at", sa.Float, nullable=False),
            sa.Column("updated_at", sa.Float, nullable=False),
        )
        if create_table:
            metadata.create_all(engine, tables=[self.table])

    def _row(self, row):
        if row is None:
            return None
        result = dict(row)
        result.pop("created_at", None)
        result.pop("updated_at", None)
        return result

    def enqueue(self, job_id: str, payload: dict[str, Any]) -> None:
        import time
        if not job_id:
            raise ValueError("job_id is required")
        sa, table = self.sa, self.table
        with self.engine.begin() as conn:
            existing = conn.execute(sa.select(table.c.job_id).where(table.c.job_id == job_id)).first()
            if existing is None:
                now = time.time()
                conn.execute(table.insert().values(job_id=job_id, status="pending", payload=payload,
                    attempts=0, created_at=now, updated_at=now))

    def get(self, job_id: str):
        with self.engine.connect() as conn:
            row = conn.execute(self.sa.select(self.table).where(self.table.c.job_id == job_id)).mappings().first()
        return self._row(row)

    def list_pending(self):
        with self.engine.connect() as conn:
            rows = conn.execute(self.sa.select(self.table).where(self.table.c.status == "pending")
                                .order_by(self.table.c.created_at)).mappings().all()
        return [self._row(row) for row in rows]

    def _update(self, job_id, values, *, where_status=None):
        import time
        table = self.table
        condition = table.c.job_id == job_id
        if where_status is not None:
            condition = condition & (table.c.status == where_status)
        values["updated_at"] = time.time()
        with self.engine.begin() as conn:
            result = conn.execute(table.update().where(condition).values(**values))
        if result.rowcount != 1:
            raise KeyError(job_id)

    def claim(self, job_id: str, *, worker_id: str) -> bool:
        import time
        with self.engine.begin() as conn:
            changed = conn.execute(self.table.update().where(
                (self.table.c.job_id == job_id) & (self.table.c.status == "pending")
            ).values(status="claimed", worker_id=worker_id, updated_at=time.time()))
        return changed.rowcount == 1

    def checkpoint(self, job_id: str, *, result: Any) -> None:
        self._update(job_id, {"result_checkpoint": result}, where_status="claimed")

    def release(self, job_id: str) -> None:
        self._update(job_id, {"status": "pending", "worker_id": None}, where_status="claimed")

    def complete(self, job_id: str, *, result: Any = None) -> None:
        self._update(job_id, {"status": "completed", "result": result,
                              "result_checkpoint": None, "worker_id": None})

    def retry(self, job_id: str, *, error: str) -> None:
        self._increment(job_id, "pending", error)

    def defer(self, job_id: str, *, error: str) -> None:
        self.retry(job_id, error=error)

    def fail(self, job_id: str, *, error: str, retryable: bool = False,
             increment_attempt: bool = True) -> None:
        values = {"status": "failed", "error": error, "worker_id": None}
        if increment_attempt:
            values["attempts"] = self.table.c.attempts + 1
        self._update(job_id, values)

    def _increment(self, job_id, status, error):
        import time
        table = self.table
        with self.engine.begin() as conn:
            result = conn.execute(table.update().where(table.c.job_id == job_id).values(
                status=status, error=error, worker_id=None,
                attempts=table.c.attempts + 1, updated_at=time.time()))
        if result.rowcount != 1:
            raise KeyError(job_id)
