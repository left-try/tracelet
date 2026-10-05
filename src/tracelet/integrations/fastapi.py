from __future__ import annotations

from contextlib import asynccontextmanager


@asynccontextmanager
async def worker_lifespan(worker):
    await worker.start()
    try:
        yield
    finally:
        await worker.stop()


def attach_worker(app, worker):
    """Attach a worker to an existing FastAPI/Starlette app lifespan."""
    original = app.router.lifespan_context

    @asynccontextmanager
    async def combined_lifespan(application):
        async with original(application):
            async with worker_lifespan(worker):
                yield

    app.router.lifespan_context = combined_lifespan
    return app
