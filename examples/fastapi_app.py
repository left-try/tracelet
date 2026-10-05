"""Minimal FastAPI integration; install tracelet-evals[fastapi] to run it."""

from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI

from tracelet import (
    Event,
    EvaluationWorker,
    FileStore,
    RequiredTextEvaluator,
    Tracelet,
)

store = FileStore("./.tracelet")
capture = Tracelet(storage=store)
worker = EvaluationWorker(
    store=store,
    evaluators={"required-text": RequiredTextEvaluator(required=["answer"])},
)


@asynccontextmanager
async def lifespan(app):
    await worker.start()
    try:
        yield
    finally:
        await worker.stop()


app = FastAPI(lifespan=lifespan)


@app.post("/answer")
async def answer(prompt: str):
    # Replace this stub with the application's production model call.
    output = f"answer to: {prompt}"
    await capture.record(
        Event(request_id=str(uuid4()), input=prompt, output=output),
        evaluators=[RequiredTextEvaluator(required=["answer"])],
    )
    return {"answer": output}
