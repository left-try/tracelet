import asyncio
import tempfile
import unittest
from pathlib import Path

from tests._support import public_symbol


class WorkerFileOutboxIntegrationTests(unittest.TestCase):
    def test_archive_retry_reuses_checkpointed_result_after_completion_failure(self):
        import asyncio
        import tempfile
        from pathlib import Path

        Event = public_symbol("Event")
        EvaluationWorker = public_symbol("EvaluationWorker")
        FileStore = public_symbol("FileStore")

        async def scenario(directory):
            root = Path(directory)
            base_store = FileStore(root / "evaluation")
            archive_store = FileStore(root / "archive")
            base_store.enqueue(
                "checkpoint-source",
                {"event": Event(request_id="checkpoint-source", input="q", output="a").to_dict()},
            )

            class FailFirstComplete:
                def __init__(self, wrapped):
                    self.wrapped = wrapped
                    self.failed = False
                def __getattr__(self, name):
                    return getattr(self.wrapped, name)
                def complete(self, job_id, *, result=None):
                    if not self.failed:
                        self.failed = True
                        raise OSError("temporary completion failure")
                    return self.wrapped.complete(job_id, result=result)

            calls = []
            async def evaluator(_event):
                calls.append(len(calls) + 1)
                return {"score_type": "numeric", "score": calls[-1]}

            worker = EvaluationWorker(
                store=FailFirstComplete(base_store),
                evaluator=evaluator,
                archive_store=archive_store,
                poll_interval=0.001,
            )
            await worker.start()
            await worker.wait_idle(timeout=1)
            await worker.stop()
            return base_store.get("checkpoint-source"), archive_store.get("tracelet-eval:checkpoint-source"), calls

        with tempfile.TemporaryDirectory() as directory:
            completed, archive, calls = asyncio.run(scenario(directory))

        self.assertEqual(calls, [1])
        self.assertEqual(completed["result"]["score"], 1)
        self.assertEqual(archive["payload"]["result"]["score"], 1)

    def test_archive_store_cannot_be_same_as_evaluation_store(self):
        import tempfile
        from pathlib import Path

        EvaluationWorker = public_symbol("EvaluationWorker")
        FileStore = public_symbol("FileStore")

        with tempfile.TemporaryDirectory() as directory:
            store = FileStore(Path(directory))
            with self.assertRaises(ValueError):
                EvaluationWorker(store=store, evaluator=lambda _: None, archive_store=FileStore(Path(directory)))

    def test_worker_writes_completed_evaluation_to_separate_archive_outbox(self):
        import asyncio
        import tempfile
        from pathlib import Path

        Event = public_symbol("Event")
        EvaluationWorker = public_symbol("EvaluationWorker")
        FileStore = public_symbol("FileStore")
        S3Sink = public_symbol("S3Sink")
        S3DrainWorker = public_symbol("S3DrainWorker")

        async def scenario(directory):
            root = Path(directory)
            evaluation_store = FileStore(root / "evaluation")
            archive_store = FileStore(root / "archive")
            evaluation_store.enqueue(
                "archive-source",
                {"event": Event(request_id="archive-source", input="q", output="a").to_dict()},
            )
            async def evaluator(_event):
                return {"score_type": "boolean", "score": True}
            worker = EvaluationWorker(
                store=evaluation_store,
                evaluator=evaluator,
                archive_store=archive_store,
            )
            await worker.start()
            await worker.wait_idle(timeout=1)
            await worker.stop()
            class S3Client:
                def __init__(self):
                    self.objects = []
                async def put_object(self, **kwargs):
                    self.objects.append(kwargs)

            client = S3Client()
            drain = S3DrainWorker(store=archive_store, sink=S3Sink(client=client, bucket="evals"))
            await drain.drain_once()
            return evaluation_store.get("archive-source"), archive_store.get("tracelet-eval:archive-source"), client.objects

        with tempfile.TemporaryDirectory() as directory:
            completed, archived, objects = asyncio.run(scenario(directory))

        self.assertEqual(completed["status"], "completed")
        self.assertEqual(archived["status"], "completed")
        self.assertEqual(archived["payload"]["source_job_id"], "archive-source")
        self.assertTrue(archived["payload"]["result"]["score"])
        self.assertIn('"source_job_id": "archive-source"', objects[0]["Body"].decode())

    def test_pipeline_result_list_is_serialized_and_completed(self):
        import asyncio
        import tempfile

        from tests._support import public_symbol

        Event = public_symbol("Event")
        EvaluationPipeline = public_symbol("EvaluationPipeline")
        EvaluationWorker = public_symbol("EvaluationWorker")
        FileStore = public_symbol("FileStore")
        RequiredTextEvaluator = public_symbol("RequiredTextEvaluator")
        Tracelet = public_symbol("Tracelet")

        async def run():
            with tempfile.TemporaryDirectory() as directory:
                store = FileStore(directory)
                tracelet = Tracelet(storage=store)
                await tracelet.record(Event(request_id="pipeline-result", input="q", output="answer"))
                worker = EvaluationWorker(
                    store=store,
                    evaluator=EvaluationPipeline(
                        deterministic=[RequiredTextEvaluator(required=["answer"])]
                    ).run,
                )
                await worker.start()
                await worker.wait_idle(timeout=1)
                await worker.stop()
                return store.get("pipeline-result")

        record = asyncio.run(run())
        self.assertEqual(record["status"], "completed")
        self.assertEqual(record["result"][0]["evaluator"], "required-text")
        self.assertTrue(record["result"][0]["score"])

    def test_worker_restarts_and_processes_job_left_pending_on_disk(self):
        FileStore = public_symbol("FileStore")
        EvaluationWorker = public_symbol("EvaluationWorker")

        async def scenario(directory):
            first = FileStore(Path(directory))
            first.enqueue(job_id="survives-restart", payload={"request_id": "r-durable"})
            recovered = FileStore(Path(directory))
            processed = asyncio.Event()

            async def evaluator(event):
                self.assertEqual(event.request_id, "r-durable")
                processed.set()
                return {"score": True}

            worker = EvaluationWorker(store=recovered, evaluator=evaluator, poll_interval=0.001)
            await worker.start()
            await asyncio.wait_for(processed.wait(), timeout=1)
            await worker.stop()

            self.assertEqual(recovered.list_pending(), [])
            self.assertEqual(recovered.get("survives-restart")["status"], "completed")

        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(scenario(directory))


if __name__ == "__main__":
    unittest.main()
