import asyncio
import tempfile
import unittest
from pathlib import Path

from tests._support import public_symbol


class WorkerTests(unittest.TestCase):
    def test_worker_redacts_event_results_errors_and_archive_records(self):
        Event = public_symbol("Event")
        EvaluationWorker = public_symbol("EvaluationWorker")
        FileStore = public_symbol("FileStore")

        async def scenario(directory):
            root = Path(directory)
            store = FileStore(root / "evaluation")
            archive = FileStore(root / "archive")

            async def evaluator(event):
                if event.request_id == "redact-error":
                    raise RuntimeError("provider failed: access_token=access-token-supersecret")
                return {
                    "score_type": "text",
                    "score": "password=result-secret-value",
                }

            worker = EvaluationWorker(
                store=store, evaluator=evaluator, archive_store=archive,
                max_attempts=1, poll_interval=0.002,
            )
            await worker.enqueue(Event(
                request_id="redact-success",
                input="api_key=input-secret-value",
                output="ok",
            ))
            await worker.enqueue(Event(request_id="redact-error", input="q", output="a"))
            await worker.start()
            await worker.wait_idle(timeout=2)
            await worker.stop()
            return store.get("redact-success"), store.get("redact-error"), archive.get(
                "tracelet-eval:redact-success"
            )

        with tempfile.TemporaryDirectory() as directory:
            completed, failed, archived = asyncio.run(scenario(directory))

        self.assertEqual(completed["status"], "completed")
        self.assertNotIn("input-secret-value", str(completed))
        self.assertNotIn("result-secret-value", str(completed))
        self.assertEqual(failed["status"], "failed")
        self.assertNotIn("access-token-supersecret", str(failed))
        self.assertNotIn("input-secret-value", str(archived))
        self.assertNotIn("result-secret-value", str(archived))

    def test_redaction_failure_fails_closed_before_enqueue(self):
        Event = public_symbol("Event")
        EvaluationWorker = public_symbol("EvaluationWorker")
        FileStore = public_symbol("FileStore")
        RedactionPolicy = public_symbol("RedactionPolicy")

        async def scenario(directory):
            store = FileStore(Path(directory))
            worker = EvaluationWorker(
                store=store,
                evaluator=lambda _event: {"score_type": "boolean", "score": True},
                redaction=RedactionPolicy(custom_patterns=("(",)),
            )
            with self.assertRaisesRegex(RuntimeError, "secret redaction failed"):
                await worker.enqueue(Event(
                    request_id="redaction-fails-closed",
                    input="api_key=never-persist-this-secret",
                    output="a",
                ))
            return store.get("redaction-fails-closed")

        with tempfile.TemporaryDirectory() as directory:
            self.assertIsNone(asyncio.run(scenario(directory)))

    def test_worker_processes_enqueued_job_without_blocking_enqueue(self):
        FileStore = public_symbol("FileStore")
        EvaluationWorker = public_symbol("EvaluationWorker")
        Event = public_symbol("Event")

        async def scenario():
            with tempfile.TemporaryDirectory() as directory:
                store = FileStore(Path(directory))
                processed = asyncio.Event()

                async def evaluator(event):
                    await asyncio.sleep(0.01)
                    processed.set()
                    return {"score": True}

                worker = EvaluationWorker(store=store, evaluator=evaluator)
                await worker.start()
                await worker.enqueue(Event(request_id="r-worker", input="q", output="a"))
                self.assertFalse(processed.is_set())
                await asyncio.wait_for(processed.wait(), timeout=1)
                await worker.stop()
                self.assertEqual(store.list_pending(), [])

        asyncio.run(scenario())

    def test_worker_never_exceeds_configured_concurrency(self):
        EvaluationWorker = public_symbol("EvaluationWorker")
        FileStore = public_symbol("FileStore")

        async def scenario():
            with tempfile.TemporaryDirectory() as directory:
                store = FileStore(Path(directory))
                for index in range(6):
                    store.enqueue(job_id=f"job-{index}", payload={"request_id": f"r-{index}"})
                active = 0
                peak = 0
                finished = 0
                all_finished = asyncio.Event()

                async def evaluator(payload):
                    nonlocal active, peak, finished
                    active += 1
                    peak = max(peak, active)
                    await asyncio.sleep(0.01)
                    active -= 1
                    finished += 1
                    if finished == 6:
                        all_finished.set()
                    return {"score": True}

                worker = EvaluationWorker(store=store, evaluator=evaluator, max_concurrency=2)
                await worker.start()
                await asyncio.wait_for(all_finished.wait(), timeout=2)
                await worker.stop()

                self.assertLessEqual(peak, 2)
                self.assertEqual(store.list_pending(), [])

        asyncio.run(scenario())

    def test_worker_keeps_unfinished_job_pending_after_shutdown_timeout(self):
        EvaluationWorker = public_symbol("EvaluationWorker")
        FileStore = public_symbol("FileStore")

        async def scenario():
            with tempfile.TemporaryDirectory() as directory:
                store = FileStore(Path(directory))
                store.enqueue(job_id="unfinished", payload={"request_id": "r-pending"})
                entered = asyncio.Event()

                async def evaluator(payload):
                    entered.set()
                    await asyncio.Event().wait()

                worker = EvaluationWorker(store=store, evaluator=evaluator)
                await worker.start()
                await asyncio.wait_for(entered.wait(), timeout=1)
                await worker.stop(drain_timeout=0)

                self.assertEqual([job["job_id"] for job in store.list_pending()], ["unfinished"])

        asyncio.run(scenario())

    def test_permanent_job_failure_does_not_prevent_next_job_from_completing(self):
        EvaluationWorker = public_symbol("EvaluationWorker")
        FileStore = public_symbol("FileStore")

        async def scenario():
            with tempfile.TemporaryDirectory() as directory:
                store = FileStore(Path(directory))
                store.enqueue(job_id="fails", payload={"request_id": "fail-this"})
                store.enqueue(job_id="succeeds", payload={"request_id": "succeed-this"})
                processed = 0
                all_processed = asyncio.Event()

                async def evaluator(event):
                    nonlocal processed
                    processed += 1
                    if event.request_id == "fail-this":
                        raise ValueError("invalid evaluator input")
                    if processed == 2:
                        all_processed.set()
                    return {"score": True}

                worker = EvaluationWorker(store=store, evaluator=evaluator, max_attempts=1)
                await worker.start()
                await asyncio.wait_for(all_processed.wait(), timeout=1)
                await worker.stop()

                self.assertEqual(store.get("fails")["status"], "failed")
                self.assertEqual(store.get("succeeds")["status"], "completed")

        asyncio.run(scenario())

    def test_worker_rejects_new_jobs_after_shutdown(self):
        EvaluationWorker = public_symbol("EvaluationWorker")
        FileStore = public_symbol("FileStore")

        async def scenario(directory):
            worker = EvaluationWorker(store=FileStore(Path(directory)), evaluator=lambda _: True)
            await worker.start()
            await worker.stop()
            with self.assertRaises(RuntimeError):
                await worker.enqueue({"request_id": "too-late"})

        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(scenario(directory))

    def test_registry_worker_runs_all_registered_evaluators_when_job_has_no_selector(self):
        EvaluationWorker = public_symbol("EvaluationWorker")
        FileStore = public_symbol("FileStore")

        async def scenario(directory):
            store = FileStore(Path(directory))
            store.enqueue("default-registry", {"request_id": "r", "input": "q", "output": "a"})

            class One:
                name = "one"
                def evaluate(self, event): return {"score_type": "boolean", "score": True}

            class Two:
                name = "two"
                def evaluate(self, event): return {"score_type": "boolean", "score": False}

            worker = EvaluationWorker(store=store, evaluators={"one": One(), "two": Two()})
            await worker.start()
            await worker.wait_idle(timeout=1)
            await worker.stop()
            return store.get("default-registry")

        with tempfile.TemporaryDirectory() as directory:
            record = asyncio.run(scenario(directory))

        self.assertEqual([result["evaluator"] for result in record["result"]], ["one", "two"])


if __name__ == "__main__":
    unittest.main()
