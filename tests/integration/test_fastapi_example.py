import asyncio
import tempfile
import unittest
from pathlib import Path

from tests._support import public_symbol


class FastAPIExampleTests(unittest.TestCase):
    def test_example_endpoint_returns_model_answer_and_persists_deterministic_eval(self):
        Tracelet = public_symbol("Tracelet")
        FileStore = public_symbol("FileStore")
        RequiredTextEvaluator = public_symbol("RequiredTextEvaluator")
        Event = public_symbol("Event")

        async def scenario(directory):
            store = FileStore(Path(directory))
            tracelet = Tracelet(storage=store)
            event = Event(request_id="example-1", input="q", output="answer contains ok")
            await tracelet.record(event, evaluators=[RequiredTextEvaluator(required=["ok"])])
            return store.list_pending()

        with tempfile.TemporaryDirectory() as directory:
            pending = asyncio.run(scenario(directory))

        self.assertEqual(pending[0]["payload"]["event"]["request_id"], "example-1")
        self.assertEqual(pending[0]["payload"]["evaluators"], ["required-text"])

    def test_worker_dispatches_recorded_evaluator_names_from_registry(self):
        Event = public_symbol("Event")
        EvaluationWorker = public_symbol("EvaluationWorker")
        FileStore = public_symbol("FileStore")
        RequiredTextEvaluator = public_symbol("RequiredTextEvaluator")
        Tracelet = public_symbol("Tracelet")

        async def scenario(directory):
            store = FileStore(Path(directory))
            evaluator = RequiredTextEvaluator(required=["ok"])
            await Tracelet(storage=store).record(
                Event(request_id="dispatch-1", input="q", output="ok"),
                evaluators=[evaluator],
            )
            worker = EvaluationWorker(store=store, evaluators={"required-text": evaluator})
            await worker.start()
            await worker.wait_idle(timeout=1)
            await worker.stop()
            return store.get("dispatch-1")

        with tempfile.TemporaryDirectory() as directory:
            completed = asyncio.run(scenario(directory))

        self.assertEqual(completed["status"], "completed")
        self.assertEqual(completed["result"][0]["evaluator"], "required-text")
        self.assertTrue(completed["result"][0]["score"])

    def test_record_accepts_registry_names_as_strings(self):
        Event = public_symbol("Event")
        EvaluationWorker = public_symbol("EvaluationWorker")
        FileStore = public_symbol("FileStore")
        RequiredTextEvaluator = public_symbol("RequiredTextEvaluator")
        Tracelet = public_symbol("Tracelet")

        async def scenario(directory):
            store = FileStore(Path(directory))
            await Tracelet(storage=store).record(
                Event(request_id="dispatch-string", input="q", output="ok"),
                evaluators=["required-text"],
            )
            evaluator = RequiredTextEvaluator(required=["ok"])
            worker = EvaluationWorker(store=store, evaluators={"required-text": evaluator})
            await worker.start()
            await worker.wait_idle(timeout=1)
            await worker.stop()
            return store.get("dispatch-string")

        with tempfile.TemporaryDirectory() as directory:
            completed = asyncio.run(scenario(directory))

        self.assertEqual(completed["status"], "completed")
        self.assertEqual(completed["result"][0]["evaluator"], "required-text")


if __name__ == "__main__":
    unittest.main()
