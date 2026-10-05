import asyncio
import unittest

from tests._support import public_symbol


class CandidateRunnerTests(unittest.TestCase):
    def test_worker_persists_candidate_outputs_and_pairwise_verdict(self):
        import tempfile
        from pathlib import Path

        Event = public_symbol("Event")
        EvaluationWorker = public_symbol("EvaluationWorker")
        FileStore = public_symbol("FileStore")
        PairwiseEvaluator = public_symbol("PairwiseEvaluator")
        CandidateRunner = public_symbol("CandidateRunner")

        async def scenario(directory):
            store = FileStore(Path(directory))
            event = Event(request_id="persist-compare", input="q", output="baseline")
            store.enqueue("persist-compare", {"event": event.to_dict()})
            async def candidate(_event):
                return "candidate"
            async def judge(_request):
                return {"winner": "B", "rationale": "better answer"}
            worker = EvaluationWorker(
                store=store,
                evaluator=lambda _event: [],
                candidate_runner=CandidateRunner(candidate=candidate),
                pairwise_evaluator=PairwiseEvaluator(judge=judge),
            )
            await worker.start()
            await worker.wait_idle(timeout=1)
            await worker.stop()
            return store.get("persist-compare")["result"]

        with tempfile.TemporaryDirectory() as directory:
            result = asyncio.run(scenario(directory))

        self.assertEqual(result["comparison"]["candidates"][0]["output"], "candidate")
        self.assertEqual(result["pairwise"][0]["winner"], "B")

    def test_baseline_and_candidate_are_linked_to_same_request_snapshot(self):
        Event = public_symbol("Event")
        CandidateRunner = public_symbol("CandidateRunner")
        received = []

        async def candidate(event):
            received.append((event.request_id, event.input, event.context))
            return "candidate answer"

        event = Event(
            request_id="compare-1", input="question", output="baseline answer", context=["evidence"]
        )
        comparison = asyncio.run(CandidateRunner(candidate=candidate).run(event))

        self.assertEqual(received, [("compare-1", "question", ["evidence"])])
        self.assertEqual(comparison.request_id, "compare-1")
        self.assertEqual(comparison.baseline.output, "baseline answer")
        self.assertEqual(comparison.candidates[0].output, "candidate answer")
        self.assertEqual(comparison.baseline.model, "baseline")
        self.assertEqual(comparison.candidates[0].model, "candidate")

    def test_candidate_timeout_is_recorded_and_does_not_change_baseline(self):
        Event = public_symbol("Event")
        CandidateRunner = public_symbol("CandidateRunner")

        async def candidate(_event):
            raise TimeoutError("candidate timeout")

        comparison = asyncio.run(
            CandidateRunner(candidate=candidate).run(
                Event(request_id="compare-2", input="q", output="served answer")
            )
        )

        self.assertEqual(comparison.baseline.output, "served answer")
        self.assertEqual(comparison.candidates[0].status, "error")
        self.assertIn("timeout", comparison.candidates[0].error)

    def test_sampling_can_skip_candidate_without_ambiguity(self):
        Event = public_symbol("Event")
        CandidateRunner = public_symbol("CandidateRunner")
        calls = []

        async def candidate(_event):
            calls.append(None)
            return "candidate"

        comparison = asyncio.run(
            CandidateRunner(candidate=candidate, sample_rate=0.0).run(
                Event(request_id="compare-3", input="q", output="baseline")
            )
        )

        self.assertEqual(calls, [])
        self.assertEqual(comparison.candidates[0].status, "skipped")

    def test_multiple_candidate_results_remain_independent(self):
        Event = public_symbol("Event")
        CandidateRunner = public_symbol("CandidateRunner")

        async def first(_event):
            return "first output"

        async def second(_event):
            raise RuntimeError("second provider down")

        comparison = asyncio.run(
            CandidateRunner(candidates={"candidate-a": first, "candidate-b": second}).run(
                Event(request_id="compare-multiple", input="q", output="baseline")
            )
        )

        self.assertEqual(comparison.candidates[0].output, "first output")
        self.assertEqual(comparison.candidates[0].status, "completed")
        self.assertEqual(comparison.candidates[1].status, "error")
        self.assertEqual(comparison.candidates[1].error, "second provider down")


if __name__ == "__main__":
    unittest.main()
