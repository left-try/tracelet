import asyncio
import unittest

from tests._support import public_symbol


class JudgePipelineTests(unittest.TestCase):
    def test_observed_cost_ceiling_skips_later_judge_calls(self):
        Event = public_symbol("Event")
        EvaluationPipeline = public_symbol("EvaluationPipeline")

        class CostedJudge:
            name = "costed"
            version = "1"
            async def evaluate(self, _event):
                from tracelet import EvaluationResult
                return EvaluationResult(evaluator=self.name, evaluator_version=self.version,
                    score_type="boolean", score=True, usage={"cost_usd": 0.02})

        pipeline = EvaluationPipeline(judge=CostedJudge(), max_judge_cost=0.02)
        async def run():
            first = await pipeline.run(Event(request_id="cost-1", input="q", output="a"))
            second = await pipeline.run(Event(request_id="cost-2", input="q", output="a"))
            return first, second
        first, second = asyncio.run(run())
        self.assertEqual(first[0].status, "completed")
        self.assertEqual(second[0].status, "skipped")
        self.assertIn("max_judge_cost", second[0].details)

    def test_judge_concurrency_is_bounded(self):
        Event = public_symbol("Event")
        EvaluationPipeline = public_symbol("EvaluationPipeline")
        from tracelet import EvaluationResult

        class SlowJudge:
            name = "slow"
            version = "1"
            def __init__(self):
                self.active = 0
                self.peak = 0
            async def evaluate(self, _event):
                self.active += 1
                self.peak = max(self.peak, self.active)
                await asyncio.sleep(0.01)
                self.active -= 1
                return EvaluationResult(evaluator=self.name, evaluator_version=self.version,
                    score_type="boolean", score=True)

        judge = SlowJudge()
        pipeline = EvaluationPipeline(judge=judge, max_concurrent_judges=2)
        async def run():
            await asyncio.gather(*(pipeline.run(Event(request_id=str(i), input="q", output="a"))
                                   for i in range(6)))
        asyncio.run(run())
        self.assertEqual(judge.peak, 2)

    def test_deterministic_failure_short_circuits_expensive_judge(self):
        Event = public_symbol("Event")
        EvaluationPipeline = public_symbol("EvaluationPipeline")
        calls = []

        class FormatCheck:
            name = "format"
            version = "1"

            def evaluate(self, event):
                return {"score_type": "boolean", "score": False}

        async def expensive_judge(_event):
            calls.append("judge")
            return {"score_type": "numeric", "score": 1.0}

        results = asyncio.run(
            EvaluationPipeline(deterministic=[FormatCheck()], judge=expensive_judge, short_circuit=True).run(
                Event(request_id="pipe-1", input="q", output="bad")
            )
        )

        self.assertEqual(calls, [])
        self.assertEqual([result.evaluator for result in results], ["format"])

    def test_ambiguous_cheap_judge_escalates_to_stronger_judge(self):
        Event = public_symbol("Event")
        EvaluationPipeline = public_symbol("EvaluationPipeline")
        calls = []

        async def cheap(_event):
            calls.append("cheap")
            return {"score_type": "categorical", "score": "uncertain"}

        async def strong(_event):
            calls.append("strong")
            return {"score_type": "boolean", "score": True}

        results = asyncio.run(
            EvaluationPipeline(cheap_judge=cheap, judge=strong, escalate_on={"uncertain"}).run(
                Event(request_id="pipe-2", input="q", output="a")
            )
        )

        self.assertEqual(calls, ["cheap", "strong"])
        self.assertEqual(results[-1].score, True)

    def test_sampling_and_max_judge_calls_bound_evaluation_volume(self):
        Event = public_symbol("Event")
        EvaluationPipeline = public_symbol("EvaluationPipeline")
        calls = []

        async def judge(_event):
            calls.append(None)
            return {"score_type": "boolean", "score": True}

        pipeline = EvaluationPipeline(judge=judge, sample_rate=0.0, max_judge_calls=1)
        results = asyncio.run(
            pipeline.run(Event(request_id="sampled-out", input="q", output="a"))
        )

        self.assertEqual(calls, [])
        self.assertEqual(results[0].status, "skipped")

    def test_max_judge_calls_is_enforced_across_multiple_events(self):
        Event = public_symbol("Event")
        EvaluationPipeline = public_symbol("EvaluationPipeline")
        calls = []

        async def judge(_event):
            calls.append(None)
            return {"score_type": "boolean", "score": True}

        pipeline = EvaluationPipeline(judge=judge, sample_rate=1.0, max_judge_calls=1)
        results = [
            asyncio.run(pipeline.run(Event(request_id=f"budget-{i}", input="q", output="a")))
            for i in range(3)
        ]

        self.assertEqual(len(calls), 1)
        self.assertEqual(results[0][0].status, "completed")
        self.assertEqual([batch[0].status for batch in results[1:]], ["skipped", "skipped"])


if __name__ == "__main__":
    unittest.main()
