import asyncio
import unittest

from tests._support import public_symbol


class EvaluatorProtocolTests(unittest.TestCase):
    def test_async_custom_evaluator_returns_a_versioned_typed_result(self):
        Event = public_symbol("Event")
        evaluate = public_symbol("evaluate")

        class CustomEvaluator:
            name = "answer-check"
            version = "2"

            async def evaluate(self, event):
                return {"score_type": "boolean", "score": event.output == "42"}

        event = Event(request_id="r-1", input="math", output="42")
        result = asyncio.run(evaluate(event, [CustomEvaluator()]))[0]

        self.assertEqual(result.evaluator, "answer-check")
        self.assertEqual(result.evaluator_version, "2")
        self.assertIs(result.score, True)
        self.assertEqual(result.status, "completed")

    def test_evaluator_exception_becomes_error_result_without_stopping_others(self):
        Event = public_symbol("Event")
        evaluate = public_symbol("evaluate")

        class Broken:
            name = "broken"
            version = "1"

            def evaluate(self, event):
                raise RuntimeError("provider unavailable")

        class Healthy:
            name = "healthy"
            version = "1"

            def evaluate(self, event):
                return {"score_type": "boolean", "score": True}

        event = Event(request_id="r-2", input="q", output="a")
        results = asyncio.run(evaluate(event, [Broken(), Healthy()]))

        self.assertEqual([result.status for result in results], ["error", "completed"])
        self.assertEqual(results[0].error, "provider unavailable")
        self.assertIs(results[1].score, True)


if __name__ == "__main__":
    unittest.main()
