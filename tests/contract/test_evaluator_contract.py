import unittest

from tests._support import public_symbol


class EvaluatorContractTests(unittest.TestCase):
    def test_built_in_evaluators_produce_results_with_the_common_contract(self):
        Event = public_symbol("Event")
        RequiredTextEvaluator = public_symbol("RequiredTextEvaluator")
        event = Event(request_id="contract-1", input="q", output="has answer")
        result = RequiredTextEvaluator(required=["answer"]).evaluate(event)

        self.assertEqual(result.evaluator, "required-text")
        self.assertEqual(result.score_type, "boolean")
        self.assertIs(result.score, True)
        self.assertEqual(result.status, "completed")


if __name__ == "__main__":
    unittest.main()
