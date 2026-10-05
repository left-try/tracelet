"""Behavioral contract for typed evaluation results."""

import unittest
from tests._support import public_symbol


class EvaluationResultTests(unittest.TestCase):
    def test_result_preserves_boolean_numeric_categorical_and_text_scores(self):
        EvaluationResult = public_symbol("EvaluationResult")
        cases = (
            ("boolean", True),
            ("numeric", 0.75),
            ("categorical", "supported"),
            ("text", "The answer is supported by the cited evidence."),
        )

        for score_type, score in cases:
            with self.subTest(score_type=score_type):
                result = EvaluationResult(
                    evaluator="faithfulness",
                    evaluator_version="1",
                    score_type=score_type,
                    score=score,
                    status="completed",
                )
                record = result.to_dict()
                self.assertEqual(record["evaluator"], "faithfulness")
                self.assertEqual(record["evaluator_version"], "1")
                self.assertEqual(record["score_type"], score_type)
                self.assertEqual(record["score"], score)
                self.assertEqual(record["status"], "completed")

    def test_result_rejects_score_value_incompatible_with_score_type(self):
        EvaluationResult = public_symbol("EvaluationResult")

        with self.assertRaises((TypeError, ValueError)):
            EvaluationResult(
                evaluator="format-check",
                evaluator_version="1",
                score_type="boolean",
                score="yes",
                status="completed",
            )


if __name__ == "__main__":
    unittest.main()
