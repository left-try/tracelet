import asyncio
import unittest

from tests._support import public_symbol


class PairwiseEvaluationTests(unittest.TestCase):
    def test_pairwise_judge_returns_a_b_or_tie_and_keeps_rationale(self):
        PairwiseEvaluator = public_symbol("PairwiseEvaluator")
        async def judge(request):
            self.assertEqual(request["candidate_a"], "Answer A")
            self.assertEqual(request["candidate_b"], "Answer B")
            return {"winner": "B", "rationale": "B cites the supplied evidence."}

        result = asyncio.run(
            PairwiseEvaluator(judge=judge).compare(
                input="Which city?", candidate_a="Answer A", candidate_b="Answer B"
            )
        )

        self.assertEqual(result.winner, "B")
        self.assertEqual(result.rationale, "B cites the supplied evidence.")

    def test_order_swap_keeps_both_verdicts_and_reports_final_tie(self):
        PairwiseEvaluator = public_symbol("PairwiseEvaluator")
        calls = []

        async def biased_judge(request):
            calls.append((request["candidate_a"], request["candidate_b"]))
            return {"winner": "A"}

        result = asyncio.run(
            PairwiseEvaluator(judge=biased_judge, swap_order=True).compare(
                input="q", candidate_a="Alpha", candidate_b="Beta"
            )
        )

        self.assertEqual(calls, [("Alpha", "Beta"), ("Beta", "Alpha")])
        self.assertEqual(result.winner, "tie")
        self.assertEqual(result.verdicts, ["A", "A"])


if __name__ == "__main__":
    unittest.main()
