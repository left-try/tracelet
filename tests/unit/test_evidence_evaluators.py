import asyncio
import unittest

from tests._support import public_symbol


class EvidenceEvaluatorTests(unittest.TestCase):
    def test_citation_presence_is_not_reported_as_semantic_entailment(self):
        EvidenceReferenceEvaluator = public_symbol("EvidenceReferenceEvaluator")
        evaluator = EvidenceReferenceEvaluator()
        result = evaluator.evaluate_output("[doc-1] This proves the claim.", evidence_ids={"doc-1"})

        self.assertEqual(result.score_type, "boolean")
        self.assertTrue(result.score)
        self.assertEqual(result.label, "reference_present")
        self.assertIsNone(result.entailment)

    def test_semantic_evaluator_distinguishes_entailment_contradiction_and_unknown(self):
        EvidenceEvaluator = public_symbol("EvidenceEvaluator")
        verdicts = iter(["entailment", "contradiction", "unknown"])

        async def classify(answer, evidence):
            self.assertTrue(answer)
            self.assertTrue(evidence)
            return next(verdicts)

        evaluator = EvidenceEvaluator(classify=classify)
        results = [
            asyncio.run(evaluator.evaluate(answer="Claim", evidence=["Source"]))
            for _ in range(3)
        ]

        self.assertEqual([result.label for result in results], ["entailment", "contradiction", "unknown"])

    def test_semantic_evaluator_accepts_event_and_runs_in_pipeline(self):
        Event = public_symbol("Event")
        EvidenceEvaluator = public_symbol("EvidenceEvaluator")
        EvaluationPipeline = public_symbol("EvaluationPipeline")
        received = []

        async def classify(answer, evidence):
            received.append((answer, evidence))
            return "entailment"

        event = Event(
            request_id="evidence-event",
            input="Question",
            output="Supported answer",
            context=[{"source": "doc-1", "text": "Evidence text"}],
        )
        results = asyncio.run(EvaluationPipeline(deterministic=[EvidenceEvaluator(classify=classify)]).run(event))

        self.assertEqual(received, [("Supported answer", [{"source": "doc-1", "text": "Evidence text"}])])
        self.assertEqual(results[0].entailment, "entailment")


if __name__ == "__main__":
    unittest.main()
