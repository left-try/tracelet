import unittest

from tests._support import public_symbol


class DeterministicEvaluatorTests(unittest.TestCase):
    def test_json_schema_check_accepts_valid_json_and_rejects_missing_required_key(self):
        JsonSchemaEvaluator = public_symbol("JsonSchemaEvaluator")
        evaluator = JsonSchemaEvaluator(schema={"type": "object", "required": ["answer"]})

        valid = evaluator.evaluate_output('{"answer":"ok"}')
        invalid = evaluator.evaluate_output('{"other":"ok"}')

        self.assertTrue(valid.score)
        self.assertFalse(invalid.score)

    def test_regex_required_forbidden_and_length_checks_cover_boundaries(self):
        RegexEvaluator = public_symbol("RegexEvaluator")
        RequiredTextEvaluator = public_symbol("RequiredTextEvaluator")
        ForbiddenTextEvaluator = public_symbol("ForbiddenTextEvaluator")
        LengthEvaluator = public_symbol("LengthEvaluator")
        RangeEvaluator = public_symbol("RangeEvaluator")
        self.assertTrue(RegexEvaluator(pattern=r"^ID-[0-9]{2}$").evaluate_output("ID-42").score)
        self.assertTrue(RequiredTextEvaluator(required=["refund", "30 days"]).evaluate_output("Refunds: 30 days").score)
        self.assertFalse(ForbiddenTextEvaluator(forbidden=["guaranteed"]).evaluate_output("guaranteed result").score)
        self.assertTrue(LengthEvaluator(max_chars=3).evaluate_output("abc").score)
        self.assertFalse(LengthEvaluator(max_chars=3).evaluate_output("abcd").score)
        self.assertTrue(RangeEvaluator(minimum=0, maximum=1).evaluate_output(0.5).score)
        self.assertFalse(RangeEvaluator(minimum=0, maximum=1).evaluate_output(2).score)

    def test_exact_match_handles_unicode_and_empty_strings(self):
        ExactMatchEvaluator = public_symbol("ExactMatchEvaluator")
        evaluator = ExactMatchEvaluator(expected="Салам 🌍")
        self.assertTrue(evaluator.evaluate_output("Салам 🌍").score)
        self.assertFalse(evaluator.evaluate_output("").score)

    def test_evidence_reference_check_only_claims_reference_presence(self):
        EvidenceReferenceEvaluator = public_symbol("EvidenceReferenceEvaluator")
        evaluator = EvidenceReferenceEvaluator()
        present = evaluator.evaluate_output("Claim [doc-1]", evidence_ids={"doc-1"})
        missing = evaluator.evaluate_output("Claim [doc-2]", evidence_ids={"doc-1"})

        self.assertTrue(present.score)
        self.assertFalse(missing.score)
        self.assertIn("reference", present.details.lower())


if __name__ == "__main__":
    unittest.main()
