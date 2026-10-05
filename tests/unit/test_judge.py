import asyncio
import unittest

from tests._support import public_symbol


class JudgeTests(unittest.TestCase):
    def test_user_callable_judge_returns_typed_score_without_provider_sdk(self):
        Event = public_symbol("Event")
        LLMJudge = public_symbol("LLMJudge")
        calls = []

        async def call_model(request):
            calls.append(request)
            return {"score": 0.9, "label": "supported", "reason": "Matches evidence."}

        judge = LLMJudge(call_model=call_model, rubric="Is the answer supported?", model="vendor/model")
        event = Event(request_id="judge-1", input="Where?", output="Here", context=["It is here."])
        result = asyncio.run(judge.evaluate(event))

        self.assertEqual(result.score_type, "numeric")
        self.assertEqual(result.score, 0.9)
        self.assertEqual(result.model, "vendor/model")
        self.assertEqual(calls[0]["rubric"], "Is the answer supported?")

    def test_malformed_judge_output_and_timeout_are_errors_not_passes(self):
        Event = public_symbol("Event")
        LLMJudge = public_symbol("LLMJudge")
        event = Event(request_id="judge-2", input="q", output="a")

        async def malformed(_request):
            return {"unexpected": "shape"}

        async def timeout(_request):
            raise TimeoutError("judge timed out")

        malformed_result = asyncio.run(LLMJudge(call_model=malformed, rubric="check").evaluate(event))
        timeout_result = asyncio.run(LLMJudge(call_model=timeout, rubric="check").evaluate(event))

        self.assertEqual(malformed_result.status, "error")
        self.assertNotEqual(malformed_result.score, True)
        self.assertEqual(timeout_result.status, "error")
        self.assertIn("timed out", timeout_result.error)

    def test_refusal_and_rate_limit_are_visible_errors(self):
        Event = public_symbol("Event")
        LLMJudge = public_symbol("LLMJudge")
        event = Event(request_id="judge-refusal", input="q", output="a")

        async def refusal(_request):
            return {"refusal": "judge policy refusal"}

        async def rate_limited(_request):
            raise RuntimeError("provider rate limit")

        refusal_result = asyncio.run(LLMJudge(call_model=refusal, rubric="check").evaluate(event))
        rate_result = asyncio.run(LLMJudge(call_model=rate_limited, rubric="check").evaluate(event))

        self.assertEqual(refusal_result.status, "error")
        self.assertIn("refusal", refusal_result.error.lower())
        self.assertEqual(rate_result.status, "error")
        self.assertIn("rate limit", rate_result.error.lower())

    def test_judge_records_model_version_latency_and_usage(self):
        Event = public_symbol("Event")
        LLMJudge = public_symbol("LLMJudge")

        async def call_model(_request):
            return {"score": True, "usage": {"input_tokens": 12, "output_tokens": 3}}

        result = asyncio.run(
            LLMJudge(call_model=call_model, rubric="check", model="judge-v2").evaluate(
                Event(request_id="judge-3", input="q", output="a")
            )
        )

        self.assertEqual(result.model, "judge-v2")
        self.assertEqual(result.usage["input_tokens"], 12)
        self.assertGreaterEqual(result.latency_ms, 0)


if __name__ == "__main__":
    unittest.main()
