import unittest
import asyncio

from tests._support import public_symbol


class RetryPolicyTests(unittest.TestCase):
    def test_transient_failure_retries_only_up_to_configured_attempt_limit(self):
        RetryPolicy = public_symbol("RetryPolicy")
        policy = RetryPolicy(max_attempts=3, initial_delay=0, max_delay=0)
        attempts = []

        async def operation():
            attempts.append(None)
            raise TimeoutError("temporary")

        async def scenario():
            with self.assertRaises(TimeoutError):
                await policy.run(operation)

        asyncio.run(scenario())

        self.assertEqual(len(attempts), 3)


if __name__ == "__main__":
    unittest.main()
