"""Privacy contract: configured redaction happens before event serialization."""

import unittest
import asyncio
import tempfile
from pathlib import Path
from tests._support import public_symbol


class RedactionTests(unittest.TestCase):
    def test_builtin_secret_detection_redacts_nested_credentials(self):
        RedactionPolicy = public_symbol("RedactionPolicy")
        policy = RedactionPolicy()
        record = policy.apply({
            "input": "authorization: Bearer ghp_123456789012345678901234567890123456",
            "metadata": {
                "credentials": [
                    "api_key=sk-proj-1234567890abcdefghijklmnop",
                    "AKIAIOSFODNN7EXAMPLE",
                ]
            },
            "notes": "The secret sauce is a good prompt.",
        })

        self.assertNotIn("ghp_123456789012345678901234567890123456", str(record))
        self.assertNotIn("sk-proj-1234567890abcdefghijklmnop", str(record))
        self.assertNotIn("AKIAIOSFODNN7EXAMPLE", str(record))
        self.assertIn("secret sauce", record["notes"])

    def test_private_key_block_is_redacted_recursively(self):
        RedactionPolicy = public_symbol("RedactionPolicy")
        private_key = "-----BEGIN PRIVATE KEY-----\nsecret-material\n-----END PRIVATE KEY-----"

        sanitized = RedactionPolicy().sanitize([{"pem": private_key}])

        self.assertNotIn("secret-material", str(sanitized))
        self.assertIn("[REDACTED]", sanitized[0]["pem"])

    def test_custom_patterns_work_when_builtin_detection_is_disabled(self):
        RedactionPolicy = public_symbol("RedactionPolicy")
        policy = RedactionPolicy(detect_secrets=False, custom_patterns=(r"CUSTOM-[A-Z0-9]{8}",))

        sanitized = policy.sanitize({"text": "sk-proj-1234567890abcdefghijklmnop CUSTOM-ABCD1234"})

        self.assertIn("sk-proj-1234567890abcdefghijklmnop", sanitized["text"])
        self.assertNotIn("CUSTOM-ABCD1234", sanitized["text"])
        self.assertIn("[REDACTED]", sanitized["text"])

    def test_tracelet_uses_secret_detection_by_default(self):
        Event = public_symbol("Event")
        Tracelet = public_symbol("Tracelet")

        class Store:
            def __init__(self):
                self.payload = None

            async def enqueue(self, job_id, payload):
                self.payload = payload

        store = Store()
        tracelet = Tracelet(storage=store)
        secret = "sk-proj-1234567890abcdefghijklmnop"

        asyncio.run(tracelet.record(Event(request_id="default-redaction", input=secret, output="ok")))

        self.assertNotIn(secret, str(store.payload))
        self.assertIn("[REDACTED]", str(store.payload))

    def test_async_record_awaits_nonblocking_async_storage_adapter(self):
        Event = public_symbol("Event")
        Tracelet = public_symbol("Tracelet")

        class SlowStore:
            async def enqueue(self, job_id, payload):
                await asyncio.sleep(0.01)

        async def scenario():
            tracelet = Tracelet(storage=SlowStore())
            ticked = asyncio.Event()
            record_task = asyncio.create_task(
                tracelet.record(Event(request_id="nonblocking", input="q", output="a"))
            )
            asyncio.get_running_loop().call_later(0.001, ticked.set)
            await asyncio.wait_for(ticked.wait(), timeout=0.02)
            await record_task

        asyncio.run(scenario())

    def test_configured_sensitive_fields_are_absent_from_serialized_event(self):
        Event = public_symbol("Event")
        RedactionPolicy = public_symbol("RedactionPolicy")
        event = Event(
            request_id="req-private",
            input={"text": "Contact me at user@example.com"},
            output="I will contact user@example.com",
            metadata={"api_key": "sk-test-secret", "team": "support"},
        )
        policy = RedactionPolicy(
            redact_fields={"input.text", "output"},
            exclude_fields={"metadata.api_key"},
            replacement="[REDACTED]",
        )

        record = event.to_dict(redaction=policy)
        serialized = str(record)

        self.assertNotIn("user@example.com", serialized)
        self.assertNotIn("sk-test-secret", serialized)
        self.assertEqual(record["input"]["text"], "[REDACTED]")
        self.assertEqual(record["output"], "[REDACTED]")
        self.assertNotIn("api_key", record["metadata"])
        self.assertEqual(record["metadata"]["team"], "support")

    def test_redaction_happens_before_event_is_written_to_storage(self):
        Event = public_symbol("Event")
        RedactionPolicy = public_symbol("RedactionPolicy")
        Tracelet = public_symbol("Tracelet")
        FileStore = public_symbol("FileStore")
        event = Event(
            request_id="private-storage",
            input="question with private data",
            output="answer",
            metadata={"token": "top-secret"},
        )
        policy = RedactionPolicy(exclude_fields={"metadata.token"})

        async def record(directory):
            store = FileStore(Path(directory))
            tracelet = Tracelet(storage=store, redaction=policy)
            await tracelet.record(event, evaluators=[])
            return store.list_pending()

        with tempfile.TemporaryDirectory() as directory:
            pending = asyncio.run(record(directory))

        self.assertNotIn("top-secret", str(pending))
        self.assertNotIn("token", str(pending[0]["payload"]["event"]["metadata"]))


if __name__ == "__main__":
    unittest.main()
