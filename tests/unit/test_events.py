"""Behavioral contract for provider-neutral evaluation events."""

import json
import unittest
from tests._support import public_symbol


class EventTests(unittest.TestCase):
    def test_event_round_trips_json_with_model_context_and_metadata(self):
        Event = public_symbol("Event")
        event = Event(
            request_id="req-123",
            input={"question": "Where is Bishkek?"},
            output="In Kyrgyzstan.",
            context=[{"source": "doc-1", "text": "Bishkek is in Kyrgyzstan."}],
            model="provider/model-v1",
            prompt_version="support-v3",
            metadata={"route": "support"},
        )

        encoded = event.to_json()
        record = json.loads(encoded)

        self.assertEqual(record["request_id"], "req-123")
        self.assertEqual(record["input"], {"question": "Where is Bishkek?"})
        self.assertEqual(record["output"], "In Kyrgyzstan.")
        self.assertEqual(record["context"][0]["source"], "doc-1")
        self.assertEqual(record["model"], "provider/model-v1")
        self.assertEqual(record["prompt_version"], "support-v3")
        self.assertEqual(record["metadata"]["route"], "support")
        self.assertEqual(record["schema_version"], 1)

        restored = Event.from_json(encoded)
        self.assertEqual(restored.to_dict(), record)

    def test_event_requires_request_input_and_output(self):
        Event = public_symbol("Event")

        with self.assertRaises((TypeError, ValueError)):
            Event(request_id="", input=None, output=None)

    def test_event_refuses_to_stringify_non_json_metadata(self):
        Event = public_symbol("Event")
        event = Event(request_id="req-object", input="q", output="a", metadata={"opaque": object()})

        with self.assertRaises(TypeError):
            event.to_json()


if __name__ == "__main__":
    unittest.main()
