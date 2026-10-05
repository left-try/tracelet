import unittest

from tests._support import public_symbol


class UserDatabaseAdapterTests(unittest.TestCase):
    def test_adapter_does_not_commit_or_rollback_application_owned_transaction(self):
        Tracelet = public_symbol("Tracelet")
        Event = public_symbol("Event")

        class ApplicationSession:
            def __init__(self):
                self.commits = 0
                self.rollbacks = 0
                self.rows = {}

            def add_outbox_record(self, job_id, payload):
                self.rows[job_id] = payload

            def get_outbox_record(self, job_id):
                return self.rows.get(job_id)

            def commit(self):
                self.commits += 1

            def rollback(self):
                self.rollbacks += 1

        session = ApplicationSession()
        class ExistingDatabaseAdapter:
            def enqueue(self, job_id, payload):
                session.add_outbox_record(job_id, payload)

            def get(self, job_id):
                return session.get_outbox_record(job_id)

            def list_pending(self):
                return []

        tracelet = Tracelet(storage=ExistingDatabaseAdapter())
        tracelet.record_nowait(Event(request_id="db-1", input="q", output="a"), job_id="db-job")

        payload = session.get_outbox_record("db-job")
        self.assertEqual(payload["request_id"], "db-1")
        self.assertEqual(payload["input"], "q")
        self.assertEqual(payload["output"], "a")
        self.assertEqual(payload["schema_version"], 1)
        self.assertEqual(session.commits, 0)
        self.assertEqual(session.rollbacks, 0)

    def test_consumer_supplied_storage_adapter_satisfies_storage_protocol(self):
        Tracelet = public_symbol("Tracelet")

        class ExistingApplicationStore:
            def enqueue(self, job_id, payload):
                return job_id

            def get(self, job_id):
                return {"job_id": job_id, "payload": {}}

            def list_pending(self):
                return []

            def claim(self, job_id, *, worker_id):
                return False

            def release(self, job_id):
                return None

            def checkpoint(self, job_id, *, result):
                return None

            def complete(self, job_id, *, result=None):
                return None

            def retry(self, job_id, *, error):
                return None

            def fail(self, job_id, *, error, retryable=False, increment_attempt=True):
                return None

        storage = ExistingApplicationStore()
        tracelet = Tracelet(storage=storage)

        self.assertIs(tracelet.storage, storage)
        self.assertTrue(public_symbol("validate_storage_adapter")(storage))


if __name__ == "__main__":
    unittest.main()
