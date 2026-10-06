import tempfile
import unittest
from pathlib import Path

from tests._support import public_symbol


class StorageContractTests(unittest.TestCase):
    def test_file_store_checkpoints_evaluator_results_until_terminal_transition(self):
        FileStore = public_symbol("FileStore")
        with tempfile.TemporaryDirectory() as directory:
            store = FileStore(Path(directory))
            store.enqueue("slot-job", {"input": "hello"})

            self.assertEqual(store.get_evaluator_checkpoints("slot-job"), {})
            with self.assertRaises(KeyError):
                store.checkpoint_evaluator("slot-job", "slot-0", result={"score": True})

            store.claim("slot-job", worker_id="worker")
            store.checkpoint_evaluator("slot-job", "slot-0", result={"score": True})
            self.assertEqual(store.get_evaluator_checkpoints("slot-job"), {"slot-0": {"score": True}})
            store.release("slot-job")
            restarted = FileStore(Path(directory))
            self.assertEqual(restarted.get_evaluator_checkpoints("slot-job"), {"slot-0": {"score": True}})

            restarted.claim("slot-job", worker_id="worker-2")
            restarted.complete("slot-job", result=[])
            self.assertEqual(restarted.get_evaluator_checkpoints("slot-job"), {})

    def test_adapter_validation_requires_every_worker_operation(self):
        validate = public_symbol("validate_storage_adapter")

        class IncompleteStore:
            def enqueue(self, job_id, payload): pass
            def get(self, job_id): pass
            def list_pending(self): pass

        self.assertFalse(validate(IncompleteStore()))

    def test_user_supplied_adapter_can_store_and_retrieve_jobs(self):
        class UserStore:
            def __init__(self):
                self.jobs = {}

            def enqueue(self, job_id, payload):
                self.jobs.setdefault(job_id, {"job_id": job_id, "payload": payload, "status": "pending"})

            def get(self, job_id):
                return self.jobs.get(job_id)

            def list_pending(self):
                return [job for job in self.jobs.values() if job["status"] == "pending"]

        store = UserStore()
        store.enqueue(job_id="contract-job", payload={"input": "hello"})

        self.assertEqual(store.get("contract-job")["payload"], {"input": "hello"})
        self.assertEqual([item["job_id"] for item in store.list_pending()], ["contract-job"])

    def test_file_store_obeys_shared_storage_contract(self):
        FileStore = public_symbol("FileStore")
        with tempfile.TemporaryDirectory() as directory:
            store = FileStore(Path(directory))
            store.enqueue(job_id="contract-file", payload={"input": "hello"})

            self.assertEqual(store.get("contract-file")["payload"], {"input": "hello"})
            self.assertEqual([item["job_id"] for item in store.list_pending()], ["contract-file"])


if __name__ == "__main__":
    unittest.main()
