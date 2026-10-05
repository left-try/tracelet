import tempfile
import unittest
from pathlib import Path

from tests._support import public_symbol


class StorageContractTests(unittest.TestCase):
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
