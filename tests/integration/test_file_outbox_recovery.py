import tempfile
import unittest
from pathlib import Path

from tests._support import public_symbol


class FileOutboxRecoveryTests(unittest.TestCase):
    def test_pending_job_is_recovered_by_new_store_instance(self):
        FileStore = public_symbol("FileStore")
        with tempfile.TemporaryDirectory() as directory:
            first = FileStore(Path(directory))
            first.enqueue(job_id="recover-me", payload={"request_id": "r-9"})
            recovered = FileStore(Path(directory))

            pending = recovered.list_pending()

        self.assertEqual([job["job_id"] for job in pending], ["recover-me"])

    def test_failed_job_keeps_attempt_count_and_error_for_inspection(self):
        FileStore = public_symbol("FileStore")
        with tempfile.TemporaryDirectory() as directory:
            store = FileStore(Path(directory))
            store.enqueue(job_id="job-fail", payload={"request_id": "r-10"})
            store.fail("job-fail", error="temporary sink outage", retryable=True)

            failed = store.get("job-fail")

        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["error"], "temporary sink outage")
        self.assertEqual(failed["attempts"], 1)

    def test_corrupt_job_is_quarantined_without_hiding_valid_pending_jobs(self):
        FileStore = public_symbol("FileStore")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pending").mkdir()
            (root / "pending" / "broken.json").write_text("not-json", encoding="utf-8")
            store = FileStore(root)
            store.enqueue(job_id="good", payload={"ok": True})

            pending = store.list_pending()
            quarantined = list((root / "failed").glob("*"))

        self.assertEqual([job["job_id"] for job in pending], ["good"])
        self.assertEqual(len(quarantined), 1)


if __name__ == "__main__":
    unittest.main()
