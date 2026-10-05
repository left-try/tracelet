import tempfile
import unittest
from pathlib import Path
import os

from tests._support import public_symbol


class FileStoreTests(unittest.TestCase):
    def test_corrupt_encoded_job_is_quarantined_and_still_findable_by_original_id(self):
        FileStore = public_symbol("FileStore")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pending").mkdir()
            (root / "pending" / "a%2Fb.json").write_text("{broken", encoding="utf-8")
            store = FileStore(root)

            self.assertEqual(store.list_pending(), [])
            quarantined = store.get("a/b")

        self.assertIsNotNone(quarantined)
        self.assertEqual(quarantined["status"], "failed")

    def test_distinct_job_ids_do_not_collide_after_path_encoding(self):
        FileStore = public_symbol("FileStore")
        with tempfile.TemporaryDirectory() as directory:
            store = FileStore(Path(directory))
            store.enqueue("a/b", {"value": "slash"})
            store.enqueue("a_b", {"value": "underscore"})

            self.assertEqual(store.get("a/b")["payload"]["value"], "slash")
            self.assertEqual(store.get("a_b")["payload"]["value"], "underscore")

    def test_prune_removes_only_terminal_jobs_older_than_cutoff(self):
        FileStore = public_symbol("FileStore")
        with tempfile.TemporaryDirectory() as directory:
            store = FileStore(Path(directory))
            store.enqueue("old-complete", {"value": 1})
            store.claim("old-complete", worker_id="worker")
            store.complete("old-complete", result={"ok": True})
            store.enqueue("old-pending", {"value": 2})
            old = store._path("completed", "old-complete")
            os.utime(old, (100, 100))

            removed = store.prune(older_than_seconds=60, now=200)

            self.assertEqual(removed, 1)
            self.assertIsNone(store.get("old-complete"))
            self.assertEqual(store.get("old-pending")["status"], "pending")

    def test_enqueue_claim_complete_and_read_back_job(self):
        FileStore = public_symbol("FileStore")
        with tempfile.TemporaryDirectory() as directory:
            store = FileStore(Path(directory))
            store.enqueue(job_id="job-1", payload={"request_id": "r-1"})

            claimed = store.claim("job-1", worker_id="worker-a")
            store.complete("job-1", result={"score": True})
            saved = store.get("job-1")

        self.assertTrue(claimed)
        self.assertEqual(saved["status"], "completed")
        self.assertEqual(saved["result"], {"score": True})

    def test_same_job_id_is_idempotent_and_does_not_overwrite_payload(self):
        FileStore = public_symbol("FileStore")
        with tempfile.TemporaryDirectory() as directory:
            store = FileStore(Path(directory))
            store.enqueue(job_id="stable-id", payload={"answer": "first"})
            store.enqueue(job_id="stable-id", payload={"answer": "second"})

            saved = store.get("stable-id")

        self.assertEqual(saved["payload"], {"answer": "first"})

    def test_only_one_worker_can_claim_a_pending_job(self):
        FileStore = public_symbol("FileStore")
        with tempfile.TemporaryDirectory() as directory:
            store = FileStore(Path(directory))
            store.enqueue(job_id="exclusive-claim", payload={"request_id": "r-lock"})

            first_claim = store.claim("exclusive-claim", worker_id="worker-a")
            second_claim = store.claim("exclusive-claim", worker_id="worker-b")

        self.assertTrue(first_claim)
        self.assertFalse(second_claim)

    def test_incomplete_temp_file_is_not_returned_as_pending_job(self):
        FileStore = public_symbol("FileStore")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pending").mkdir()
            (root / "pending" / "unfinished.tmp").write_text('{"partial":', encoding="utf-8")
            store = FileStore(root)

            self.assertEqual(store.list_pending(), [])


if __name__ == "__main__":
    unittest.main()
