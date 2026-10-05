import asyncio
import unittest

from tests._support import public_symbol


class S3SinkTests(unittest.TestCase):
    def test_sync_s3_client_runs_off_event_loop(self):
        import time
        S3Sink = public_symbol("S3Sink")
        class SlowClient:
            def put_object(self, **_kwargs):
                time.sleep(0.04)
        async def scenario():
            ticker = asyncio.create_task(asyncio.sleep(0.001))
            started = asyncio.get_running_loop().time()
            await S3Sink(client=SlowClient(), bucket="evals").write(job_id="x", record={})
            elapsed = asyncio.get_running_loop().time() - started
            self.assertTrue(ticker.done())
            self.assertGreaterEqual(elapsed, 0.03)
        asyncio.run(scenario())

    def test_s3_drain_worker_uploads_pending_jobs_and_completes_them(self):
        FileStore = public_symbol("FileStore")
        S3Sink = public_symbol("S3Sink")
        S3DrainWorker = public_symbol("S3DrainWorker")
        import tempfile

        class S3Client:
            def __init__(self):
                self.objects = []

            async def put_object(self, **kwargs):
                self.objects.append(kwargs)

        async def scenario(directory):
            store = FileStore(directory)
            store.enqueue("upload-1", {"score": 1})
            client = S3Client()
            drain = S3DrainWorker(store=store, sink=S3Sink(client=client, bucket="evals"))
            completed = await drain.drain_once()
            return store.get("upload-1"), client.objects, completed

        with tempfile.TemporaryDirectory() as directory:
            record, objects, completed = asyncio.run(scenario(directory))

        self.assertEqual(completed, 1)
        self.assertEqual(record["status"], "completed")
        self.assertEqual(objects[0]["Key"], "upload-1.json")

    def test_s3_sink_writes_schema_versioned_object_with_stable_key(self):
        S3Sink = public_symbol("S3Sink")

        class S3Client:
            def __init__(self):
                self.calls = []

            async def put_object(self, **kwargs):
                self.calls.append(kwargs)

        client = S3Client()
        sink = S3Sink(client=client, bucket="eval-archive", prefix="prod/evals")
        asyncio.run(sink.write(job_id="job-123", record={"schema_version": 1, "score": 0.8}))

        self.assertEqual(len(client.calls), 1)
        self.assertEqual(client.calls[0]["Bucket"], "eval-archive")
        self.assertEqual(client.calls[0]["Key"], "prod/evals/job-123.json")
        self.assertIn('"schema_version": 1', client.calls[0]["Body"].decode())

    def test_repeating_same_delivery_uses_same_object_key(self):
        S3Sink = public_symbol("S3Sink")

        class S3Client:
            def __init__(self):
                self.keys = []

            async def put_object(self, **kwargs):
                self.keys.append(kwargs["Key"])

        client = S3Client()
        sink = S3Sink(client=client, bucket="eval-archive", prefix="prod")
        asyncio.run(sink.write(job_id="stable", record={"score": 1}))
        asyncio.run(sink.write(job_id="stable", record={"score": 1}))

        self.assertEqual(client.keys, ["prod/stable.json", "prod/stable.json"])

    def test_failed_s3_upload_leaves_outbox_job_retryable(self):
        FileStore = public_symbol("FileStore")
        S3Sink = public_symbol("S3Sink")

        class BrokenS3:
            async def put_object(self, **kwargs):
                raise TimeoutError("S3 unavailable")

        async def scenario(directory):
            store = FileStore(directory)
            store.enqueue(job_id="upload-later", payload={"score": True})
            sink = S3Sink(client=BrokenS3(), bucket="eval-archive")
            with self.assertRaises(TimeoutError):
                await sink.deliver_from_outbox(store, "upload-later")
            return store.get("upload-later")

        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as directory:
            job = asyncio.run(scenario(Path(directory)))

        self.assertEqual(job["status"], "pending")
        self.assertGreaterEqual(job["attempts"], 1)

    def test_stopping_during_s3_upload_releases_claim_for_retry(self):
        FileStore = public_symbol("FileStore")
        S3Sink = public_symbol("S3Sink")
        S3DrainWorker = public_symbol("S3DrainWorker")
        import tempfile

        async def scenario(directory):
            store = FileStore(directory)
            store.enqueue("cancel-upload", {"score": 1})
            entered = asyncio.Event()

            class SlowS3:
                async def put_object(self, **kwargs):
                    entered.set()
                    await asyncio.Event().wait()

            drain = S3DrainWorker(store=store, sink=S3Sink(client=SlowS3(), bucket="evals"))
            task = asyncio.create_task(drain.drain_once())
            await asyncio.wait_for(entered.wait(), timeout=1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            return store.get("cancel-upload")

        with tempfile.TemporaryDirectory() as directory:
            record = asyncio.run(scenario(directory))

        self.assertEqual(record["status"], "pending")

    def test_s3_retry_limit_counts_each_upload_once(self):
        FileStore = public_symbol("FileStore")
        S3Sink = public_symbol("S3Sink")
        S3DrainWorker = public_symbol("S3DrainWorker")
        import tempfile

        class BrokenS3:
            async def put_object(self, **kwargs):
                raise TimeoutError("unavailable")

        async def scenario(directory):
            store = FileStore(directory)
            store.enqueue("retry-limit", {"score": 1})
            drain = S3DrainWorker(
                store=store,
                sink=S3Sink(client=BrokenS3(), bucket="evals"),
                max_attempts=1,
            )
            await drain.drain_once()
            return store.get("retry-limit")

        with tempfile.TemporaryDirectory() as directory:
            record = asyncio.run(scenario(directory))

        self.assertEqual(record["status"], "failed")
        self.assertEqual(record["attempts"], 1)

    def test_s3_drain_recovers_after_transient_store_poll_error(self):
        S3Sink = public_symbol("S3Sink")
        S3DrainWorker = public_symbol("S3DrainWorker")

        class FlakyStore:
            def __init__(self):
                self.polls = 0
                self.recovered = asyncio.Event()
            async def list_pending(self):
                self.polls += 1
                if self.polls == 1:
                    raise TimeoutError("temporary store error")
                self.recovered.set()
                return []

        class Client:
            async def put_object(self, **kwargs):
                return None

        async def scenario():
            store = FlakyStore()
            worker = S3DrainWorker(
                store=store,
                sink=S3Sink(client=Client(), bucket="evals"),
                poll_interval=0.01,
            )
            await worker.start()
            await asyncio.wait_for(store.recovered.wait(), timeout=1)
            await worker.stop()
            return worker.last_error

        last_error = asyncio.run(scenario())
        self.assertIsNone(last_error)


if __name__ == "__main__":
    unittest.main()
