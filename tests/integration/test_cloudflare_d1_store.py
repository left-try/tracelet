import asyncio
import sqlite3
import time
import unittest

from tests._support import public_symbol


class D1Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class SQLiteD1Transport:
    """Execute submitted SQL locally and return Cloudflare D1-shaped responses."""

    def __init__(self):
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row
        self.requests = []

    async def post(self, url, *, json, headers=None):
        self.requests.append((url, json, headers))
        sql = json["sql"]
        params = json.get("params", [])
        cursor = self.connection.execute(sql, params)
        rows = [dict(row) for row in cursor.fetchall()] if cursor.description else []
        self.connection.commit()
        return D1Response({
            "success": True,
            "result": [{"success": True, "results": rows, "meta": {"changes": cursor.rowcount}}],
            "errors": [],
        })


class CloudflareD1StoreTests(unittest.TestCase):
    def test_per_evaluator_checkpoints_survive_recovery_and_clear_on_completion(self):
        CloudflareD1Store = public_symbol("CloudflareD1Store")
        client = SQLiteD1Transport()
        store = CloudflareD1Store(client=client, query_url="https://d1.example/query")

        async def scenario():
            await store.initialize()
            await store.enqueue("slot-d1", {"request_id": "slot-d1"})
            self.assertEqual(await store.get_evaluator_checkpoints("slot-d1"), {})
            with self.assertRaises(KeyError):
                await store.checkpoint_evaluator("slot-d1", "eval-0", result={"score": 1})

            await store.claim("slot-d1", worker_id="worker")
            await store.checkpoint_evaluator("slot-d1", "eval-0", result={"score": 1})
            await store.release("slot-d1")
            restarted = CloudflareD1Store(client=client, query_url="https://d1.example/query")
            await restarted.initialize()
            self.assertEqual(await restarted.get_evaluator_checkpoints("slot-d1"), {"eval-0": {"score": 1}})

            await restarted.claim("slot-d1", worker_id="worker-2")
            await restarted.complete("slot-d1", result=[])
            self.assertEqual(await restarted.get_evaluator_checkpoints("slot-d1"), {})

        asyncio.run(scenario())

    def test_store_persists_and_transitions_jobs_using_async_d1_queries(self):
        CloudflareD1Store = public_symbol("CloudflareD1Store")
        client = SQLiteD1Transport()
        store = CloudflareD1Store(
            client=client,
            query_url="https://api.example.test/d1/query",
            api_token="test-token",
        )

        async def scenario():
            await store.initialize()
            await store.enqueue("job-1", {"input": "first"})
            await store.enqueue("job-1", {"input": "duplicate"})
            self.assertEqual((await store.get("job-1"))["payload"], {"input": "first"})
            self.assertEqual([job["job_id"] for job in await store.list_pending()], ["job-1"])
            self.assertTrue(await store.claim("job-1", worker_id="worker-a"))
            self.assertFalse(await store.claim("job-1", worker_id="worker-b"))
            await store.checkpoint("job-1", result={"score": 1})
            await store.release("job-1")
            released = await store.get("job-1")
            self.assertEqual(released["status"], "pending")
            self.assertEqual(released["result_checkpoint"], {"score": 1})
            self.assertTrue(await store.claim("job-1", worker_id="worker-a"))
            await store.complete("job-1", result={"score": 1})
            completed = await store.get("job-1")
            self.assertEqual(completed["status"], "completed")
            self.assertEqual(completed["result"], {"score": 1})
            self.assertNotIn("result_checkpoint", completed)

        asyncio.run(scenario())
        self.assertTrue(all(url == "https://api.example.test/d1/query" for url, _, _ in client.requests))
        self.assertTrue(all(headers["Authorization"] == "Bearer test-token" for _, _, headers in client.requests))
        self.assertTrue(all(isinstance(value, str) for _, body, _ in client.requests for value in body["params"]))

    def test_job_ids_are_bound_as_parameters_not_interpolated_into_sql(self):
        CloudflareD1Store = public_symbol("CloudflareD1Store")
        client = SQLiteD1Transport()
        store = CloudflareD1Store(client=client, query_url="https://d1.example/query")
        job_id = "id' OR 1=1 --"

        async def scenario():
            await store.initialize()
            await store.enqueue(job_id, {"ok": True})
            self.assertEqual((await store.get(job_id))["job_id"], job_id)
            self.assertIsNone(await store.get("other"))

        asyncio.run(scenario())
        select_calls = [(sql, params) for _, body, _ in client.requests
                        if (sql := body["sql"]).startswith("SELECT job_id")
                        for params in [body.get("params", [])]]
        self.assertTrue(any(params == [job_id] and "?" in sql for sql, params in select_calls))

    def test_initialize_recovers_claims_left_by_a_previous_single_worker_process(self):
        CloudflareD1Store = public_symbol("CloudflareD1Store")
        EvaluationWorker = public_symbol("EvaluationWorker")
        client = SQLiteD1Transport()
        previous = CloudflareD1Store(client=client, query_url="https://d1.example/query")
        restarted = CloudflareD1Store(client=client, query_url="https://d1.example/query")

        async def scenario():
            await previous.initialize()
            await previous.enqueue("abandoned", {"input": "q"})
            self.assertTrue(await previous.claim("abandoned", worker_id="old-process"))
            await previous.checkpoint("abandoned", result={"score_type": "boolean", "score": True})
            await restarted.initialize()
            recovered = await restarted.get("abandoned")
            self.assertEqual(recovered["status"], "pending")
            self.assertIsNone(recovered["worker_id"])
            self.assertEqual([item["job_id"] for item in await restarted.list_pending()], ["abandoned"])

            async def evaluator_must_not_run(_event):
                raise AssertionError("worker should reuse the persisted evaluation checkpoint")

            worker = EvaluationWorker(store=restarted, evaluator=evaluator_must_not_run, poll_interval=0.002)
            await worker.start()
            await worker.wait_idle(timeout=1)
            await worker.stop()
            completed = await restarted.get("abandoned")
            self.assertEqual(completed["status"], "completed")
            self.assertTrue(completed["result"]["score"])

        asyncio.run(scenario())

    def test_evaluation_worker_runs_jobs_through_the_async_d1_storage_contract(self):
        CloudflareD1Store = public_symbol("CloudflareD1Store")
        Event = public_symbol("Event")
        EvaluationWorker = public_symbol("EvaluationWorker")
        store = CloudflareD1Store(client=SQLiteD1Transport(), query_url="https://d1.example/query")

        async def scenario():
            await store.initialize()

            async def evaluator(_event):
                return {"score_type": "boolean", "score": True}

            worker = EvaluationWorker(store=store, evaluator=evaluator, poll_interval=0.002)
            await worker.enqueue(Event(request_id="d1-worker", input="q", output="a"))
            await worker.start()
            await worker.wait_idle(timeout=1)
            await worker.stop()
            record = await store.get("d1-worker")
            self.assertEqual(record["status"], "completed")
            self.assertTrue(record["result"]["score"])

        asyncio.run(scenario())

    def test_cloudflare_api_errors_are_not_reported_as_successful_storage(self):
        CloudflareD1Store = public_symbol("CloudflareD1Store")

        class ErrorClient:
            async def post(self, *_args, **_kwargs):
                return D1Response({"success": False, "errors": [{"message": "database unavailable"}], "result": []})

        store = CloudflareD1Store(client=ErrorClient(), query_url="https://d1.example/query")
        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            asyncio.run(store.initialize())

    def test_worker_proxy_uses_query_field_and_accepts_binding_result_shape(self):
        CloudflareD1Store = public_symbol("CloudflareD1Store")

        class WorkerProxyTransport(SQLiteD1Transport):
            async def post(self, url, *, json, headers=None):
                self.requests.append((url, json, headers))
                sql = json["query"]
                cursor = self.connection.execute(sql, json.get("params", []))
                rows = [dict(row) for row in cursor.fetchall()] if cursor.description else []
                self.connection.commit()
                return D1Response({"success": True, "results": rows, "meta": {"changes": cursor.rowcount}})

        client = WorkerProxyTransport()
        store = CloudflareD1Store(
            client=client,
            query_url="https://d1-proxy.example.test/api/all",
            api_style="worker-proxy",
        )

        async def scenario():
            await store.initialize()
            await store.enqueue("worker-job", {"source": "proxy"})
            return await store.get("worker-job")

        record = asyncio.run(scenario())
        self.assertEqual(record["payload"], {"source": "proxy"})
        self.assertTrue(all("query" in body and "sql" not in body for _, body, _ in client.requests))

    def test_retry_and_terminal_failure_update_attempts_and_status(self):
        CloudflareD1Store = public_symbol("CloudflareD1Store")
        store = CloudflareD1Store(client=SQLiteD1Transport(), query_url="https://d1.example/query")

        async def scenario():
            await store.initialize()
            await store.enqueue("retry-job", {"input": "q"})
            await store.retry("retry-job", error="temporary")
            retried = await store.get("retry-job")
            self.assertEqual(retried["status"], "pending")
            self.assertEqual(retried["attempts"], 1)
            self.assertEqual(retried["error"], "temporary")
            await store.fail("retry-job", error="terminal", increment_attempt=False)
            failed = await store.get("retry-job")
            self.assertEqual(failed["status"], "failed")
            self.assertEqual(failed["attempts"], 1)
            self.assertEqual(failed["error"], "terminal")

        asyncio.run(scenario())

    def test_pending_query_bounds_rows_returned_to_the_worker(self):
        CloudflareD1Store = public_symbol("CloudflareD1Store")
        store = CloudflareD1Store(
            client=SQLiteD1Transport(), query_url="https://d1.example/query", pending_limit=1
        )

        async def scenario():
            await store.initialize()
            await store.enqueue("first", {"n": 1})
            await store.enqueue("second", {"n": 2})
            self.assertEqual([row["job_id"] for row in await store.list_pending()], ["first"])

        asyncio.run(scenario())

    def test_sync_http_client_does_not_block_the_event_loop(self):
        CloudflareD1Store = public_symbol("CloudflareD1Store")

        class SlowSyncClient:
            def post(self, *_args, **_kwargs):
                time.sleep(0.03)
                return D1Response({"success": True, "result": [{"success": True, "results": []}]})

        async def scenario():
            ticker = asyncio.create_task(asyncio.sleep(0.001))
            await CloudflareD1Store(client=SlowSyncClient(), query_url="https://d1.example/query").initialize()
            self.assertTrue(ticker.done())

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
