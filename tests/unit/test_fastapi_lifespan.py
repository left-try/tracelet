import asyncio
import importlib.util
import unittest

from tests._support import public_symbol


class FastAPILifespanTests(unittest.TestCase):
    def test_lifespan_starts_worker_on_enter_and_stops_it_on_exit(self):
        lifespan = public_symbol("worker_lifespan")

        class Worker:
            def __init__(self):
                self.started = False
                self.stopped = False

            async def start(self):
                self.started = True

            async def stop(self):
                self.stopped = True

        async def scenario():
            worker = Worker()
            async with lifespan(worker):
                self.assertTrue(worker.started)
                self.assertFalse(worker.stopped)
            self.assertTrue(worker.stopped)

        asyncio.run(scenario())

    @unittest.skipUnless(importlib.util.find_spec("fastapi"), "FastAPI extra is not installed")
    def test_fastapi_integration_is_optional_and_does_not_change_handler_response(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        attach_worker = public_symbol("attach_worker")

        class Worker:
            def __init__(self):
                self.started = False
                self.stopped = False

            async def start(self):
                self.started = True

            async def stop(self):
                self.stopped = True

        worker = Worker()
        app = FastAPI()
        attach_worker(app, worker)

        @app.get("/health")
        async def health():
            return {"status": "ok"}

        with TestClient(app) as client:
            response = client.get("/health")
            self.assertTrue(worker.started)
        self.assertEqual(response.json(), {"status": "ok"})
        self.assertTrue(worker.stopped)


if __name__ == "__main__":
    unittest.main()
