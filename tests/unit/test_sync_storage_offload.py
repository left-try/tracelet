import asyncio
import time
import unittest

from tests._support import public_symbol


class SyncStorageOffloadTests(unittest.TestCase):
    def test_sync_adapter_io_does_not_block_event_loop(self):
        from tracelet.storage.protocol import call_storage

        class SlowStore:
            def read(self):
                time.sleep(0.04)
                return "done"

        async def scenario():
            ticker = asyncio.create_task(asyncio.sleep(0.001))
            self.assertEqual(await call_storage(SlowStore(), "read"), "done")
            self.assertTrue(ticker.done())

        asyncio.run(scenario())

    def test_file_store_can_be_used_through_nonblocking_adapter_call(self):
        import tempfile
        FileStore = public_symbol("FileStore")
        from tracelet.storage.protocol import call_storage

        async def scenario():
            with tempfile.TemporaryDirectory() as directory:
                store = FileStore(directory)
                await call_storage(store, "enqueue", "async-io", {"input": "q"})
                row = await call_storage(store, "get", "async-io")
                self.assertEqual(row["payload"]["input"], "q")

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
