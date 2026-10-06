import unittest

from tests._support import public_symbol


class SQLAlchemyStoreTests(unittest.TestCase):
    def test_per_evaluator_checkpoints_survive_release_and_clear_on_terminal_state(self):
        try:
            import sqlalchemy as sa
        except ImportError:
            self.skipTest("SQLAlchemy optional extra is not installed")
        SQLAlchemyStore = public_symbol("SQLAlchemyStore")
        store = SQLAlchemyStore(sa.create_engine("sqlite:///:memory:"))
        store.enqueue("slot-sql", {"request_id": "slot-sql"})
        self.assertEqual(store.get_evaluator_checkpoints("slot-sql"), {})
        with self.assertRaises(KeyError):
            store.checkpoint_evaluator("slot-sql", "eval-0", result={"score": 1})

        store.claim("slot-sql", worker_id="worker")
        store.checkpoint_evaluator("slot-sql", "eval-0", result={"score": 1})
        store.release("slot-sql")
        self.assertEqual(store.get_evaluator_checkpoints("slot-sql"), {"eval-0": {"score": 1}})

        store.claim("slot-sql", worker_id="worker-2")
        store.fail("slot-sql", error="terminal")
        self.assertEqual(store.get_evaluator_checkpoints("slot-sql"), {})

    def test_sqlalchemy_store_outbox_contract(self):
        try:
            import sqlalchemy as sa
        except ImportError:
            self.skipTest("SQLAlchemy optional extra is not installed")
        SQLAlchemyStore = public_symbol("SQLAlchemyStore")
        store = SQLAlchemyStore(sa.create_engine("sqlite:///:memory:"))
        store.enqueue("sql-1", {"request_id": "sql-1"})
        self.assertEqual(store.get("sql-1")["status"], "pending")
        self.assertTrue(store.claim("sql-1", worker_id="worker"))
        store.checkpoint("sql-1", result={"score": True})
        store.complete("sql-1", result={"score": True})
        self.assertEqual(store.get("sql-1")["result"], {"score": True})
        self.assertFalse(store.claim("sql-1", worker_id="worker-2"))


if __name__ == "__main__":
    unittest.main()
