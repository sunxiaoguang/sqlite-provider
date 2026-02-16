from pathlib import Path

import pytest

from .conftest import open_db


def test_exception_inside_transaction_rolls_back_all_changes(tmp_path: Path) -> None:
    db_path = tmp_path / "atomicity.db"
    with open_db(db_path, synchronous="FULL") as conn:
        conn.exec(
            """
            CREATE TABLE queue(
                id INTEGER PRIMARY KEY,
                state TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        conn.exec(
            """
            INSERT INTO queue(id, state, attempts) VALUES
                (1, 'pending', 0),
                (2, 'pending', 0),
                (3, 'pending', 0);
            """
        )

        with pytest.raises(RuntimeError, match="simulated power failure"):
            conn.exec("BEGIN;")
            try:
                conn.exec("UPDATE queue SET state='processing', attempts=attempts+1 WHERE id IN (1,2)")
                conn.exec("DELETE FROM queue WHERE id=3")
                raise RuntimeError("simulated power failure")
            except Exception:
                conn.exec("ROLLBACK;")
                raise

        rows = conn.query("SELECT id, state, attempts FROM queue ORDER BY id")
        assert rows == [(1, "pending", 0), (2, "pending", 0), (3, "pending", 0)]
        assert conn.query_one("PRAGMA integrity_check")[0] == "ok"
