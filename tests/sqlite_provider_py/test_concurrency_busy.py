import threading
import time
from pathlib import Path

from .conftest import SqliteError, open_db, retry_locked


def test_busy_timeout_plus_retry_under_write_contention(tmp_path: Path) -> None:
    db_path = tmp_path / "busy.db"
    with open_db(db_path) as conn:
        conn.exec("CREATE TABLE events(id INTEGER PRIMARY KEY, src TEXT NOT NULL)")

    start = threading.Event()
    retries_seen: list[int] = []
    retries_lock = threading.Lock()

    def lock_holder() -> None:
        conn = open_db(db_path)
        try:
            conn.exec("BEGIN IMMEDIATE;")
            conn.exec("INSERT INTO events(src) VALUES ('holder')")
            start.set()
            time.sleep(0.2)
            conn.exec("COMMIT;")
        finally:
            conn.close()

    def writer(i: int) -> None:
        start.wait(timeout=2.0)
        conn = open_db(db_path)
        attempts = {"busy_retries": 0}

        def do_insert() -> int:
            try:
                conn.exec("BEGIN IMMEDIATE;")
                conn.exec(f"INSERT INTO events(src) VALUES ('w{i}')")
                conn.exec("COMMIT;")
                return attempts["busy_retries"]
            except SqliteError as exc:
                try:
                    conn.exec("ROLLBACK;")
                except SqliteError:
                    pass
                if "locked" in exc.message.lower():
                    attempts["busy_retries"] += 1
                raise

        retry_count = retry_locked(do_insert)
        with retries_lock:
            retries_seen.append(retry_count)
        conn.close()

    holder = threading.Thread(target=lock_holder, daemon=True)
    workers = [threading.Thread(target=writer, args=(i,), daemon=True) for i in range(8)]

    holder.start()
    for worker in workers:
        worker.start()
    holder.join(timeout=5.0)
    for worker in workers:
        worker.join(timeout=5.0)

    assert not holder.is_alive()
    assert all(not worker.is_alive() for worker in workers)

    with open_db(db_path) as conn:
        total = conn.query_one("SELECT COUNT(*) FROM events")[0]

    assert total == 9
    assert any(retries > 0 for retries in retries_seen)
