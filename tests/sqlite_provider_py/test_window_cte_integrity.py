from pathlib import Path

import pytest

from .conftest import SQLITE_DONE, open_db


def test_moving_average_sql_equals_python_oracle(tmp_path: Path) -> None:
    db_path = tmp_path / "analytics.db"
    values = [10.0, 12.0, 11.5, 13.0, 12.5, 14.0, 16.0, 15.5, 17.0, 18.0]

    with open_db(db_path) as conn:
        conn.exec("CREATE TABLE readings(ts_ms INTEGER PRIMARY KEY, value REAL NOT NULL)")
        with conn.prepare("INSERT INTO readings(ts_ms, value) VALUES (?, ?)") as stmt:
            for i, value in enumerate(values):
                stmt.bind_int64(1, 1_700_000_000_000 + i * 1000)
                stmt.bind_double(2, value)
                assert stmt.step() == SQLITE_DONE
                stmt.reset()

        sql_rows = conn.query(
            """
            WITH ordered AS (
                SELECT ts_ms, value
                FROM readings
                ORDER BY ts_ms
            )
            SELECT
                ts_ms,
                AVG(value) OVER (
                    ORDER BY ts_ms
                    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
                ) AS mov_avg
            FROM ordered
            ORDER BY ts_ms
            """
        )

    expected = []
    for i in range(len(values)):
        window = values[max(0, i - 5) : i + 1]
        expected.append(sum(window) / len(window))

    assert len(sql_rows) == len(expected)
    for (_, sql_avg), py_avg in zip(sql_rows, expected):
        assert sql_avg == pytest.approx(py_avg, rel=1e-9, abs=1e-9)
