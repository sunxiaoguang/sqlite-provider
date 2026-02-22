import json
from pathlib import Path

from .conftest import SQLITE_DONE, SqliteError, open_db, skip_test


def test_json1_deep_filter_partial_update_and_index_usage(tmp_path: Path) -> None:
    db_path = tmp_path / "json1.db"
    with open_db(db_path) as conn:
        try:
            conn.query_one("SELECT json_valid('{\"k\":1}')")
        except SqliteError:
            skip_test("JSON1 not available")

        conn.exec(
            """
            CREATE TABLE sensor_events(
                id INTEGER PRIMARY KEY,
                device_id TEXT NOT NULL,
                metadata TEXT NOT NULL CHECK(json_valid(metadata))
            );
            """
        )
        conn.exec(
            """
            CREATE INDEX idx_sensor0_status
            ON sensor_events(json_extract(metadata, '$.sensors[0].status'));
            """
        )

        rows = [
            (
                "dev-a",
                json.dumps(
                    {
                        "sensors": [
                            {"id": "temp-1", "status": "active", "threshold": {"high": 81.5}},
                            {"id": "vibe-1", "status": "inactive"},
                        ],
                        "sync": {"version": 7},
                    }
                ),
            ),
            (
                "dev-b",
                json.dumps(
                    {
                        "sensors": [{"id": "temp-2", "status": "inactive"}],
                        "sync": {"version": 3},
                    }
                ),
            ),
        ]
        for i in range(200):
            rows.append(
                (
                    f"dev-extra-{i}",
                    json.dumps(
                        {
                            "sensors": [{"id": f"temp-extra-{i}", "status": "inactive"}],
                            "sync": {"version": i},
                        }
                    ),
                )
            )
        with conn.prepare("INSERT INTO sensor_events(device_id, metadata) VALUES (?, ?)") as stmt:
            for device_id, metadata in rows:
                stmt.bind_text(1, device_id)
                stmt.bind_text(2, metadata)
                assert stmt.step() == SQLITE_DONE
                stmt.reset()

        found = conn.query(
            """
            SELECT device_id
            FROM sensor_events
            WHERE json_extract(metadata, '$.sensors[0].status') = 'active'
            """
        )
        assert found == [("dev-a",)]

        plan = conn.query(
            """
            EXPLAIN QUERY PLAN
            SELECT id FROM sensor_events
            WHERE json_extract(metadata, '$.sensors[0].status') = 'active'
            """
        )
        assert any("idx_sensor0_status" in str(step) for step in plan)

        conn.exec(
            """
            UPDATE sensor_events
            SET metadata = json_set(
                metadata,
                '$.sensors[0].status', 'inactive',
                '$.sync.version', 8
            )
            WHERE device_id = 'dev-a'
            """
        )
        after = conn.query_one(
            """
            SELECT
                json_extract(metadata, '$.sensors[0].status'),
                json_extract(metadata, '$.sensors[0].threshold.high'),
                json_extract(metadata, '$.sync.version')
            FROM sensor_events WHERE device_id = 'dev-a'
            """
        )
        assert after == ("inactive", 81.5, 8)
