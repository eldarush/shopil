from contextlib import closing
import sqlite3

import pytest

from data_access.streaming import WINDOW_TABLES, read_stream_snapshot


def test_reader_does_not_create_a_missing_database(tmp_path):
    database = tmp_path / "missing.db"
    with pytest.raises(sqlite3.OperationalError):
        read_stream_snapshot(database)
    assert not database.exists()


def test_commit_between_queries_does_not_mix_progress_and_rows(tmp_path, monkeypatch):
    database = tmp_path / "stream.db"
    connect = sqlite3.connect
    with closing(connect(database)) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.executescript("""
            CREATE TABLE stream_meta (
                singleton, checkpoint, watermark_ms, config_json, finalized
            );
            INSERT INTO stream_meta VALUES (1, 2, 120, '{}', 0);
            CREATE TABLE counters (name, value);
            INSERT INTO counters VALUES ('accepted_reviews', 2);
        """)
        for table, _ in WINDOW_TABLES:
            writer.execute(f"CREATE TABLE {table} (window_start_ms, product_id)")
        writer.commit()

        committed = False

        def connect_reader(*args, **kwargs):
            reader = connect(*args, **kwargs)

            def on_query(sql):
                nonlocal committed
                if "FROM counters" in sql and not committed:
                    with writer:
                        writer.execute("UPDATE stream_meta SET checkpoint = 3, watermark_ms = 180")
                        writer.execute("UPDATE counters SET value = 3")
                        writer.execute("INSERT INTO review_alerts VALUES (0, 5)")
                    committed = True

            reader.set_trace_callback(on_query)
            return reader

        monkeypatch.setattr(sqlite3, "connect", connect_reader)
        before = read_stream_snapshot(database)
        after = read_stream_snapshot(database)

    assert committed
    assert (before["checkpoint"], before["watermark_ms"]) == (2, 120)
    assert before["counters"]["accepted_reviews"] == 2
    assert before["rows"]["review_alerts"] == []
    assert (after["checkpoint"], after["watermark_ms"]) == (3, 180)
    assert after["counters"]["accepted_reviews"] == 3
    assert after["rows"]["review_alerts"] == [{"window_start_ms": 0, "product_id": 5}]
