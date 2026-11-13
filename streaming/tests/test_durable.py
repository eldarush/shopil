"""SHOPIL-23: per-record transaction boundary and idempotent replay on durable state."""
from __future__ import annotations

import pytest

from shopil_streaming.durable import durable_state, fingerprint
from streaming_lab import InjectedCrash, StreamEngine, teaching_fixture


def _prefix(tmp_path, n):
    path = tmp_path / f"prefix{n}.db"
    with StreamEngine(path) as engine:
        for item in teaching_fixture()[:n]:
            engine.ingest(item.event, offset=item.offset)
    return durable_state(path)


@pytest.mark.parametrize("stage,offset", [("before_commit", 8), ("after_commit", 8), ("before_commit", 11),
                                          ("after_commit", 15)])
def test_crash_leaves_whole_record_or_nothing(tmp_path, stage, offset):
    path = tmp_path / "crash.db"
    with StreamEngine(path) as engine:
        with pytest.raises(InjectedCrash):
            for item in teaching_fixture():
                engine.ingest(item.event, offset=item.offset, crash_at=stage if item.offset == offset else None)
    expected = _prefix(tmp_path, offset if stage == "before_commit" else offset + 1)
    assert durable_state(path) == expected


def test_replay_after_reopen_keeps_fingerprint(tmp_path):
    path = tmp_path / "s.db"
    with StreamEngine(path) as engine:
        for item in teaching_fixture():
            engine.ingest(item.event, offset=item.offset)
    before = fingerprint(path)
    with StreamEngine(path) as engine:
        assert {engine.ingest(i.event, offset=i.offset)["status"] for i in teaching_fixture()} == {"replayed"}
    assert fingerprint(path) == before
