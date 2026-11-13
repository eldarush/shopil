from concurrent.futures import ThreadPoolExecutor
import json
from uuid import uuid4

import pytest

from chat.audit import JsonlAudit


def test_snapshot_and_rejected_draft_are_preserved_separately_from_history(tmp_path):
    request_id = uuid4().hex
    audit = JsonlAudit(tmp_path / "audit")
    events = [
        {"event": "snapshot", "request_id": request_id, "snapshot_id": "s1",
         "evidence_json": '{"paid_revenue_ils":"0.00"}'},
        {"event": "attempt", "request_id": request_id, "snapshot_id": "s1",
         "attempt": 1, "draft": "ההכנסה היא 100 ש״ח", "is_acceptable": False,
         "feedback": "הנתון הנכון הוא אפס"},
        {"event": "terminal", "request_id": request_id, "snapshot_id": "s1",
         "status": "rejected", "attempts": 3},
    ]
    for event in events:
        audit(event)
    output = tmp_path / "audit" / f"{request_id}.jsonl"
    assert [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()] == events


def test_concurrent_writes_keep_complete_json_records(tmp_path):
    audit = JsonlAudit(tmp_path)
    ids = [uuid4().hex for _ in range(3)]
    events = [{"event": "attempt", "request_id": ids[i % 3],
               "attempt": i, "draft": "תשובה " * 1000} for i in range(60)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(audit, events))
    actual = [json.loads(line) for path in tmp_path.glob("*.jsonl")
              for line in path.read_text(encoding="utf-8").splitlines()]
    assert sorted(actual, key=lambda row: row["attempt"]) == events


@pytest.mark.parametrize("request_id", ["../escape", "", None, "not-a-uuid"])
def test_invalid_request_id_cannot_create_a_file(tmp_path, request_id):
    with pytest.raises(ValueError):
        JsonlAudit(tmp_path)({"event": "snapshot", "request_id": request_id})
    assert list(tmp_path.iterdir()) == []


def test_storage_failure_is_not_suppressed(tmp_path, monkeypatch):
    def fail_sync(_):
        raise OSError("simulated disk failure")
    monkeypatch.setattr("chat.audit.os.fsync", fail_sync)
    with pytest.raises(OSError, match="simulated disk failure"):
        JsonlAudit(tmp_path)({"event": "terminal", "request_id": uuid4().hex, "status": "error"})


def test_nonfinite_values_are_rejected_before_writing(tmp_path):
    with pytest.raises(ValueError):
        JsonlAudit(tmp_path)({"event": "snapshot", "request_id": uuid4().hex, "value": float("nan")})
    assert list(tmp_path.iterdir()) == []
