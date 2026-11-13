"""Experiment mechanics use scripted responses; these are not live-model evidence."""

import asyncio
import json
import subprocess

import pytest

from experiments.chat_evaluation import (
    BAD_DRAFT, mock_three_rejections, rejection_experiment, teaching_evidence,
)
from experiments.codex_teaching import _TeachingModel, verify_text_only_turn
from chat.model import ModelError


@pytest.fixture(scope="module")
def snapshot(tmp_path_factory):
    result = teaching_evidence(tmp_path_factory.mktemp("teaching-evaluation") / "stream.db")
    # SQL input stays real; scripted-model transcripts use stable test metadata.
    result.update(snapshot_id="teaching-test-snapshot", captured_at="2026-11-01T12:00:00+00:00")
    return result


def test_sql_fixture_has_exact_reference_and_preserves_existing_database(tmp_path, snapshot):
    source = snapshot["sources"][0]
    assert source["rows"][0] == {
        "accepted_cents": 12200, "accepted_ils": "122",
        "late_cents": 1300, "late_ils": "13",
    }
    assert source["checkpoint"] == 16
    assert len(source["accepted_event_ids_and_products"]) == 7
    assert set(source["late_event_ids"]) == {"order-late", "order-boundary"}
    database = tmp_path / "existing.db"
    database.write_bytes(b"keep")
    with pytest.raises(FileExistsError):
        teaching_evidence(database)
    assert database.read_bytes() == b"keep"
    assert snapshot["stream_config"] == {"window_seconds": 60, "grace_seconds": 10,
                                          "out_of_order_seconds": 0, "alert_threshold": 3}
    assert len(snapshot["fixture"]["sha256_utf8_lf"]) == 64


def test_changed_teaching_fixture_is_rejected_before_database_creation(tmp_path, monkeypatch):
    fixtures = tmp_path / "streaming/fixtures/streaming"
    fixtures.mkdir(parents=True)
    (fixtures / "manifest.json").write_text(json.dumps({"fixtures": {"teaching": {"sha256": "wrong"}}}))
    (fixtures / "teaching.jsonl").write_text("changed")
    monkeypatch.setattr("experiments.chat_evaluation.ROOT", tmp_path)
    with pytest.raises(ValueError, match="manifest"):
        teaching_evidence(tmp_path / "stream.db")
    assert not (tmp_path / "stream.db").exists()


@pytest.mark.parametrize("approve_bad_draft", [False, True])
def test_experiment_requires_rejection_then_actual_correction_call(tmp_path, snapshot, approve_bad_draft):
    class ScriptedModel:
        def __init__(self):
            self.calls = []

        async def complete(self, messages, *, schema=None):
            self.calls.append((messages, schema))
            if schema:
                return json.dumps({"is_acceptable": approve_bad_draft or len(self.calls) > 1,
                                   "feedback": "Correct the accepted amount to 122 ILS"})
            correction = json.loads(messages[-1]["content"])
            assert correction["previous_draft"] == BAD_DRAFT
            assert "122" in correction["feedback"]
            return "הסכום שהתקבל לצבירה הוא 122 ש״ח."

    model = ScriptedModel()
    result = asyncio.run(rejection_experiment(model, snapshot, tmp_path))
    assert result["cycle_passed"] is (not approve_bad_draft)
    assert len(model.calls) == (1 if approve_bad_draft else 3)
    persisted = [json.loads(line) for path in tmp_path.glob("*.jsonl")
                 for line in path.read_text(encoding="utf-8").splitlines()]
    assert persisted == result["events"]
    assert {event["snapshot_id"] for event in persisted} == {snapshot["snapshot_id"]}


def test_explicit_mock_stops_after_three_rejections(snapshot):
    result = asyncio.run(mock_three_rejections(snapshot))
    assert result["passed"]
    assert result["case"].startswith("MOCKED")
    assert result["events"][-1]["attempts"] == 3


def test_live_turn_checks_completion_and_rejects_tool_activity():
    completed = {"type": "turn.completed"}
    warning = {"type": "item.completed", "item": {"type": "error", "message": "Host warning"}}
    verify_text_only_turn([warning, completed])
    with pytest.raises(ModelError, match="did not complete"):
        verify_text_only_turn([warning])
    with pytest.raises(ModelError, match="invoked a tool"):
        verify_text_only_turn([{"type": "item.started", "item": {"type": "command_execution"}}, completed])


def test_unchanged_wrong_draft_is_not_a_successful_correction(tmp_path, snapshot):
    class RepeatingModel:
        evaluations = 0

        async def complete(self, messages, *, schema=None):
            if not schema:
                return BAD_DRAFT
            self.evaluations += 1
            return json.dumps({"is_acceptable": self.evaluations > 1, "feedback": "Scripted decision"})

    result = asyncio.run(rejection_experiment(RepeatingModel(), snapshot, tmp_path))
    assert result["status"] == "approved"
    assert not result["cycle_passed"]
    assert result["manual_semantic_review_required"]


def test_model_deadline_retains_partial_output_and_audits_terminal_error(tmp_path, snapshot, monkeypatch):
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired("codex", 180, output=b"partial output", stderr=b"partial error")

    monkeypatch.setattr("experiments.codex_teaching.shutil.which", lambda name: "codex")
    monkeypatch.setattr("experiments.codex_teaching.subprocess.run", timeout)
    result = asyncio.run(rejection_experiment(_TeachingModel(tmp_path), snapshot, tmp_path / "audit"))
    assert result["status"] == "error" and not result["cycle_passed"]
    assert result["events"][-1]["error_type"] == "ModelError"
    assert result["events"][-1]["error_stage"] == "critic"
    assert (tmp_path / "call-1.events.jsonl").read_bytes() == b"partial output"
    assert (tmp_path / "call-1.stderr.txt").read_bytes() == b"partial error"
