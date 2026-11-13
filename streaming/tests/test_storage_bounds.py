"""Storage-boundary rejection must be durable and leave business state unchanged."""
import pytest

from streaming_lab import Event, StreamEngine
from streaming_lab.events import InvalidEvent, SQLITE_MAX_INTEGER, T0


def order(key="order", **overrides):
    return {"event_id": key, "kind": "order", "event_time": T0.isoformat(),
            "product_id": 1, "amount_cents": 1, **overrides}


@pytest.mark.parametrize("field", ["product_id", "amount_cents", "order_id"])
def test_oversized_fields_are_dead_lettered_and_next_record_continues(tmp_path, field):
    path = tmp_path / "bounds.db"
    bad = order(**{field: SQLITE_MAX_INTEGER + 1})
    with StreamEngine(path) as engine:
        assert engine.ingest(bad, offset=0)["status"] == "invalid"
        snap = engine.snapshot()
        assert snap["checkpoint"] == 0 and snap["watermark"] is None
        assert snap["dead_letters"][0]["payload"] == bad
        assert "64-bit" in snap["dead_letters"][0]["reason"]
        assert engine.ingest(order("next"), offset=1)["status"] == "accepted"
    with StreamEngine(path) as engine:
        before = engine.snapshot()
        assert engine.ingest(bad, offset=0)["status"] == "replayed"
        assert engine.snapshot() == before


def test_direct_event_objects_have_the_same_integer_bounds():
    with pytest.raises(InvalidEvent, match="64-bit"):
        Event("review", "review", T0, SQLITE_MAX_INTEGER + 1, rating=1)


@pytest.mark.parametrize("field", ["event_id", "text"])
def test_unpaired_surrogates_are_preserved_in_dead_letters(tmp_path, field):
    bad = {"event_id": "bad", "kind": "review", "event_time": T0.isoformat(),
           "product_id": 1, "rating": 1, "text": "valid", field: "\ud800"}
    path = tmp_path / "unicode.db"
    with StreamEngine(path) as engine:
        assert engine.ingest(bad, offset=0)["status"] == "invalid"
        assert engine.ingest(order("next"), offset=1)["status"] == "accepted"
    with StreamEngine(path) as engine:
        snap = engine.snapshot()
        assert snap["checkpoint"] == 1
        assert snap["dead_letters"][0]["payload"] == bad
        assert "Unicode" in snap["dead_letters"][0]["reason"]


def test_invalid_payload_with_surrogate_key_is_still_auditable(tmp_path):
    with StreamEngine(tmp_path / "unicode-key.db") as engine:
        bad = {"\ud800": "invalid schema"}
        assert engine.ingest(bad, offset=0)["status"] == "invalid"
        assert engine.snapshot()["dead_letters"][0]["payload"] == bad


def test_window_overflow_retains_exact_money_and_rejected_payload(tmp_path):
    with StreamEngine(tmp_path / "sum.db") as engine:
        first = order(amount_cents=SQLITE_MAX_INTEGER)
        assert engine.ingest(first, offset=0)["status"] == "accepted"
        before = engine.snapshot()
        overflow = order("overflow")
        assert engine.ingest(overflow, offset=1)["status"] == "invalid"
        assert engine.ingest(first, offset=2)["status"] == "duplicate"
        snap = engine.snapshot()
        assert snap["open_windows"] == before["open_windows"]
        assert snap["watermark"] == before["watermark"]
        assert snap["stats"]["accepted"] == 1
        assert snap["dead_letters"][0]["payload"] == overflow
        assert engine.ingest(order("next-window", event_time="2026-11-01T10:02:00Z"), offset=3)["status"] == "accepted"
        assert engine.snapshot()["revenue_windows"][0]["revenue_cents"] == SQLITE_MAX_INTEGER
        assert engine.ingest(order("late", event_time=T0.isoformat()), offset=4)["status"] == "late"


@pytest.mark.parametrize("stamp,config", [
    ("9999-12-31T23:59:59.999999Z", {}),
    ("0001-01-01T00:00:00Z", {"out_of_order_seconds": 1}),
    ("2026-11-01T10:00:00Z", {"window_seconds": 315537897600}),
])
def test_unrenderable_dates_never_commit_unreadable_state(tmp_path, stamp, config):
    path = tmp_path / "dates.db"
    bad = Event.from_dict(order(event_time=stamp))
    with StreamEngine(path, **config) as engine:
        assert engine.ingest(bad, offset=0)["status"] == "invalid"
        snap = engine.snapshot()
        assert snap["checkpoint"] == 0 and snap["watermark"] is None
        assert snap["dead_letters"][0]["payload"] == bad.to_dict()
        assert "years 1..9999" in snap["dead_letters"][0]["reason"]
    with StreamEngine(path, **config) as engine:
        assert engine.snapshot() == snap


@pytest.mark.parametrize("key", ["window_seconds", "grace_seconds", "out_of_order_seconds"])
def test_unrepresentable_time_policies_are_rejected_before_database_creation(tmp_path, key):
    path = tmp_path / "absent.db"
    with pytest.raises(ValueError, match=key):
        StreamEngine(path, **{key: 315537897601})
    assert not path.exists()
