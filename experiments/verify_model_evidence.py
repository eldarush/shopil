"""Independently check frozen bytes, approved sentences and SQL for an Ollama run."""

import argparse
from datetime import datetime, timedelta
from hashlib import sha256
import json
from pathlib import Path
import re
from decimal import Decimal

import duckdb

from chat.grounded import candidate_feedback


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def verify_case(directory):
    result = read(directory / "results.json")
    assert result["passed"], directory
    correction = result["case"] == "correction"
    turns = {turn["snapshot_id"]: turn for turn in result["turns"]}
    snapshots, attempts = {}, {}
    for path in (directory / "audit").glob("*.jsonl"):
        events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        stamp, terminal = events[0], events[-1]
        sid = stamp["snapshot_id"]
        assert all(event["snapshot_id"] == sid for event in events)
        snapshots[sid] = stamp["evidence_json"]
        attempts[sid] = [event for event in events if event["event"] == "attempt"]
        assert terminal["status"] == turns[sid]["status"]
        if terminal["status"] == "approved":
            last = attempts[sid][-1]
            assert last["is_acceptable"] and last["draft"] == turns[sid]["answer"]
            actor = json.loads(last["actor_output_json"])
            assert last["draft"] == actor["answer"].strip() + "\n\n" + actor["citation"].strip()
            assert not candidate_feedback(last["draft"], json.loads(snapshots[sid])), last["draft"]
    assert snapshots.keys() == turns.keys()
    seen = {key: {"actor": [], "critic": []} for key in snapshots}
    for path in sorted(directory.glob("call-*.json")):
        call = read(path)
        frozen = call["messages"][2]["content"]
        evidence = json.loads(frozen)
        sid = evidence["snapshot_id"]
        seen[sid][call["role"]].append(call)
        assert turns[sid]["question"] == evidence["question"]
        assert snapshots[sid] == frozen and sha256(frozen.encode()).hexdigest() == call["evidence_sha256"]
        if "stream_update" in call:
            update = call["stream_update"]
            assert datetime.fromisoformat(call["started"]) < datetime.fromisoformat(update["at"]) < datetime.fromisoformat(call["completed"])
            assert update["result"]["status"] == "accepted" and call["actor_pending_after_update"]
    for sid, records in attempts.items():
        calls = seen[sid]
        assert records and len(records) == len(calls["actor"]) == len(calls["critic"]), sid
        for attempt, actor, critic in zip(records, calls["actor"], calls["critic"]):
            assert actor["response"] == attempt["actor_output_json"]
            assert json.loads(critic["response"]) == {
                "is_acceptable": attempt.get("critic_is_acceptable", attempt["is_acceptable"]),
                "feedback": attempt["critic_feedback"],
            }
    if correction:
        sid = result["snapshot_id"]
        selected = attempts[sid]
        assert result["injected_first_draft"] is None
        assert selected == result["attempts"] and 2 <= len(selected) <= 3
        assert selected[-1]["is_acceptable"] is True
        frozen = json.loads(snapshots[sid])
        assert len(selected) == len(seen[sid]["actor"]) == len(seen[sid]["critic"])
        for index, (attempt, actor, critic) in enumerate(zip(selected, seen[sid]["actor"], seen[sid]["critic"])):
            assert all("enum" not in actor["schema"]["properties"][field] for field in ("answer", "citation"))
            assert json.loads(critic["response"]) == {
                "is_acceptable": attempt["is_acceptable"], "feedback": attempt["feedback"]
            }
            assert attempt["critic_feedback"] == attempt["feedback"]
            messages = actor["messages"]
            assert json.loads(messages[3]["content"]) == {"question": frozen["question"]}
            if index:
                assert messages[4] == {"role": "assistant", "content": selected[index - 1]["draft"]}
                assert json.loads(messages[5]["content"]) == {"feedback": selected[index - 1]["feedback"]}
            else:
                assert len(messages) == 4
            assert attempt["is_acceptable"] is (index == len(selected) - 1)
        assert candidate_feedback(selected[0]["draft"], frozen), "First draft needs an independently checkable error"
        assert not candidate_feedback(selected[-1]["draft"], frozen), selected[-1]["draft"]
        prose = json.loads(selected[-1]["actor_output_json"])["answer"]
        assert "רווח" in prose and re.search(r"עלות|עלויות|הוצאות", prose) and not re.search(r"\d", prose)
        return result, frozen
    return result, None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    for case in ("correction", "concurrent", "conversation"):
        result, frozen = verify_case(args.evidence / case)
        if case == "correction":
            correction = frozen
        count = sum(turn["status"] == "approved" for turn in result["turns"])
        print(f"{case}: verified {count} approved turns and every frozen request.")
    base = """SELECT count(*) AS paid_orders, sum(amount) AS paid_revenue_ils
FROM marts.fct_orders WHERE status = 'paid'
AND order_date >= DATE '2026-01-01' AND order_date < DATE '2027-01-01'"""
    conversation = read(args.evidence / "conversation" / "results.json")
    with duckdb.connect(str(args.warehouse), read_only=True) as db:
        scope = correction["query_scope"]
        assert scope["metric"] == "profit" and scope["availability"] == "missing"
        assert scope["consulted_table"] == "marts.fct_orders"
        columns = [row[0] for row in db.execute("DESCRIBE marts.fct_orders").fetchall()]
        assert columns == scope["available_columns"]
        assert not any(re.search(r"cost|profit|expense", name) for name in columns)
        print(f"{correction['snapshot_id']}: confirmed the orders schema has no cost or profit column.")
        first, last = db.execute("SELECT min(order_date), max(order_date) FROM marts.fct_orders").fetchone()
        exclusive_end = last + timedelta(days=1)
        for turn, product in zip(conversation["turns"], (None, 5)):
            sql = base if product is None else base + " AND product_id = ?"
            count, amount = db.execute(sql, [] if product is None else [product]).fetchone()
            prose = turn["answer"].split("\n\n", 1)[0]
            numbers = [Decimal(x.replace(",", "")) for x in re.findall(r"\d+(?:,\d{3})*(?:\.\d+)?", prose)]
            assert Decimal(amount) in numbers and "marts.fct_orders" in turn["answer"]
            assert str(first) in turn["answer"] and str(exclusive_end) in turn["answer"]
            assert "לא כולל" in turn["answer"]
            print(f"{turn['snapshot_id']}: product={product}, count={count}, ILS={amount}, "
                  f"verified source coverage=[{first}, {exclusive_end}), text={prose}")


if __name__ == "__main__":
    main()
