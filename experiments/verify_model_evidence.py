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
    turns = {turn["snapshot_id"]: turn for turn in result["turns"]}
    snapshots = {}
    for path in (directory / "audit").glob("*.jsonl"):
        events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        stamp, terminal = events[0], events[-1]
        sid = stamp["snapshot_id"]
        assert terminal["status"] == "approved" and events[-2]["is_acceptable"]
        assert events[-2]["draft"] == turns[sid]["answer"]
        assert all(e["snapshot_id"] == sid for e in events)
        snapshots[sid] = stamp["evidence_json"]
        draft = turns[sid]["answer"]
        assert not candidate_feedback(draft, json.loads(snapshots[sid])), draft
        actor = json.loads(events[-2]["actor_output_json"])
        assert draft == actor["answer"].strip() + "\n\n" + actor["citation"].strip()
        assert events[-2]["critic_is_acceptable"] is True
    assert snapshots.keys() == turns.keys()
    seen = {key: set() for key in snapshots}
    for path in sorted(directory.glob("call-*.json")):
        call = read(path)
        frozen = call["messages"][2]["content"]
        evidence = json.loads(frozen)
        sid = evidence["snapshot_id"]
        seen[sid].add(call["role"])
        assert turns[sid]["question"] == evidence["question"]
        assert snapshots[sid] == frozen and sha256(frozen.encode()).hexdigest() == call["evidence_sha256"]
        if "stream_update" in call:
            update = call["stream_update"]
            assert datetime.fromisoformat(call["started"]) < datetime.fromisoformat(update["at"]) < datetime.fromisoformat(call["completed"])
            assert update["result"]["status"] == "accepted" and call["actor_pending_after_update"]
    assert all(roles == {"actor", "critic"} for roles in seen.values())
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    for case in ("correction", "concurrent", "conversation"):
        result = verify_case(args.evidence / case)
        print(f"{case}: verified {len(result['turns'])} approved turns and every frozen request.")
    base = """SELECT count(*) AS paid_orders, sum(amount) AS paid_revenue_ils
FROM marts.fct_orders WHERE status = 'paid'
AND order_date >= DATE '2026-01-01' AND order_date < DATE '2027-01-01'"""
    conversation = read(args.evidence / "conversation" / "results.json")
    with duckdb.connect(str(args.warehouse), read_only=True) as db:
        first, last = db.execute("SELECT min(order_date), max(order_date) FROM marts.fct_orders").fetchone()
        exclusive_end = last + timedelta(days=1)
        for turn, product in zip(conversation["turns"], (None, 5)):
            sql = base if product is None else base + " AND product_id = ?"
            count, amount = db.execute(sql, [] if product is None else [product]).fetchone()
            prose = turn["answer"].split("\n\n", 1)[0]
            numbers = [Decimal(x.replace(",", "")) for x in re.findall(r"\d+(?:,\d{3})*(?:\.\d+)?", prose)]
            assert Decimal(amount) in numbers and "marts.fct_orders" in turn["answer"]
            assert f"מ־{first} (כולל) עד {exclusive_end} (לא כולל)" in turn["answer"]
            print(f"{turn['snapshot_id']}: product={product}, count={count}, ILS={amount}, "
                  f"verified source coverage=[{first}, {exclusive_end}), text={prose}")


if __name__ == "__main__":
    main()
