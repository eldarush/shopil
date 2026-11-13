"""SHOPIL-37: reproducible fixtures and independent databases.

    uv run python experiments/shopil_37_fixtures.py --write   # (re)generate fixture files + manifest
    uv run python experiments/shopil_37_fixtures.py           # verify and write evidence

Verification:
  * regenerating each fixture from the course generator reproduces the committed
    file byte for byte (same sha256);
  * loading a file returns exactly the generator's deliveries, in source order;
  * the same fixture processed on two independent fresh DBs gives identical
    canonical_results, matching the manifest's expected values;
  * control: SORTING the input by event_time changes the results (late events
    disappear), which is why fixtures are never sorted.
"""
from __future__ import annotations

import sys

from shopil_streaming.fixtures import (FIXTURE_DIR, GENERATORS, TEACHING_CONFIG, fixture_path, fresh_engine,
                                       load_fixture, manifest, sha256, to_jsonl)
from shopil_streaming.runs import md_table, shekels, write_json, write_text
from streaming_lab import Delivery, Event, batch_truth
from streaming_lab.reporting import canonical_results

TICKET = "SHOPIL-37"


def run(deliveries, tag: str, finalize: bool = True) -> dict:
    with fresh_engine(tag, **TEACHING_CONFIG) as engine:
        for item in deliveries:
            engine.ingest(item.event, offset=item.offset)
        return canonical_results(engine.finalize_demo() if finalize else engine.snapshot())


def expected_values(deliveries) -> dict:
    final = run(deliveries, "shopil-37-expected")
    t = final["totals"]
    return {"records": len(deliveries), "accepted_orders": t["orders"], "accepted_revenue_cents": t["revenue_cents"],
            "late_orders": t["late_orders"], "late_order_cents": t["late_order_cents"],
            "reviews": t["reviews"], "negative_reviews": t["negative_reviews"],
            "duplicates": final["stats"]["duplicates"], "invalid": final["stats"]["invalid"],
            "alerts": final["stats"]["alerts_emitted"], "revenue_windows": len(final["revenue_windows"]),
            "batch_truth": batch_truth(deliveries)}


def write_fixtures() -> None:
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    entries = {}
    for name, (generator, make) in GENERATORS.items():
        deliveries = make()
        text = to_jsonl(deliveries)
        fixture_path(name).write_bytes(text.encode("utf-8"))      # LF line endings, stable hash on every OS
        entries[name] = {"file": f"{name}.jsonl", "generator": generator, "sha256": sha256(text),
                         "expected_with_teaching_config": expected_values(deliveries)}
    import json
    (FIXTURE_DIR / "manifest.json").write_bytes((json.dumps(
        {"config": TEACHING_CONFIG, "source": "paulavrahami/streaming-course-demo@c27b4fc",
         "order": "source/arrival order; offsets 0..n-1; never sorted by event_time", "fixtures": entries},
        ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8"))
    print(f"wrote {len(entries)} fixtures to {FIXTURE_DIR}")


def sorted_by_event_time(deliveries) -> list[Delivery]:
    valid = [d for d in deliveries if isinstance(d.event, Event)]
    invalid = [d for d in deliveries if not isinstance(d.event, Event)]
    ordered = sorted(valid, key=lambda d: d.event.event_time) + invalid
    return [Delivery(i, d.event) for i, d in enumerate(ordered)]


def main() -> int:
    if "--write" in sys.argv:
        write_fixtures()
    info = manifest()
    rows, checks = [], {}
    for name, (generator, make) in GENERATORS.items():
        entry = info["fixtures"][name]
        regenerated = to_jsonl(make())
        committed = fixture_path(name).read_bytes().decode("utf-8")
        loaded = load_fixture(name)
        first = run(loaded, f"shopil-37-{name}-a")
        second = run(load_fixture(name), f"shopil-37-{name}-b")
        exp = entry["expected_with_teaching_config"]
        t = first["totals"]
        checks[f"{name}: regenerated == committed file (sha256)"] = sha256(regenerated) == sha256(committed) == entry["sha256"]
        checks[f"{name}: loaded deliveries == generator output, same order"] = loaded == make()
        checks[f"{name}: two independent fresh DBs -> identical canonical_results"] = first == second
        checks[f"{name}: results match manifest expectations"] = (
            t["revenue_cents"], t["late_order_cents"], first["stats"]["duplicates"], first["stats"]["alerts_emitted"]
        ) == (exp["accepted_revenue_cents"], exp["late_order_cents"], exp["duplicates"], exp["alerts"])
        rows.append({"fixture": name, "records": len(loaded), "sha256": entry["sha256"][:16],
                     "accepted_ils": shekels(t["revenue_cents"]), "late_ils": shekels(t["late_order_cents"]),
                     "duplicates": first["stats"]["duplicates"], "invalid": first["stats"]["invalid"],
                     "late": first["stats"]["late"], "alerts": first["stats"]["alerts_emitted"]})
    control = []
    for name in ("teaching", "orders300"):
        source = run(load_fixture(name), f"shopil-37-{name}-source")
        resorted = run(sorted_by_event_time(load_fixture(name)), f"shopil-37-{name}-sorted")
        control.append({"fixture": name, "source_order_late": source["stats"]["late"],
                        "sorted_late": resorted["stats"]["late"],
                        "source_order_accepted_ils": shekels(source["totals"]["revenue_cents"]),
                        "sorted_accepted_ils": shekels(resorted["totals"]["revenue_cents"])})
    checks["control: sorting by event_time changes late counts (so we never sort)"] = all(
        c["source_order_late"] != c["sorted_late"] for c in control)
    write_json(TICKET, "fixtures.json", {"fixtures": rows, "sorting_control": control, "checks": checks})
    passed = all(checks.values())
    report = f"""# SHOPIL-37 — reproducible fixtures and independent databases

Commands: `uv run python experiments/shopil_37_fixtures.py --write` regenerates the files; `uv run python experiments/shopil_37_fixtures.py` verifies them. Fixtures live in `fixtures/streaming/` (JSON Lines, one delivery per line, in source order) with `manifest.json` holding generator, sha256, config and expected results. `shopil_streaming.fixtures.load_fixture` refuses any file whose offsets are not `0..n-1` in file order; `fresh_engine` always opens a new DB under `runs/`.

## Fixtures (config: 60 s windows, out_of_order 0, grace 10, threshold 3)
{md_table(rows)}

## Control: why input must not be sorted by event time
{md_table(control)}

Sorting removes the disorder the experiments are about. Late events become on-time and the accepted revenue changes, so a sorted run would no longer test the assignment's lateness rules.

## Checks
{md_table([{'check': k, 'result': 'PASS' if v else 'FAIL'} for k, v in checks.items()])}

**Result: {'PASS' if passed else 'FAIL'}**
"""
    write_text(TICKET, "README.md", report)
    print(report)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
