"""Compare the complete event trace to an independent copy of the pinned course code."""
import argparse
from hashlib import sha256
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
REVISION = "c27b4fc1b343e5bfca1880ad3c5ae34e97f60b71"
COURSE_FILES = {
    "__init__.py": "2489c76f873ece4f50b2a4816be48b335344600affbf7a412a4ca9f0b0bfd33c",
    "engine.py": "ca6aec1d4d72b69ee905dbafd594b254ef79fe53599a50abf3aa6b88d8aa2647",
    "events.py": "c748c78c7adbe60b70b6f706bd9a9a5dfdef719f6bfdaea832765b15ef263dfd",
    "live.py": "2b3a06d6e5767fd197207a5c6d3dfceaa1cb1ebda4cd9455a1559f676c0cfcc9",
    "microbatch.py": "fe7741447bb9b6aa858ee08a5dd5c166fc3beb7603a03c013ace078bba4ab667",
    "reporting.py": "a50c54f69ca91c0c83170cd3bb23e587060d5b77b038887d71b6083d2699c0ef",
}


def collect(source_dir):
    sys.path.insert(0, str(source_dir.resolve()))
    from streaming_lab import StreamEngine
    import streaming_lab
    if not Path(streaming_lab.__file__).resolve().is_relative_to(source_dir.resolve()):
        raise RuntimeError("Loaded a different streaming package")
    rows = []
    with TemporaryDirectory() as temp:
        for fixture in ("teaching", "reviews", "orders300"):
            raw = (ROOT / f"fixtures/streaming/{fixture}.jsonl").read_text(encoding="utf-8")
            deliveries = [json.loads(line) for line in raw.splitlines()]
            for grace in (0, 10, 120):
                digest = sha256()
                with StreamEngine(Path(temp) / f"{fixture}-{grace}.db", window_seconds=60,
                                  grace_seconds=grace, out_of_order_seconds=0) as engine:
                    for delivery in deliveries:
                        decision = engine.ingest(delivery["event"], offset=delivery["offset"])
                        pair = [decision, engine.snapshot()]
                        digest.update(json.dumps(pair, sort_keys=True).encode())
                    digest.update(json.dumps(engine.finalize_demo(), sort_keys=True).encode())
                rows.append(dict(fixture=fixture, grace=grace, compared_offsets=len(deliveries),
                                 trace_sha256=digest.hexdigest()))
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--course-src", default=ROOT / "course_reference", type=Path,
                        help="Source directory for the pinned course revision")
    parser.add_argument("--collect", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.collect:
        print(json.dumps(collect(args.course_src)))
        return

    for name, expected in COURSE_FILES.items():
        body = (args.course_src / "streaming_lab" / name).read_bytes().replace(b"\r\n", b"\n")
        if sha256(body).hexdigest() != expected:
            raise SystemExit(f"Expected the unmodified course {name} at revision {REVISION}")

    def run(src):
        data = subprocess.check_output([sys.executable, "-I", __file__, "--collect", "--course-src", str(src)])
        return json.loads(data)

    upstream = run(args.course_src)
    shopil = run(ROOT / "src")
    passed = upstream == shopil
    print(json.dumps(dict(course_revision=REVISION, course_file_sha256=COURSE_FILES,
                          comparison="every decision and complete snapshot after each "
                          "offset in input order, followed by finalize_demo snapshot", course=upstream,
                          shopil=shopil, passed=passed), indent=2))
    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
