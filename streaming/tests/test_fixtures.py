"""SHOPIL-37: committed fixtures are reproducible, ordered and never sorted."""
from __future__ import annotations

import pytest

from shopil_streaming.fixtures import GENERATORS, fixture_path, load_fixture, manifest, sha256, to_jsonl


@pytest.mark.parametrize("name", list(GENERATORS))
def test_committed_fixture_matches_generator(name):
    _, make = GENERATORS[name]
    text = fixture_path(name).read_bytes().decode("utf-8")
    assert sha256(text) == sha256(to_jsonl(make())) == manifest()["fixtures"][name]["sha256"]
    assert load_fixture(name) == make()


def test_loader_rejects_reordered_file(tmp_path):
    lines = fixture_path("teaching").read_text(encoding="utf-8").splitlines(keepends=True)
    lines[1], lines[2] = lines[2], lines[1]
    (tmp_path / "swapped.jsonl").write_text("".join(lines), encoding="utf-8")
    with pytest.raises(ValueError, match="offsets must be 0..n-1"):
        load_fixture("swapped", tmp_path)
