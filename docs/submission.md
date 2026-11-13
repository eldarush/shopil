# Submission bundle

Assignment 1 requires source code, experiment inputs, setup instructions, attribution, test results, model audit logs, an explanation of at most three pages (excluding diagram/results), and screenshots of active ingestion and a sourced, timestamped answer. Keep generated evidence outside Git. Do not include credentials, environments or databases in the submission ZIP.

Collect reviewed outputs in an ignored directory such as `runs/submission/`. Add `selection.json` there, listing only the files the grader needs:

```json
{
  "etl": ["etl/dbt-results.json", "etl/load-clean-review.md"],
  "streaming": ["streaming/experiments.json", "streaming/reconciliation.md"],
  "chat": ["chat/actor-critic.jsonl", "chat/manual-review.md"],
  "screenshots": ["screenshots/active-stream.png", "screenshots/sourced-answer.png"],
  "report": ["report.pdf"]
}
```

These are example filenames, not evidence supplied by this document. Preserve input/settings/expected/actual results for each experiment; label mocked models clearly. Include the actual completed batch/checker/dbt run, both stream modes and flush reasons, time-policy/dedup checks, the real correction cycle, missing/follow-up cases and the concurrent-update test. Review the screenshots and the two approved answers against their actual SQL results. Keep source attribution in the source README and report.

From a clean, reviewed source checkout:

```text
uv run --locked python -m tools.package_submission --evidence runs/submission --output artifacts/shopil-submission.zip
```

The ZIP contains `source/` from the exact Git commit, explicitly selected `evidence/`, and a manifest with the commit and SHA-256 of every included file. Repeated packaging of identical inputs is byte reproducible. Existing ZIPs are not overwritten. The command checks required groups, nonempty files, path containment and prohibited filenames; it does not determine whether a screenshot or experiment proves correctness. Unselected files are excluded. Inspect the resulting archive and rehearse the complete README workflow before submission. Nothing is uploaded to Moodle by this command.
