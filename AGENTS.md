# ShopIL

Use the installed `shopil-project`, `jira-workflow`, and `ponytail` skills. Assignment 1 is the authority; course starter code is the base, never the instructor solution.

Before each Jira write, resolve the authenticated account and freshly check the issue's assignee accountId. Only edit that user's issues. Never create or claim tickets without explicit instructions.

Work on one ticket per branch and PR, targeting `master`. Use `feature/SHOPIL-N-description` (or `fix`, `test`, `docs`, `ci`) and Conventional Commits with `[SHOPIL-N]`. Run the relevant tests and `tools/workflow_guard.py`; include actual results and limitations in the PR. Merge only after current-head CI passes. Pull master after each merge.

Keep ETL/streaming runtimes isolated from chat when dependencies differ. Preserve historical paid revenue in ILS separately from streaming event amounts in cents. One question uses one frozen evidence snapshot for all Actor/Critic attempts; only approved answers enter conversation history.

Keep secrets, databases, environments and generated logs outside Git. Required submission results belong in a generated artifact bundle. Preserve source attribution and deterministic test inputs.
