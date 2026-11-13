# Collaboration workflow

Use Jira for task ownership, acceptance criteria, and progress. The workflow below is a team convention; branch protection and required checks are not configured yet.

1. Move the relevant Jira task to In Progress when work starts.
2. Update `main` with `git switch main` followed by `git pull --ff-only`.
3. Create a branch named `feature/SHOPIL-<number>-<short-description>` (or `fix/SHOPIL-<number>-<short-description>`).
4. Keep the change focused on that task. Include the Jira key in commit messages and the pull request title.
5. Run meaningful checks for the changed component and capture the command, result, and any limitations.
6. Inspect `git diff --cached` before committing. Do not commit secrets, local databases, generated data, or unrelated course documents.
7. Push the feature branch and open a pull request targeting `main`. Link the Jira task, explain behavior, and include verification evidence using the PR template.
8. Request a teammate review. After review, merge and verify the task's acceptance criteria before moving it to Done.

Record reused lecturer code and any modifications in `docs/course-sources.md`. Keep instructor solutions separate from independent assignment work. Agree on shared identifiers and evidence contracts before changing component boundaries.

Repository initialization is the bootstrap commit on `main`; subsequent implementation work follows the feature-branch workflow above. No teammate accounts or assignments are assumed by this scaffold.
