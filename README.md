# ShopIL AI Business Assistant

Course assignment combining historical ShopIL data, continuous order/review processing, and a Hebrew business assistant with Actor/Critic evaluation.

## Status

Repository initialization only (SHOPIL-7). The component directories are placeholders; application code, dependencies, runtime setup, CI, and assignment experiments remain to be implemented in their Jira tasks. No application run command is available yet.

## Project links

- [Jira board](https://privaterelay-team-yb37vjhl.atlassian.net/jira/software/projects/SHOPIL/boards/3)
- [Repository initialization task: SHOPIL-7](https://privaterelay-team-yb37vjhl.atlassian.net/browse/SHOPIL-7)
- [Assignment instructions (Moodle login required)](https://moodle.colman.ac.il/mod/assign/view.php?id=45837)
- [Course source materials](docs/course-sources.md)

## Layout

| Directory | Responsibility |
| --- | --- |
| `etl/` | Historical data loading, review cleaning, dbt models and metrics |
| `streaming/` | Continuous ingestion, live/microbatch processing, windows and SQLite state |
| `data_access/` | Defined queries and consistent evidence snapshots |
| `chat/` | Hebrew Gradio UI, Actor/Critic prompts and bounded evaluation loop |
| `integration/` | Cross-component experiments and reproducible verification |
| `docs/` | Source attribution and project documentation |

## Get started

```sh
git clone https://github.com/eldarush/shopil.git
cd shopil
```

This is a private repository; team members need repository access and GitHub authentication. Follow [CONTRIBUTING.md](CONTRIBUTING.md) for feature branches and pull requests. Runtime and dependency setup is tracked in [SHOPIL-8](https://privaterelay-team-yb37vjhl.atlassian.net/browse/SHOPIL-8).

## Data and secrets

Keep runtime databases, generated data, logs, virtual environments, and credentials out of Git. Root and nested `.env` files are ignored, while `.env.example` files may contain variable names and empty/example values only. Ignore patterns are a safeguard, not a secret scanner; review every staged diff and never force-add sensitive files.

When implementations are added, finish Batch preparation before starting the chat, keep streaming active during responses, and freeze one evidence snapshot across every Actor/Critic attempt for a question. Historical paid revenue and streaming order-event amounts must remain separate unless compatibility and non-overlap are proven.
