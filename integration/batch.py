"""Run the complete course Batch preparation before the application opens DuckDB."""

import argparse
from io import BytesIO
import os
from pathlib import Path
import shutil
import subprocess
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[1]
COURSE_REVISION = "6788526695bbb20ac40f46a947413fff3545b86a"
MODELS = ("staging/stg_reviews.sql", "staging/_stg_reviews.yml",
          "marts/mart_product_reviews.sql", "marts/_mart_product_reviews.yml")


def prepare_batch(lab_dir: Path, etl_python: Path, run_dir: Path) -> Path:
    """Keep model overlays/results in a new run directory; reuse the warehouse/state."""
    lab_dir, run_dir = lab_dir.resolve(), run_dir.resolve()
    # POSIX venv Python is a symlink; resolving it would select the base environment.
    etl_python = Path(os.path.abspath(etl_python))
    dbt = etl_python.parent / ("dbt.exe" if os.name == "nt" else "dbt")
    for path in (etl_python, dbt, lab_dir / "pipelines/02_dlt_pipeline.py",
                 lab_dir / "exercises/check_exercise.py",
                 *(ROOT / "etl/dbt/models" / name for name in MODELS)):
        if not path.is_file():
            raise FileNotFoundError(f"Required Batch component is missing: {path}")
    revision = subprocess.check_output(["git", "-C", str(lab_dir), "rev-parse", "HEAD"], text=True).strip()
    if revision != COURSE_REVISION:
        raise ValueError(f"Course checkout must be at {COURSE_REVISION}")
    # Export only tracked base models, excluding any local exercise solution or old dbt output.
    base_project = subprocess.check_output(
        ["git", "-C", str(lab_dir), "archive", "--format=zip", COURSE_REVISION, "dbt_project"])
    run_dir.mkdir(parents=True, exist_ok=False)
    with ZipFile(BytesIO(base_project)) as archive:
        archive.extractall(run_dir)
    project = run_dir / "dbt_project"
    for name in MODELS:
        target = project / "models" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / "etl/dbt/models" / name, target)
    warehouse = lab_dir / "warehouse.duckdb"
    environment = dict(os.environ, PYTHONUTF8="1", PYTHONIOENCODING="utf-8",
                       DBT_SEND_ANONYMOUS_USAGE_STATS="false", WAREHOUSE_PATH=str(warehouse))

    def run(command, cwd=lab_dir):
        subprocess.run([str(part) for part in command], cwd=cwd, env=environment, check=True)

    run([etl_python, lab_dir / "pipelines/02_dlt_pipeline.py", "--offline"])
    run([etl_python, ROOT / "etl/load_reviews.py", lab_dir])
    run([dbt, "build", "--project-dir", project, "--profiles-dir", project], cwd=project)
    run([etl_python, ROOT / "etl/check_base.py", warehouse])
    for step in (1, 2, 3):
        run([etl_python, lab_dir / "exercises/check_exercise.py", step])
    run([etl_python, ROOT / "etl/check_stg_reviews.py", lab_dir])
    print(f"Batch complete. Warehouse: {warehouse}; dbt results: {project / 'target/run_results.json'}")
    return warehouse


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lab-dir", type=Path, required=True)
    parser.add_argument("--etl-python", type=Path, required=True, help="Isolated ETL interpreter")
    parser.add_argument("--run-dir", type=Path, required=True, help="New directory for dbt project and results")
    args = parser.parse_args()
    prepare_batch(args.lab_dir, args.etl_python, args.run_dir)


if __name__ == "__main__":
    main()
