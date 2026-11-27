"""Airflow version of the pipeline that GitHub Actions runs today (.github/workflows/collect.yml).

fetch_bronze -> build_silver_gold -> quality_gate
                                  \\-> publish

Same scripts, same 10-minute schedule. Airflow adds what Actions can't: retries with backoff,
an alert when the quality gate fails, and a run history per task.
"""
from datetime import datetime, timedelta

from airflow import DAG

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2
    from airflow.operators.bash import BashOperator

REPO = "/opt/airflow/repos/flight-of-passage-live"

default_args = {
    "owner": "lalithya",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=5),
}

with DAG(
    dag_id="flight_of_passage_waits",
    description="Animal Kingdom wait times: bronze -> silver -> gold (SQL) -> quality gate",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 11, 26),
    catchup=False,          # a missed snapshot can't be fetched later, so never backfill
    max_active_runs=1,      # runs append to the same history; keep them in order
    default_args=default_args,
    tags=["portfolio", "medallion", "data-quality"],
) as dag:
    fetch_bronze = BashOperator(task_id="fetch_bronze", bash_command=f"cd {REPO} && python pipeline/collect.py")

    build_silver_gold = BashOperator(
        task_id="build_silver_gold",
        bash_command=f"cd {REPO} && python pipeline/build.py",  # silver in Python, gold in DuckDB SQL (pipeline/sql/)
    )

    quality_gate = BashOperator(
        task_id="quality_gate",
        bash_command=f"cd {REPO} && python pipeline/check.py",
        retries=0,  # a failed check is a signal, not a flake
    )

    publish = BashOperator(
        task_id="publish",
        bash_command=(
            f"cd {REPO} && git add data && (git diff --cached --quiet || "
            "git commit -m \"data: snapshot $(date -u +%Y-%m-%dT%H:%MZ)\" && git push)"
        ),
    )

    fetch_bronze >> build_silver_gold >> [quality_gate, publish]
