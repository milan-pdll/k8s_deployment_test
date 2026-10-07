"""Scheduled database maintenance: `python -m pgs_db.jobs <job>` as the pgs_jobs role.

The jobs (database/src/pgs_db/jobs.py) rebuild the Gold summaries the API serves (the
map's per-region counts, the admin dashboard's domain stats), recompute page scores,
link domains to local bodies, hand claims of crashed ETL/indexer processes back to the
queue and purge old logs. Each job runs in one transaction under an advisory lock, so
an overlapping run skips instead of doubling up. They run with the ETL image's
/opt/etl-venv interpreter (pgs-db needs SQLAlchemy 2; Airflow's own environment has 1.4).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PYTHON = "/opt/etl-venv/bin/python"
# dag_id -> (schedule, jobs in order). Mirrors the schedule suggested in pgs_db/jobs.py.
SCHEDULES = {
    "pgs_db_jobs_frequent": ("*/10 * * * *", ["release-stale", "stats"]),
    "pgs_db_jobs_hourly": ("17 * * * *", ["scores"]),
    "pgs_db_jobs_daily": ("43 2 * * *", ["reference", "purge"]),
}


def _job_env() -> dict[str, str]:
    return {
        # Role pgs_jobs, not the schema owner and not the ETL's role.
        "DATABASE_URL": os.environ.get("PGS_JOBS_DATABASE_URL", ""),
        "DB_APPLICATION_NAME": "pgs-jobs",
    }


for dag_id, (schedule, jobs) in SCHEDULES.items():
    with DAG(
        dag_id=dag_id,
        description=f"pgs_db maintenance: {', '.join(jobs)}",
        schedule=schedule,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
        tags=["database", "maintenance"],
    ) as dag:
        previous = None
        for job in jobs:
            task = BashOperator(
                task_id=job.replace("-", "_"),
                bash_command=f"{PYTHON} -m pgs_db.jobs {job}",
                env=_job_env(),
                append_env=True,
                execution_timeout=timedelta(minutes=30),
            )
            if previous is not None:
                previous >> task
            previous = task
    globals()[dag_id] = dag
