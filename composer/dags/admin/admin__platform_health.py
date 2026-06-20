"""ADMIN: platform health. Summarize failed DAG runs in the last window and post a digest.

A safety net beyond per-DAG alerts: catches DAGs that failed to even start, import errors, and gives
on-call a single periodic "state of the platform" message.
"""
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.utils import dates
from airflow.utils.state import DagRunState
from airflow.utils.db import provide_session
from airflow.models import DagRun

from dags.utils import alerting

LOOKBACK_HOURS = 6


with DAG(
    dag_id="admin__platform_health",
    schedule="0 */6 * * *",
    start_date=dates.days_ago(1),
    catchup=False,
    tags=["admin", "health"],
    default_args={"owner": "edp"},
):

    @task()
    @provide_session
    def summarize(session=None):
        from airflow.utils.timezone import utcnow

        since = utcnow() - timedelta(hours=LOOKBACK_HOURS)
        failed = (
            session.query(DagRun)
            .filter(DagRun.state == DagRunState.FAILED, DagRun.end_date >= since)
            .all()
        )
        if not failed:
            print("No failed DAG runs in window.")
            return 0

        lines = "".join(f"<li>{r.dag_id} — {r.run_id}</li>" for r in failed)
        alerting.notify_failure_lightweight(
            dag_id="admin__platform_health",
            task_id="summarize",
            run_id=f"last_{LOOKBACK_HOURS}h",
            exception=f"{len(failed)} failed DAG run(s):<ul>{lines}</ul>",
        )
        return len(failed)

    summarize()
