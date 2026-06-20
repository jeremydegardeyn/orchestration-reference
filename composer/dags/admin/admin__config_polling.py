"""ADMIN: poll configs and trigger pipelines whose cron window is currently active.

This implements the "external polling" trigger model from the reference: instead of relying purely on
Airflow's own scheduler, a frequently-running poller scans configs and fires the DAGs that are due.
Useful when upstream readiness (a .DONE marker, an external signal) gates the real run.

Here it's simplified to a cron-window check; extend check_due() with your readiness signal.
"""
import os
from datetime import datetime, timedelta

import pytz
from croniter import croniter

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import dates

from dags.utils import common_utils

CONFIG_ROOT = "/home/airflow/gcs/dags/configs"
PATTERNS = ["gcs_to_bigquery", "dataform"]
POLL_MINUTES = int(common_utils.env_override("15", "AIRFLOW_POLL_INTERVAL_MINUTES"))


def _cron_active(expr: str, tz: str) -> bool:
    now = datetime.now(pytz.timezone(tz)) + timedelta(seconds=30)
    floor = croniter(expr, now).get_prev(datetime)
    return floor < now < floor + timedelta(minutes=POLL_MINUTES)


def _due_dags() -> list[dict]:
    due = []
    for pattern in PATTERNS:
        path = f"{CONFIG_ROOT}/{pattern}"
        if not os.path.isdir(path):
            continue
        for fn in os.listdir(path):
            if not fn.endswith((".yaml", ".yml")):
                continue
            cfg = common_utils.get_yaml_file_contents(os.path.join(path, fn))
            for s in cfg.get("schedules", []):
                interval = (s.get("interval") or "").strip()
                tz = (s.get("timezone") or "UTC").strip()
                # Only poll-triggered configs participate here.
                if s.get("trigger_type", {}).get("external_polling") and interval and _cron_active(interval, tz):
                    due.append({"dag_id": f"{pattern}__{os.path.splitext(fn)[0]}"})
    return due


with DAG(
    dag_id="admin__config_polling",
    schedule="*/15 * * * *",
    start_date=dates.days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["admin", "polling"],
    default_args={"owner": "edp"},
):

    @task()
    def find_due() -> list[dict]:
        due = _due_dags()
        print(f"{len(due)} due pipeline(s): {due}")
        return due

    # Dynamic-task-mapping fan-out: trigger each due DAG.
    TriggerDagRunOperator.partial(
        task_id="trigger_due",
        wait_for_completion=False,
    ).expand_kwargs(find_due())
