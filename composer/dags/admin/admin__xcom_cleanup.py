"""ADMIN: housekeeping. Trim old XCom rows + clean up stale entries to keep the metadata DB lean.

Composer's metadata DB grows with XComs and task history; large XCom tables slow the scheduler.
This nightly job deletes XComs older than the retention window.
"""
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.utils import dates
from airflow.utils.db import provide_session
from airflow.models import XCom

RETENTION_DAYS = 15


with DAG(
    dag_id="admin__xcom_cleanup",
    schedule="0 2 * * *",
    start_date=dates.days_ago(1),
    catchup=False,
    tags=["admin", "housekeeping"],
    default_args={"owner": "edp", "retries": 1, "retry_delay": timedelta(minutes=5)},
):

    @task()
    @provide_session
    def purge_xcom(session=None):
        from airflow.utils.timezone import utcnow

        cutoff = utcnow() - timedelta(days=RETENTION_DAYS)
        deleted = session.query(XCom).filter(XCom.timestamp < cutoff).delete(synchronize_session=False)
        session.commit()
        print(f"Deleted {deleted} XCom rows older than {RETENTION_DAYS} days")
        return deleted

    purge_xcom()
