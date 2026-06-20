"""ADMIN: bridge from a failed pipeline to the shared LLM error-analyzer.

Triggered (via TriggerDagRunOperator with ONE_FAILED) by any pattern DAG when its core job fails.
The failure payload arrives in dag_run.conf; we forward it to the error-analyzer Cloud Run service,
which pulls the underlying Dataflow/Dataform logs, analyzes them, and posts the rich alert.

Kept as a separate admin DAG (rather than inline) so the LLM call doesn't run on the worker holding
the pipeline slot, and so it can be tested/triggered independently.
"""
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.utils import dates

from dags.utils import alerting, common_utils

with DAG(
    dag_id="admin__agent_analysis",
    schedule=None,                 # only ever triggered by other DAGs
    start_date=dates.days_ago(1),
    catchup=False,
    max_active_runs=4,
    tags=["admin", "alerting"],
    default_args={"owner": "edp", "retries": 1, "retry_delay": timedelta(minutes=1)},
):

    @task()
    def analyze(**kwargs):
        conf = kwargs["dag_run"].conf or {}
        payload = {
            "service": conf.get("service", "dataflow"),
            "project_id": common_utils.env_override("your-gcp-project-id", "WORK_PROJECT"),
            "region": common_utils.env_override("us-central1", "DATAFLOW_REGION"),
            "pipeline_id": conf.get("parent_dag_id", conf.get("dag_id", "unknown")),
            "task_id": conf.get("task_id", "unknown"),
            "job_id": conf.get("job_id"),
            "repository_id": conf.get("repository_id"),
            "workflow_invocation_id": conf.get("workflow_invocation_id"),
        }
        result = alerting.call_error_analyzer(payload)
        print("error-analyzer result:", result)
        return result

    analyze()
