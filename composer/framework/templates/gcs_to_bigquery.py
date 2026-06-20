"""TEMPLATE: gcs_to_bigquery pattern.

Rendered per config by framework/dag_builder.py. Lands GCS files into BigQuery by launching a
Dataflow Flex Template (with a DTS fallback path selected by `workflow_type` in the config).
On failure, the terminator routes to the shared error-analyzer via the admin agent_analysis DAG.

Templating: the `env_override('KEY')` Jinja filter resolves at build time from the builder's env.
Airflow's own xcom_pull macros are wrapped in raw blocks so they survive build-time rendering.
"""
from datetime import timedelta
from time import time

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.utils import dates
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery_dts import (
    BigQueryDataTransferServiceStartTransferRunsOperator,
)

from dags.utils import common_utils
from dags.utils import gcs_to_bigquery_utils

project_id = "{{ 'default' | env_override('INGESTION_PROJECT') }}"
location = "{{ 'default' | env_override('INGESTION_REGION') }}"

default_dag_args = {
    "start_date": dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=240),
    "owner": "edp",
    "depends_on_past": False,
    "on_failure_callback": common_utils.failure_notification,
}

with DAG(
    dag_id="{{ 'default' | env_override('DAG_ID') }}",
    schedule={{ 'default' | env_override('DAG_SCHEDULE') }},
    tags={{ 'default' | env_override('DAG_TAGS') }},
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args=default_dag_args,
):
    get_config = common_utils.read_config()
    build_execution_string = gcs_to_bigquery_utils.build_execution_string(
        project_id=project_id, location=location
    )

    def route_workflow(**kwargs):
        config = kwargs["ti"].xcom_pull(task_ids="read_config", key="config")
        return "start_dts_job" if config.get("workflow_type", "dataflow") == "dts" else "start_dataflow_job"

    branch_op = BranchPythonOperator(task_id="route_workflow", python_callable=route_workflow)

    start_dts_job = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id="start_dts_job",
        project_id="{% raw %}{{ task_instance.xcom_pull(task_ids='build_execution_string', key='project') }}{% endraw %}",
        location=location,
        transfer_config_id="{% raw %}{{ task_instance.xcom_pull(task_ids='build_execution_string', key='transfer_config_id') }}{% endraw %}",
        requested_run_time={"seconds": int(time() + 60)},
    )

    start_dataflow_job = gcs_to_bigquery_utils.run_gcs_to_bq()

    # On Dataflow failure, hand off to the shared LLM error-analyzer (admin DAG wrapper).
    trigger_agent_analysis = TriggerDagRunOperator(
        task_id="trigger_agent_analysis",
        trigger_dag_id="admin__agent_analysis",
        wait_for_completion=True,
        conf={
            "service": "dataflow",
            "task_id": "start_dataflow_job",
            "job_id": "{% raw %}{{ task_instance.xcom_pull(task_ids='start_dataflow_job', key='dataflow_job_id') }}{% endraw %}",
        },
        poke_interval=10,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    @task(provide_context=True, trigger_rule=TriggerRule.ALL_DONE, retries=0,
          on_failure_callback=None, outlets={{ 'default' | env_override('DAG_OUTLETS') }})
    def terminator(**kwargs):
        common_utils.final_status(**kwargs)

    final_status = terminator()

get_config >> build_execution_string >> branch_op
branch_op >> [start_dts_job, start_dataflow_job]
start_dataflow_job >> trigger_agent_analysis
[start_dts_job, start_dataflow_job, trigger_agent_analysis] >> final_status
