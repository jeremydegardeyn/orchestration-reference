"""TEMPLATE: dataform pattern.

Rendered per config by framework/dag_builder.py. Executes a Dataform tag or action: compiles the
repo at a commitish, then submits a workflow invocation scoped to the configured tag(s)/target(s).
On failure, routes to the shared error-analyzer via the admin agent_analysis DAG.

Templating: the env_override Jinja filter resolves at build time; Airflow xcom_pull macros are wrapped
in raw blocks so they survive build-time rendering.
"""
from datetime import timedelta

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.utils import dates
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

from dags.utils import common_utils
from dags.utils import dataform_utils

repository_id = "{{ 'default' | env_override('DATAFORM_REPO') }}"
dataform_git_commitish = "{{ 'default' | env_override('DATAFORM_GIT_COMMITISH') }}"

# Compilation vars map logical layer names → GCP project ids (env-driven).
dataform_compilation_vars = {
    "raw": "{{ 'default' | env_override('RAW_PROJECT') }}",
    "work": "{{ 'default' | env_override('WORK_PROJECT') }}",
    "curated": "{{ 'default' | env_override('CURATED_PROJECT') }}",
}

default_dag_args = {
    "start_date": dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=240),
    "project_id": "{{ 'default' | env_override('DATAFORM_PROJECT') }}",
    "region": "{{ 'default' | env_override('DATAFORM_REGION') }}",
    "owner": "edp",
    "on_failure_callback": common_utils.failure_notification,
}

with DAG(
    dag_id="{{ 'default' | env_override('DAG_ID') }}",
    schedule={{ 'default' | env_override('DAG_SCHEDULE') }},
    tags={{ 'default' | env_override('DAG_TAGS') }},
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    render_template_as_native_obj=True,
    default_args=default_dag_args,
):
    get_config = common_utils.read_config()
    build_execution_string = dataform_utils.build_execution_string(
        dataform_git_commitish, dataform_compilation_vars
    )

    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        repository_id=repository_id,
        compilation_result="{% raw %}{{ task_instance.xcom_pull(task_ids='build_execution_string', key='compilation_request') }}{% endraw %}",
        retries=3,
    )

    render_compilation_in_workflow_request = PythonOperator(
        task_id="render_compilation_in_workflow_request",
        python_callable=common_utils.render_embedded_jinja,
        op_kwargs={"task": "build_execution_string", "key": "workflow_request"},
    )

    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id="create_workflow_invocation",
        repository_id=repository_id,
        wait_time=10,
        workflow_invocation="{% raw %}{{ task_instance.xcom_pull(task_ids='render_compilation_in_workflow_request', key='workflow_request') }}{% endraw %}",
        retries=0,
    )

    trigger_agent_analysis = TriggerDagRunOperator(
        task_id="trigger_agent_analysis",
        trigger_dag_id="admin__agent_analysis",
        wait_for_completion=True,
        conf={
            "service": "dataform",
            "task_id": "create_workflow_invocation",
            "repository_id": repository_id,
            "workflow_invocation_id": "{% raw %}{{ task_instance.xcom_pull(task_ids='create_workflow_invocation', key='dataform_workflow_invocation_config').workflow_invocation_id }}{% endraw %}",
        },
        poke_interval=10,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    @task(provide_context=True, trigger_rule=TriggerRule.ALL_DONE, retries=0,
          on_failure_callback=None, outlets={{ 'default' | env_override('DAG_OUTLETS') }})
    def terminator(**kwargs):
        common_utils.final_status(**kwargs)

    final_status = terminator()

get_config >> build_execution_string >> create_compilation_result \
    >> render_compilation_in_workflow_request >> create_workflow_invocation
create_workflow_invocation >> trigger_agent_analysis
[create_workflow_invocation, trigger_agent_analysis] >> final_status
