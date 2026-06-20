"""dataform pattern: build a Dagster asset that runs a Dataform tag or action.

Compiles the repo, submits a workflow invocation scoped to the configured tag(s)/target(s), polls to
completion. On failure, hands the precise invocation id to the shared error-analyzer and re-raises.
"""
from dagster import AssetExecutionContext, AssetsDefinition, MetadataValue, asset

from orchestration_dagster.framework.config import PipelineConfig, env
from orchestration_dagster.framework.common import gcp, alerting


def build(config: PipelineConfig) -> AssetsDefinition:
    exec_type = None
    exec_cfg = None
    for s in config.schedules:
        if "tag" in s.executions:
            exec_type, exec_cfg = "tag", s.executions["tag"]
            break
        if "object" in s.executions:
            exec_type, exec_cfg = "object", s.executions["object"]
            break
    if exec_cfg is None:
        raise ValueError(f"{config.pipeline_id}: missing executions.tag or executions.object")

    @asset(
        name=config.pipeline_id,
        group_name="dataform",
        op_tags={"pattern": "dataform", "service": "dataform"},
        tags={f"tag_{t}": "" for t in config.tag_names},
        compute_kind="dataform",
    )
    def _asset(context: AssetExecutionContext):
        context.log.info("Running Dataform %s for %s", exec_type, config.pipeline_id)
        try:
            invocation_id = gcp.run_dataform(exec_type, exec_cfg)
        except gcp.DataformInvocationError as e:
            _handle_failure(context, e.invocation_id, e.repo)
            raise
        context.add_output_metadata({
            "workflow_invocation_id": invocation_id,
            "execution_type": exec_type,
            "console": MetadataValue.url(
                f"https://console.cloud.google.com/bigquery/dataform/locations/"
                f"{env('DATAFORM_REGION', 'us-central1')}/repositories/{env('DATAFORM_REPO')}"
                f"/workflows/{invocation_id}?project={env('DATAFORM_PROJECT')}"
            ),
        })

    def _handle_failure(context: AssetExecutionContext, invocation_id: str, repo: str) -> None:
        payload = {
            "service": "dataform",
            "project_id": env("DATAFORM_PROJECT", "your-gcp-project-id"),
            "region": env("DATAFORM_REGION", "us-central1"),
            "pipeline_id": config.pipeline_id,
            "task_id": "create_workflow_invocation",
            "repository_id": repo,
            "workflow_invocation_id": invocation_id,
        }
        context.log.error("Dataform invocation %s failed; invoking error-analyzer", invocation_id)
        try:
            alerting.call_error_analyzer(payload)
        except Exception as e:  # noqa: BLE001
            alerting.notify_failure_lightweight(config.pipeline_id, "create_workflow_invocation",
                                                context.run_id, f"Dataform {invocation_id} failed; analyzer error: {e}")

    return _asset
