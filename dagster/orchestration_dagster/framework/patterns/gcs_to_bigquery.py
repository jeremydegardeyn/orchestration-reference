"""gcs_to_bigquery pattern: build a Dagster asset that lands GCS files into BigQuery via Dataflow.

The asset launches the Flex Template, polls to completion, and attaches the job id + console link as
materialization metadata. On Dataflow failure it hands the precise job id to the shared error-analyzer
(rich LLM alert) and re-raises so the run is red.
"""
from dagster import AssetExecutionContext, AssetsDefinition, MetadataValue, asset

from orchestration_dagster.framework.config import PipelineConfig, env
from orchestration_dagster.framework.common import gcp, alerting


def build(config: PipelineConfig) -> AssetsDefinition:
    exec_block = None
    for s in config.schedules:
        if "gcs_to_bigquery" in s.executions:
            exec_block = s.executions["gcs_to_bigquery"]
            break
    if exec_block is None:
        raise ValueError(f"{config.pipeline_id}: missing executions.gcs_to_bigquery")

    source = exec_block["source"]
    dataflow = exec_block.get("dataflow", {})
    sink = exec_block["sink"]
    cfg = {
        "landing_bucket": source["landing_bucket"],
        "file_pattern": source["file_pattern"],
        "field_delimiter": source.get("field_delimiter", ","),
        "skip_leading_rows": source.get("skip_leading_rows", 0),
        "max_bad_records": source.get("max_bad_records", 0),
        "is_truncate": source.get("is_truncate", False),
        "quote_char": source.get("quote_char", '"'),
        "machine_type": dataflow.get("machine_type", "n1-standard-2"),
        "max_workers": dataflow.get("max_workers", 4),
        "disk_size_gb": dataflow.get("disk_size_gb", 25),
        "raw_dataset": sink["raw_dataset"],
        "raw_table": sink["raw_table"],
        "error_bucket": sink["error_bucket"],
    }

    @asset(
        name=config.pipeline_id,
        group_name="gcs_to_bigquery",
        op_tags={"pattern": "gcs_to_bigquery", "service": "dataflow"},
        tags={f"feed_{t}": "" for t in config.tag_names},
        compute_kind="dataflow",
    )
    def _asset(context: AssetExecutionContext):
        context.log.info("Launching Dataflow flex template for %s", config.pipeline_id)
        try:
            job_id = gcp.launch_gcs_to_bq_flex_template(cfg, config.tag_names, context.run_id)
        except gcp.DataflowJobError as e:
            _handle_failure(context, e.job_id)
            raise
        context.add_output_metadata({
            "dataflow_job_id": job_id,
            "console": MetadataValue.url(
                f"https://console.cloud.google.com/dataflow/jobs/"
                f"{env('DATAFLOW_REGION', 'us-central1')}/{job_id}?project={env('WORK_PROJECT')}"
            ),
            "raw_table": f"{cfg['raw_dataset']}.{cfg['raw_table']}",
        })

    def _handle_failure(context: AssetExecutionContext, job_id: str) -> None:
        payload = {
            "service": "dataflow",
            "project_id": env("WORK_PROJECT", "your-gcp-project-id"),
            "region": env("DATAFLOW_REGION", "us-central1"),
            "pipeline_id": config.pipeline_id,
            "task_id": "launch_flex_template",
            "job_id": job_id,
        }
        context.log.error("Dataflow job %s failed; invoking error-analyzer", job_id)
        try:
            alerting.call_error_analyzer(payload)
        except Exception as e:  # noqa: BLE001 - never let alerting failure mask the real error
            alerting.notify_failure_lightweight(config.pipeline_id, "launch_flex_template",
                                                context.run_id, f"Dataflow {job_id} failed; analyzer error: {e}")

    return _asset
