"""gcs_to_bigquery pattern utilities: build the execution params and launch the Dataflow Flex Template."""
import time
import logging

import google.auth
from googleapiclient.discovery import build
from airflow.decorators import task

from dags.utils import common_utils

logger = logging.getLogger(__name__)


@task()
def build_execution_string(project_id: str, location: str, **kwargs):
    """Read the config from XCom and push a consolidated dataflow_config (or DTS params) for downstream tasks."""
    config = kwargs["ti"].xcom_pull(task_ids="read_config", key="config")
    workflow_type = config.get("workflow_type", "dataflow")

    schedules = config.get("schedules", [config])
    df_block = None
    for s in schedules:
        execs = s.get("executions", {}) or {}
        if "gcs_to_bigquery" in execs:
            df_block = execs["gcs_to_bigquery"]
            break

    if workflow_type == "dataflow":
        if not df_block:
            raise ValueError("Missing executions.gcs_to_bigquery in config")
        source = df_block.get("source", {})
        dataflow = df_block.get("dataflow", {})
        sink = df_block.get("sink", {})
        consolidated = {
            "landing_bucket": source.get("landing_bucket"),
            "file_pattern": source.get("file_pattern"),
            "field_delimiter": source.get("field_delimiter", ","),
            "skip_leading_rows": source.get("skip_leading_rows", 0),
            "max_bad_records": source.get("max_bad_records", 0),
            "is_truncate": source.get("is_truncate", False),
            "quote_char": source.get("quote_char", '"'),
            "machine_type": dataflow.get("machine_type", "n1-standard-2"),
            "max_workers": dataflow.get("max_workers", 4),
            "disk_size_gb": dataflow.get("disk_size_gb", 25),
            "raw_dataset": sink.get("raw_dataset"),
            "raw_table": sink.get("raw_table"),
            "error_bucket": sink.get("error_bucket"),
        }
        missing = [k for k, v in consolidated.items() if v is None]
        if missing:
            raise ValueError(f"Missing config values: {missing}")
        kwargs["ti"].xcom_push("dataflow_config", consolidated)
    else:
        # DTS path: resolve the transfer config by display name (left as an exercise; see reference).
        raise NotImplementedError("DTS path not implemented in this reference; use workflow_type: dataflow")


@task(task_id="start_dataflow_job")
def run_gcs_to_bq(**kwargs):
    """Launch the gcs_to_bigquery Dataflow Flex Template and poll to a terminal state."""
    ti = kwargs["ti"]
    cfg = ti.xcom_pull(task_ids="build_execution_string", key="dataflow_config")
    config = ti.xcom_pull(task_ids="read_config", key="config")

    tags = [t["name"].replace("_", "-") for t in config.get("dag_attr", {}).get("tags", [])]
    job_name = "gcs-to-bigquery--" + "-".join(tags)
    feed_name = "-".join(tags)

    work_project = common_utils.env_override("default", "WORK_PROJECT")
    raw_project = common_utils.env_override("default", "RAW_PROJECT")
    dataflow_bucket = common_utils.env_override("default", "DATAFLOW_BUCKET")
    dataflow_region = common_utils.env_override("default", "DATAFLOW_REGION")
    dataflow_subnet = common_utils.env_override("default", "DATAFLOW_SUBNETWORK")
    dataflow_sa = common_utils.env_override("default", "INGESTION_SA")

    body = {
        "launchParameter": {
            "containerSpecGcsPath": f"gs://{dataflow_bucket}/templates/gcs_to_bigquery.json",
            "jobName": job_name,
            "environment": {
                "machineType": cfg["machine_type"],
                "maxWorkers": cfg["max_workers"],
                "subnetwork": dataflow_subnet,
                "workerRegion": dataflow_region,
                "additionalExperiments": ["use_runner_v2"],
                "serviceAccountEmail": dataflow_sa,
                "tempLocation": f"gs://{dataflow_bucket}/temp",
                "stagingLocation": f"gs://{dataflow_bucket}/staging",
                "ipConfiguration": "WORKER_IP_PRIVATE",
                "diskSizeGb": cfg["disk_size_gb"],
            },
            "parameters": {
                "input_file": f"gs://{cfg['landing_bucket']}/{cfg['file_pattern']}",
                "is_truncate": str(cfg["is_truncate"]).lower(),
                "feed_name": feed_name,
                "raw_bq_table": f"{raw_project}.{cfg['raw_dataset']}.{cfg['raw_table']}",
                "err_bq_table": f"{raw_project}.admin.error_logs_gcs_to_bq",
                "max_bad_records": str(cfg["max_bad_records"]),
                "error_bucket": cfg["error_bucket"],
                "delimiter": cfg["field_delimiter"],
                "quote_char": cfg["quote_char"],
                "skip_leading_rows": str(cfg["skip_leading_rows"]),
                "pipeline_run_id": kwargs["run_id"],
            },
        }
    }

    credentials, _ = google.auth.default()
    dataflow = build("dataflow", "v1b3", credentials=credentials)
    resp = (
        dataflow.projects().locations().flexTemplates()
        .launch(projectId=work_project, location=dataflow_region, body=body)
        .execute()
    )
    job_id = resp["job"]["id"]
    logger.info("Dataflow job launched: %s", job_id)
    ti.xcom_push(key="dataflow_job_id", value=job_id)

    # Poll to terminal state.
    while True:
        job = (
            dataflow.projects().locations().jobs()
            .get(projectId=work_project, location=dataflow_region, jobId=job_id)
            .execute()
        )
        state = job.get("currentState")
        logger.info("Job %s state: %s", job_id, state)
        if state == "JOB_STATE_DONE":
            return
        if state in ("JOB_STATE_FAILED", "JOB_STATE_CANCELLED"):
            raise Exception(f"Dataflow job {job_id} ended in {state}")
        time.sleep(60)
