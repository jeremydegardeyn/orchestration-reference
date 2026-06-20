"""GCP execution helpers shared by the patterns.

These are platform-agnostic: the same Flex Template launch + Dataform invocation logic the Composer
repo uses, here as plain functions the Dagster assets call. Keeping them free of Dagster imports makes
them unit-testable and means the two platforms stay behaviorally identical.
"""
import time
import logging

import google.auth
from googleapiclient.discovery import build
from google.cloud import dataform_v1beta1

from orchestration_dagster.framework.config import env

logger = logging.getLogger(__name__)


class DataflowJobError(Exception):
    """Raised when a launched Dataflow job ends in a non-DONE terminal state."""

    def __init__(self, job_id: str, state: str):
        self.job_id = job_id
        self.state = state
        super().__init__(f"Dataflow job {job_id} ended in {state}")


def launch_gcs_to_bq_flex_template(cfg: dict, tag_names: list[str], run_id: str) -> str:
    """Launch the gcs_to_bigquery Flex Template and poll to terminal state. Returns the job id.

    Raises DataflowJobError on failure (carrying the job id so the failure sensor can analyze it).
    """
    work_project = env("WORK_PROJECT", "your-gcp-project-id")
    raw_project = env("RAW_PROJECT", work_project)
    bucket = env("DATAFLOW_BUCKET")
    region = env("DATAFLOW_REGION", "us-central1")
    subnet = env("DATAFLOW_SUBNETWORK")
    sa = env("INGESTION_SA")

    feed = "-".join(tag_names)
    body = {
        "launchParameter": {
            "containerSpecGcsPath": f"gs://{bucket}/templates/gcs_to_bigquery.json",
            "jobName": "gcs-to-bigquery--" + feed,
            "environment": {
                "machineType": cfg["machine_type"],
                "maxWorkers": cfg["max_workers"],
                "subnetwork": subnet,
                "workerRegion": region,
                "serviceAccountEmail": sa,
                "additionalExperiments": ["use_runner_v2"],
                "tempLocation": f"gs://{bucket}/temp",
                "stagingLocation": f"gs://{bucket}/staging",
                "ipConfiguration": "WORKER_IP_PRIVATE",
                "diskSizeGb": cfg["disk_size_gb"],
            },
            "parameters": {
                "input_file": f"gs://{cfg['landing_bucket']}/{cfg['file_pattern']}",
                "is_truncate": str(cfg["is_truncate"]).lower(),
                "feed_name": feed,
                "raw_bq_table": f"{raw_project}.{cfg['raw_dataset']}.{cfg['raw_table']}",
                "err_bq_table": f"{raw_project}.admin.error_logs_gcs_to_bq",
                "max_bad_records": str(cfg["max_bad_records"]),
                "error_bucket": cfg["error_bucket"],
                "delimiter": cfg["field_delimiter"],
                "quote_char": cfg["quote_char"],
                "skip_leading_rows": str(cfg["skip_leading_rows"]),
                "pipeline_run_id": run_id,
            },
        }
    }

    credentials, _ = google.auth.default()
    dataflow = build("dataflow", "v1b3", credentials=credentials)
    resp = (
        dataflow.projects().locations().flexTemplates()
        .launch(projectId=work_project, location=region, body=body)
        .execute()
    )
    job_id = resp["job"]["id"]
    logger.info("Launched Dataflow job %s", job_id)

    while True:
        job = (
            dataflow.projects().locations().jobs()
            .get(projectId=work_project, location=region, jobId=job_id)
            .execute()
        )
        state = job.get("currentState")
        if state == "JOB_STATE_DONE":
            return job_id
        if state in ("JOB_STATE_FAILED", "JOB_STATE_CANCELLED"):
            raise DataflowJobError(job_id, state)
        time.sleep(60)


class DataformInvocationError(Exception):
    def __init__(self, invocation_id: str, repo: str):
        self.invocation_id = invocation_id
        self.repo = repo
        super().__init__(f"Dataform workflow invocation {invocation_id} failed")


def run_dataform(execution_type: str, exec_cfg: dict) -> str:
    """Compile the repo, submit a workflow invocation for a tag/object, poll to terminal. Returns invocation id."""
    project = env("DATAFORM_PROJECT", "your-gcp-project-id")
    region = env("DATAFORM_REGION", "us-central1")
    repo = env("DATAFORM_REPO", "edp-dataform")
    commitish = env("DATAFORM_GIT_COMMITISH", "main")

    client = dataform_v1beta1.DataformClient()
    parent = f"projects/{project}/locations/{region}/repositories/{repo}"

    # 1. compile
    comp = client.create_compilation_result(
        parent=parent,
        compilation_result={
            "git_commitish": commitish,
            "code_compilation_config": {
                "default_database": project,
                "vars": {"do_full_refresh": str(exec_cfg.get("do_full_refresh", False)).lower()},
            },
        },
    )

    # 2. build invocation config (tag vs object)
    if execution_type == "tag":
        inv_cfg = {"included_tags": [n["name"] for n in exec_cfg["names"]]}
    else:
        inv_cfg = {"included_targets": [
            {"database": n.get("database"), "schema": n.get("schema_") or n.get("schema"), "name": n.get("name")}
            for n in exec_cfg["names"]
        ]}
    inv_cfg.update({
        "transitive_dependencies_included": exec_cfg.get("include_dependencies", False),
        "transitive_dependents_included": exec_cfg.get("include_dependents", False),
        "fully_refresh_incremental_tables_enabled": False,
    })

    # 3. invoke
    invocation = client.create_workflow_invocation(
        parent=parent,
        workflow_invocation={"compilation_result": comp.name, "invocation_config": inv_cfg},
    )
    invocation_id = invocation.name.split("/")[-1]
    logger.info("Dataform workflow invocation %s submitted", invocation_id)

    # 4. poll
    while True:
        wi = client.get_workflow_invocation(name=invocation.name)
        state = wi.state.name
        if state == "SUCCEEDED":
            return invocation_id
        if state in ("FAILED", "CANCELLED"):
            raise DataformInvocationError(invocation_id, repo)
        time.sleep(20)
