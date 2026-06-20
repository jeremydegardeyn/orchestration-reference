"""Tool: pull error messages from a failed Dataflow job."""
import logging

from google.cloud import dataflow_v1beta3

logger = logging.getLogger(__name__)


def get_dataflow_job_messages(project_id: str, region: str, job_id: str) -> dict:
    """Fetch error-level messages for a Dataflow job.

    Args:
        project_id: GCP project that owns the job.
        region: Dataflow regional endpoint (e.g. "us-central1").
        job_id: The Dataflow job id.

    Returns:
        dict with "errors" (list of {time, text}) and a console "link".
    """
    link = (
        f"https://console.cloud.google.com/dataflow/jobs/{region}/{job_id}"
        f"?project={project_id}"
    )
    try:
        client = dataflow_v1beta3.MessagesV1Beta3Client()
        request = dataflow_v1beta3.ListJobMessagesRequest(
            project_id=project_id,
            location=region,
            job_id=job_id,
            minimum_importance=dataflow_v1beta3.JobMessageImportance.JOB_MESSAGE_ERROR,
        )
        errors = [
            {"time": str(m.time), "text": m.message_text}
            for m in client.list_job_messages(request=request)
        ]
        return {"errors": errors[:25], "link": link}
    except Exception as e:  # noqa: BLE001 - surface retrieval failure to the LLM
        logger.warning("Failed to pull Dataflow messages for %s: %s", job_id, e)
        return {"errors": [], "link": link, "retrieval_error": str(e)}
