"""Tool: pull failed actions from a Dataform workflow invocation."""
import logging

from google.cloud import dataform_v1beta1

logger = logging.getLogger(__name__)


def get_dataform_failed_actions(
    project_id: str, region: str, repository_id: str, workflow_invocation_id: str
) -> dict:
    """Fetch FAILED actions for a Dataform workflow invocation.

    Returns:
        dict with "failed_actions" (list of {target, failure_reason}) and a console "link".
    """
    link = (
        f"https://console.cloud.google.com/bigquery/dataform/locations/{region}"
        f"/repositories/{repository_id}/workflows/{workflow_invocation_id}?project={project_id}"
    )
    name = (
        f"projects/{project_id}/locations/{region}/repositories/{repository_id}"
        f"/workflowInvocations/{workflow_invocation_id}"
    )
    try:
        client = dataform_v1beta1.DataformClient()
        request = dataform_v1beta1.QueryWorkflowInvocationActionsRequest(name=name)
        failed = []
        for action in client.query_workflow_invocation_actions(request=request):
            if action.state.name == "FAILED":
                t = action.canonical_target
                failed.append(
                    {
                        "target": f"{t.database}.{t.schema}.{t.name}",
                        "failure_reason": action.failure_reason,
                    }
                )
        return {"failed_actions": failed[:25], "link": link}
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to pull Dataform actions for %s: %s", workflow_invocation_id, e)
        return {"failed_actions": [], "link": link, "retrieval_error": str(e)}
