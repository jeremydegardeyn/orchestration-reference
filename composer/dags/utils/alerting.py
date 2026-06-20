"""Error → LLM → alert bridge for Composer.

Two paths:
  1. notify_failure_lightweight(): a cheap, synchronous Teams ping from the on_failure_callback so a
     human knows immediately, even if the LLM service is down.
  2. call_error_analyzer(): invoked by the admin__agent_analysis DAG to get a full LLM root-cause
     analysis from the shared error-analyzer Cloud Run service (which also posts the rich alert).

Keeping the two separate means a flaky LLM never suppresses the basic alert.
"""
import json
import logging

import requests
import google.auth.transport.requests
import google.oauth2.id_token

from dags.utils import common_utils

logger = logging.getLogger(__name__)


def _analyzer_url() -> str:
    return common_utils.env_override("", "ERROR_ANALYZER_URL")


def _teams_webhook() -> str:
    # In real use, read from Secret Manager; env var here for brevity.
    return common_utils.env_override("", "ALERT_TEAMS_CHANNEL")


def notify_failure_lightweight(dag_id: str, task_id: str, run_id: str, exception: str) -> None:
    webhook = _teams_webhook()
    if not webhook:
        logger.warning("No ALERT_TEAMS_CHANNEL set; failure on %s.%s: %s", dag_id, task_id, exception)
        return
    env = common_utils.env_override("dev", "ENV")
    body = (
        f"<strong>EDP DAG failure in {env}</strong><br/>"
        f"<div>dag_id: {dag_id}</div><div>task_id: {task_id}</div><div>run_id: {run_id}</div>"
        f"<p>{exception}</p>"
    )
    try:
        requests.post(
            webhook,
            data=json.dumps({"attachments": [{"contentType": "text/html", "content": body}]}),
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
    except requests.RequestException as e:
        logger.error("Lightweight Teams alert failed: %s", e)


def call_error_analyzer(payload: dict) -> dict:
    """POST the failure payload to the shared analyzer (ID-token authenticated, private Cloud Run)."""
    url = _analyzer_url()
    if not url:
        logger.error("ERROR_ANALYZER_URL not configured; skipping LLM analysis")
        return {"status": "skipped", "reason": "no url"}

    # Cloud Run private invocation needs a Google-signed ID token for the target audience.
    auth_req = google.auth.transport.requests.Request()
    token = google.oauth2.id_token.fetch_id_token(auth_req, url)

    resp = requests.post(
        f"{url}/analyze",
        json=payload,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=180,
    )
    resp.raise_for_status()
    return resp.json()
