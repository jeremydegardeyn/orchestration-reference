"""Error → LLM → alert bridge for Dagster.

Same contract as the Composer alerting module: a lightweight immediate Teams ping, plus a call to the
shared error-analyzer Cloud Run service for full LLM root-cause analysis. Invoked from the run-failure
sensor (see sensors/failure_sensor.py).
"""
import json
import logging

import requests
import google.auth.transport.requests
import google.oauth2.id_token

from orchestration_dagster.framework.config import env

logger = logging.getLogger(__name__)


def notify_failure_lightweight(pipeline_id: str, step: str, run_id: str, message: str) -> None:
    webhook = env("ALERT_TEAMS_CHANNEL")
    if not webhook:
        logger.warning("No ALERT_TEAMS_CHANNEL; failure in %s/%s: %s", pipeline_id, step, message)
        return
    body = (
        f"<strong>EDP pipeline failure in {env('ENV', 'dev')}</strong><br/>"
        f"<div>pipeline: {pipeline_id}</div><div>step: {step}</div><div>run_id: {run_id}</div>"
        f"<p>{message}</p>"
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
    url = env("ERROR_ANALYZER_URL")
    if not url:
        logger.error("ERROR_ANALYZER_URL not set; skipping LLM analysis")
        return {"status": "skipped"}
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
