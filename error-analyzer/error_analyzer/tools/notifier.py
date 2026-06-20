"""Tool: send the analysis to the alert channel (Teams webhook, with email fallback).

Single-source notifier shared by the agent. Both orchestration platforms ultimately route their
failures through this so the alert format is identical regardless of platform.
"""
import json
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests

from error_analyzer.config import get_settings

logger = logging.getLogger(__name__)


def _format_html(service_name: str, summary: str, details: dict, resolution_steps: str) -> str:
    rows = "".join(f"<li><strong>{k}:</strong> {v}</li>" for k, v in details.items())
    return (
        f"<strong>EDP pipeline failure — {service_name}</strong><br/>"
        f"<p>{summary}</p>"
        f"<ul>{rows}</ul>"
        f"<p><strong>Suggested resolution:</strong></p><p>{resolution_steps}</p>"
    )


def send_alert(
    service_name: str,
    summary: str,
    details_json: str,
    resolution_steps: str,
) -> dict:
    """Format and send a single alert. Teams if configured, else email, else log-only.

    Args:
        service_name: e.g. "Dataflow" or "Dataform".
        summary: One-paragraph root-cause summary.
        details_json: JSON string of key/value metadata (project, region, job id, links).
        resolution_steps: Suggested next steps.

    Returns:
        dict {status, channel, message}.
    """
    settings = get_settings()
    try:
        details = json.loads(details_json) if details_json else {}
    except json.JSONDecodeError as e:
        details = {"raw_details": details_json, "parse_error": str(e)}

    body = _format_html(service_name, summary, details, resolution_steps)

    if not settings.alerts_enabled:
        logger.info("Alerts disabled (ALERTS_ON_OFF=OFF). Would have sent:\n%s", body)
        return {"status": "skipped", "channel": "none", "message": "alerts disabled"}

    # Prefer Teams.
    if settings.teams_webhook:
        try:
            resp = requests.post(
                settings.teams_webhook,
                data=json.dumps({"attachments": [{"contentType": "text/html", "content": body}]}),
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            return {"status": "success", "channel": "teams", "message": "sent"}
        except requests.RequestException as e:
            logger.error("Teams alert failed: %s", e)
            # fall through to email

    # Email fallback.
    if settings.smtp_host and settings.alert_recipients:
        try:
            msg = MIMEMultipart()
            msg["From"] = "noreply@edp"
            msg["To"] = settings.alert_recipients
            msg["Subject"] = f"EDP pipeline failure in {settings.env} | {service_name}"
            msg.attach(MIMEText(body, "html"))
            with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as server:
                server.sendmail(
                    msg["From"],
                    [r.strip() for r in settings.alert_recipients.split(",")],
                    msg.as_string(),
                )
            return {"status": "success", "channel": "email", "message": "sent"}
        except Exception as e:  # noqa: BLE001
            logger.error("Email alert failed: %s", e)
            return {"status": "error", "channel": "email", "message": str(e)}

    logger.warning("No alert channel configured; analysis:\n%s", body)
    return {"status": "error", "channel": "none", "message": "no channel configured"}
