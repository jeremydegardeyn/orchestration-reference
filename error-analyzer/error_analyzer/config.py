"""Runtime configuration for the error-analyzer agent.

All values come from the environment so the same image runs in dev/qa/prod.
See env/*.env for per-environment values.
"""
import os
from functools import lru_cache

from pydantic import BaseModel


class Settings(BaseModel):
    # Vertex / model
    project_id: str = os.getenv("GOOGLE_CLOUD_PROJECT", "your-gcp-project-id")
    location: str = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
    llm_model: str = os.getenv("LLM_MODEL", "gemini-2.5-pro")

    # Alerting
    env: str = os.getenv("ENV", "dev")
    alerts_enabled: bool = os.getenv("ALERTS_ON_OFF", "ON").upper() == "ON"
    teams_webhook: str | None = os.getenv("ALERT_TEAMS_CHANNEL")
    alert_recipients: str | None = os.getenv("ALERT_RECIPIENTS")
    smtp_host: str | None = os.getenv("ALERT_SMTP_HOST")
    smtp_port: int = int(os.getenv("ALERT_SMTP_PORT", "25"))


@lru_cache
def get_settings() -> Settings:
    return Settings()
