"""HTTP entrypoint so both orchestration platforms can invoke the analyzer.

POST /analyze with a FailurePayload. The service runs the ADK root agent, which routes to the right
specialist, analyzes the underlying job logs, and alerts once. A deterministic fallback path
(`analyze_and_alert`) runs if the LLM/Vertex call fails, so an alert is ALWAYS sent.

Run locally:   uvicorn error_analyzer.server:app --port 8080
Container:     see Dockerfile (same command)
"""
import asyncio
import json
import logging

from fastapi import FastAPI
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types
from pydantic import BaseModel

from .agent import root_agent
from .tools.dataflow_logs import get_dataflow_job_messages
from .tools.dataform_logs import get_dataform_failed_actions
from .tools.notifier import send_alert

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("error_analyzer")

app = FastAPI(title="EDP error-analyzer")


class FailurePayload(BaseModel):
    service: str                    # "dataflow" | "dataform"
    project_id: str
    region: str
    pipeline_id: str                # dag_id / dagster job or asset
    task_id: str
    job_id: str | None = None       # dataflow
    workflow_invocation_id: str | None = None  # dataform
    repository_id: str | None = None           # dataform
    runtime_exception: str | None = None


def analyze_and_alert(payload: FailurePayload) -> dict:
    """Deterministic, LLM-free path. Pulls logs and sends a structured alert.

    Used as the guaranteed fallback if the agent run fails. Also handy for tests.
    """
    if payload.service == "dataflow":
        logs = get_dataflow_job_messages(payload.project_id, payload.region, payload.job_id or "")
        first = logs["errors"][0]["text"] if logs.get("errors") else (payload.runtime_exception or "unknown")
        details = {
            "Pipeline": payload.pipeline_id,
            "Task": payload.task_id,
            "Project": payload.project_id,
            "Region": payload.region,
            "Job ID": payload.job_id,
            "Console": logs.get("link"),
        }
        return send_alert("Dataflow", f"Dataflow job failed: {first}", json.dumps(details),
                          "Inspect the job console link; check input data, schema, and worker quota.")
    elif payload.service == "dataform":
        actions = get_dataform_failed_actions(
            payload.project_id, payload.region, payload.repository_id or "",
            payload.workflow_invocation_id or "")
        failed = actions.get("failed_actions") or []
        first = failed[0]["failure_reason"] if failed else (payload.runtime_exception or "unknown")
        details = {
            "Pipeline": payload.pipeline_id,
            "Task": payload.task_id,
            "Repository": payload.repository_id,
            "Failed targets": ", ".join(a["target"] for a in failed) or "n/a",
            "Console": actions.get("link"),
        }
        return send_alert("Dataform", f"Dataform workflow failed: {first}", json.dumps(details),
                          "Open the workflow console link; review the failing target's SQL and dependencies.")
    return send_alert("Unknown", payload.runtime_exception or "unknown failure", "{}", "Inspect orchestrator logs.")


async def _run_agent(payload: FailurePayload) -> str:
    session_service = InMemorySessionService()
    runner = Runner(agent=root_agent, app_name="error_analyzer", session_service=session_service)
    await session_service.create_session(app_name="error_analyzer", user_id="orchestrator", session_id=payload.pipeline_id)
    content = types.Content(role="user", parts=[types.Part(text=payload.model_dump_json())])
    final = ""
    async for event in runner.run_async(user_id="orchestrator", session_id=payload.pipeline_id, new_message=content):
        if event.is_final_response() and event.content and event.content.parts:
            final = event.content.parts[0].text or ""
    return final


@app.post("/analyze")
async def analyze(payload: FailurePayload) -> dict:
    try:
        summary = await asyncio.wait_for(_run_agent(payload), timeout=120)
        return {"status": "analyzed", "summary": summary}
    except Exception as e:  # noqa: BLE001 - never fail silently; always alert
        logger.exception("Agent run failed, using deterministic fallback")
        result = analyze_and_alert(payload)
        return {"status": "fallback", "fallback_alert": result, "error": str(e)}


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}
