"""Root error-analyzer agent and its specialist sub-agents.

Topology (ADK):

    root_agent (router)
    ├── dataflow_agent   → get_dataflow_job_messages
    ├── dataform_agent   → get_dataform_failed_actions
    └── alerting_agent   → send_alert   (called exactly once)

The root agent reads `LLM_MODEL` from the environment so the model is swappable per environment.
"""
import os

from google.adk.agents import Agent

from . import prompt
from .tools.dataflow_logs import get_dataflow_job_messages
from .tools.dataform_logs import get_dataform_failed_actions
from .tools.notifier import send_alert

_MODEL = os.getenv("LLM_MODEL", "gemini-2.5-pro")

dataflow_agent = Agent(
    name="dataflow_agent",
    model=_MODEL,
    description="Retrieves and analyzes Google Cloud Dataflow job errors.",
    instruction=prompt.DATAFLOW_AGENT_INSTR,
    tools=[get_dataflow_job_messages],
)

dataform_agent = Agent(
    name="dataform_agent",
    model=_MODEL,
    description="Retrieves and analyzes Google Cloud Dataform workflow invocation errors.",
    instruction=prompt.DATAFORM_AGENT_INSTR,
    tools=[get_dataform_failed_actions],
)

alerting_agent = Agent(
    name="alerting_agent",
    model=_MODEL,
    description="Formats and sends a single alert to the configured channel.",
    instruction=prompt.ALERTING_AGENT_INSTR,
    tools=[send_alert],
)

root_agent = Agent(
    name="edp_error_analyzer_agent",
    model=_MODEL,
    description="Routes a pipeline failure to the right specialist and alerts once.",
    instruction=prompt.ROOT_AGENT_INSTR,
    sub_agents=[dataflow_agent, dataform_agent, alerting_agent],
)
