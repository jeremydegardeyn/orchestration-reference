"""Common utilities shared by every Composer pipeline pattern.

Split of concerns:
  - build-time helpers (parse_tags, generate_schedule, parse_outlets...) used by dag_builder.py
  - runtime helpers (read_config, final_status, failure_notification...) used inside DAGs

Alerting is delegated to dags/utils/alerting.py, which calls the shared error-analyzer service.
"""
import os
import json
import logging

import yaml
from jinja2 import Template

logger = logging.getLogger(__name__)

CONFIG_ROOT = "/home/airflow/gcs/dags/configs"


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------
def env_override(value, key):
    """Resolve an env var with a default. Used both at build time and runtime."""
    return os.getenv(key, value)


def str_to_bool(s) -> bool:
    if isinstance(s, bool):
        return s
    return str(s).strip().lower() == "true"


def get_yaml_file_contents(path: str) -> dict:
    with open(path) as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            raise Exception(f"ERROR reading {path}: {exc}")


# ---------------------------------------------------------------------------
# Build-time: derive DAG attributes from a config dict
# ---------------------------------------------------------------------------
def parse_tags(config: dict) -> str:
    tags = (config.get("dag_attr", {}) or {}).get("tags", []) or []
    names = [t["name"] for t in tags if "name" in t]
    return json.dumps(names)


def parse_alert_after_run_minutes(config: dict):
    return (config.get("dag_attr", {}) or {}).get("alert_after_run_minutes", "")


def parse_outlets(config: dict) -> str:
    """Datasets this DAG produces (for Airflow Dataset-driven scheduling). Empty list if none."""
    events = (config.get("dag_attr", {}) or {}).get("events", {}) or {}
    updates = events.get("updates_datasets", []) or []
    names = [u["name"] for u in updates if "name" in u]
    # Emit a Python list literal of Dataset(...) refs into the template.
    if not names:
        return "[]"
    refs = ", ".join(f'Dataset("{n}")' for n in names)
    return f"[{refs}]"


def generate_schedule(config: dict) -> str:
    """Produce the `schedule=` value for the DAG: a cron string, dataset trigger, or None.

    Returns a *string of Python* that the template injects verbatim, e.g. '"0 3 * * MON"' or 'None'.
    """
    schedules = config.get("schedules", []) or []
    cron = None
    for s in schedules:
        interval = (s.get("interval") or "").strip()
        if interval:
            cron = interval
            break
    if cron:
        return json.dumps(cron)  # -> "0 3 * * MON"
    return "None"


# ---------------------------------------------------------------------------
# Runtime: inside the DAG
# ---------------------------------------------------------------------------
# Airflow is only present in the Composer runtime, not at build time (dag_builder.py runs in plain CI).
# Guard the imports with a no-op shim so the build-time helpers above stay importable without Airflow.
try:  # noqa: SIM105
    from airflow.decorators import task  # noqa: E402
    from airflow.utils.state import State  # noqa: E402
    from airflow.operators.python import get_current_context  # noqa: E402
except ImportError:  # build-time / unit-test context
    State = None  # type: ignore

    def task(*_args, **_kwargs):  # type: ignore
        def _decorator(fn):
            return fn

        if _args and callable(_args[0]):
            return _args[0]
        return _decorator

    def get_current_context():  # type: ignore
        raise RuntimeError("get_current_context is only available in the Airflow runtime")


@task()
def read_config(**kwargs):
    """Read the config for this DAG: either passed via dag_run.conf or resolved from the dag_id."""
    conf = kwargs["dag_run"].conf or {}
    config = conf.get("config")
    if config is None:
        parts = kwargs["dag"].dag_id.split("__")
        pattern = parts[0]
        name = "__".join(parts[1:])
        config = get_yaml_file_contents(f"{CONFIG_ROOT}/{pattern}/{name}.yaml")
    kwargs["ti"].xcom_push("config", config)


def render_jinja_template(template_str: str, **context) -> str:
    return Template(template_str).render(**context)


def render_embedded_jinja(task: str, key: str):
    """Render Jinja embedded inside an XCom dict (used by the dataform workflow request)."""
    context = get_current_context()
    ti = context["ti"]
    payload = ti.xcom_pull(task_ids=task, key=key)
    rendered = {
        k: render_jinja_template(v, **context) if isinstance(v, str) else v
        for k, v in payload.items()
    }
    ti.xcom_push(key=key, value=rendered)


def final_status(**kwargs):
    """Terminator task: fail the DAG run if any upstream task failed (so the run is red)."""
    for ti in kwargs["dag_run"].get_task_instances():
        if ti.current_state() == State.FAILED and ti.task_id != kwargs["task_instance"].task_id:
            raise Exception(f"Task {ti.task_id} failed")


def failure_notification(context):
    """on_failure_callback: emit the control event, then the human-facing Teams ping.

    Two outputs, deliberately ordered and deliberately different in kind:

    1. The **control event** (FinChat ADR-0026) is the record. It carries no exception
       text, correlates on dag_id+run_id so retries collapse, and reaches ServiceNow as
       an em_event through the shared dispatch pipeline.
    2. The **Teams ping** is break-glass notification, not a record. It exists so a human
       still hears about a failure when the downstream path is broken — which is exactly
       the case the record cannot cover, because a broken path cannot report itself.

    The control event goes first: if anything below it throws, the auditable record has
    already been written.
    """
    if env_override("ON", "ALERTS_ON_OFF") != "ON":
        return
    from dags.utils import alerting, control_events

    ti = context["ti"]

    try:
        control_events.emit_dag_failure(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
            # Emitter-set and therefore advisory. Composer has no Cloud Run service name
            # to derive a trustworthy environment from, so the dispatch workflow falls
            # back to this value for orchestration events (see docs/26 F18).
            environment=env_override("dev", "ENV"),
            owner=(ti.task.owner if getattr(ti, "task", None) else None),
        )
    except Exception:  # never let evidence break the task it is describing
        pass

    alerting.notify_failure_lightweight(
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=ti.run_id,
        exception=str(context.get("exception")),
    )
