"""
Canonical technical-control event envelope — Composer emitter (FinChat ADR-0026).

**This is a deliberate mirror of `ui/control_events.py` in the finchat repo.** The two
repositories deploy independently and share no package, so the envelope is duplicated
rather than imported. That duplication is the drift risk, and it is managed the only way
duplication can be: `ENVELOPE_KEYS` is frozen and asserted by tests on *both* sides, so a
field added to one and not the other fails a build instead of quietly producing events
the dispatch workflow cannot read.

A DAG failure is an operational control signal in exactly the same sense a Model Armor
block is a security one: something the platform was supposed to do did not happen, and an
auditor's question is the same — was anyone told. Emitting the identical envelope means
one Cloud Logging sink, one dispatch workflow, and one correlation domain in ServiceNow
serve both, instead of orchestration growing its own parallel alerting stack.

Emission is a JSON line on stdout. Airflow's task logger ships stdout to Cloud Logging,
so there is no client library, no credentials, and nothing that can fail a task.

Gated by CONTROL_EVENTS=1. Off, emit() is a no-op.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timezone

SOURCES = ("model_armor", "dlp", "composer", "scc")
SEVERITIES = ("INFO", "WARNING", "ERROR", "CRITICAL")

# Frozen contract. Mirrored in finchat ui/control_events.py — change both or neither.
ENVELOPE_KEYS = frozenset({
    "control_id",
    "source",
    "environment",
    "severity",
    "message_key",
    "occurred_at",
    "principal_hash",
    "evidence_ref",
    "filters",
})

SALT = os.getenv("CONTROL_EVENT_SALT", "")


def enabled() -> bool:
    return os.getenv("CONTROL_EVENTS", "").lower() in ("1", "true", "yes")


def principal_hash(principal: str | None) -> str:
    if not principal:
        return "anonymous"
    return hashlib.sha256(f"{SALT}:{principal}".encode("utf-8")).hexdigest()[:16]


def build(*, control_id: str, source: str, environment: str, severity: str = "WARNING",
          principal: str | None = None, evidence_ref: str | None = None,
          filters: list | None = None, key_parts: tuple = (),
          occurred_at: str | None = None) -> dict:
    """Construct an envelope.

    Note what this signature does not accept: no exception text, no log excerpt, no task
    output. An Airflow traceback routinely contains connection strings, row values and
    query text, and a ServiceNow incident is readable by a whole assignment group. The
    detail stays in the Airflow log, reachable from `evidence_ref`.
    """
    if source not in SOURCES:
        raise ValueError(f"unknown source {source!r}; expected one of {SOURCES}")
    if severity not in SEVERITIES:
        raise ValueError(f"unknown severity {severity!r}; expected one of {SEVERITIES}")
    return {
        "control_id": control_id,
        "source": source,
        "environment": environment,
        "severity": severity,
        "message_key": ":".join([source, control_id, *(p for p in key_parts if p)]),
        "occurred_at": occurred_at or datetime.now(timezone.utc).isoformat(),
        "principal_hash": principal_hash(principal),
        "evidence_ref": evidence_ref or "",
        "filters": sorted(filters or []),
    }


def emit(event: dict) -> None:
    """One structured line on stdout. Never raises — a control event must not fail the
    task it is describing, and the Airflow log holds the detail regardless."""
    if not enabled():
        return
    try:
        sys.stdout.write(json.dumps({
            "severity": event.get("severity", "WARNING"),
            "message": f"control_event {event.get('source')}/{event.get('control_id')}",
            "control_event": event,
        }, separators=(",", ":")) + "\n")
        sys.stdout.flush()
    except Exception:  # pragma: no cover
        pass


def emit_dag_failure(*, dag_id: str, task_id: str, run_id: str, environment: str,
                     owner: str | None = None) -> dict:
    """Emit a DAG-failure control event.

    The correlation key is dag_id + run_id, deliberately excluding task_id and try_number.
    Airflow retries a task before the run goes red, and several tasks in one run usually
    fail for one reason — so a key that included either would raise three incidents for
    one broken pipeline. Event Management collapses on message_key; `task_id` is carried
    in control_id so the first failing task is still visible on the alert.
    """
    event = build(
        control_id=f"composer.dag_failure.{task_id}",
        source="composer",
        environment=environment,
        severity="ERROR",
        principal=owner,
        evidence_ref=run_id,
        key_parts=(dag_id, run_id),
    )
    # control_id carries the task, but the KEY must not, or retries stop collapsing.
    event["message_key"] = f"composer:composer.dag_failure:{dag_id}:{run_id}"
    emit(event)
    return event
