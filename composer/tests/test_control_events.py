"""
Offline tests for the Composer control-event emitter (FinChat ADR-0026).

No Airflow, no GCP, no network.

The envelope is a deliberate mirror of `ui/control_events.py` in the finchat repo — two
repos, no shared package. `test_envelope_key_set_is_frozen` is the guard that keeps them
honest: if either side adds a field without the other, one of the two builds fails rather
than the dispatch workflow silently reading an envelope it does not understand.
"""
import io
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags" / "utils"))

import control_events as ce  # noqa: E402


def test_envelope_key_set_is_frozen():
    """Mirrored in finchat ui/control_events.py — change both or neither."""
    ev = ce.build(control_id="composer.dag_failure.t", source="composer", environment="dev")
    assert set(ev) == set(ce.ENVELOPE_KEYS)
    assert ce.ENVELOPE_KEYS == frozenset({
        "control_id", "source", "environment", "severity", "message_key",
        "occurred_at", "principal_hash", "evidence_ref", "filters",
    })


# --- correlation: the reason this exists ------------------------------------

def test_retries_of_one_task_collapse_to_one_key():
    """Airflow retries before the run goes red. Three tries is one failure, not three
    incidents."""
    keys = {
        ce.emit_dag_failure(dag_id="gcs_to_bigquery", task_id="load", run_id="run-1",
                            environment="prod")["message_key"]
        for _try in range(3)
    }
    assert len(keys) == 1


def test_different_tasks_in_one_run_collapse_too():
    """Several tasks failing in one run usually share one cause — one broken pipeline is
    one incident."""
    a = ce.emit_dag_failure(dag_id="d", task_id="extract", run_id="run-1", environment="prod")
    b = ce.emit_dag_failure(dag_id="d", task_id="load", run_id="run-1", environment="prod")
    assert a["message_key"] == b["message_key"]


def test_the_failing_task_is_still_visible_even_though_it_is_not_in_the_key():
    a = ce.emit_dag_failure(dag_id="d", task_id="extract", run_id="run-1", environment="prod")
    assert "extract" in a["control_id"]
    assert "extract" not in a["message_key"]


def test_separate_runs_do_not_collapse():
    a = ce.emit_dag_failure(dag_id="d", task_id="t", run_id="run-1", environment="prod")
    b = ce.emit_dag_failure(dag_id="d", task_id="t", run_id="run-2", environment="prod")
    assert a["message_key"] != b["message_key"]


def test_separate_dags_do_not_collapse():
    a = ce.emit_dag_failure(dag_id="one", task_id="t", run_id="r", environment="prod")
    b = ce.emit_dag_failure(dag_id="two", task_id="t", run_id="r", environment="prod")
    assert a["message_key"] != b["message_key"]


# --- redaction --------------------------------------------------------------

def test_build_cannot_be_handed_an_exception_or_log_excerpt():
    """An Airflow traceback routinely carries connection strings, row values and query
    text; a ServiceNow incident is readable by a whole assignment group."""
    import inspect
    params = set(inspect.signature(ce.build).parameters)
    for leaky in ("exception", "traceback", "message", "log", "detail", "output", "sql"):
        assert leaky not in params


def test_owner_is_hashed_not_carried():
    e = ce.emit_dag_failure(dag_id="d", task_id="t", run_id="r", environment="prod",
                            owner="edp-oncall@datadinosaur.com")
    assert "edp-oncall@datadinosaur.com" not in json.dumps(e)
    assert len(e["principal_hash"]) == 16


def test_missing_owner_is_explicit():
    e = ce.emit_dag_failure(dag_id="d", task_id="t", run_id="r", environment="prod")
    assert e["principal_hash"] == "anonymous"


# --- emission ---------------------------------------------------------------

def test_emit_is_a_noop_when_disabled(monkeypatch):
    monkeypatch.delenv("CONTROL_EVENTS", raising=False)
    buf = io.StringIO()
    with redirect_stdout(buf):
        ce.emit(ce.build(control_id="c", source="composer", environment="dev"))
    assert buf.getvalue() == ""


def test_emit_writes_one_parseable_json_line(monkeypatch):
    monkeypatch.setenv("CONTROL_EVENTS", "1")
    buf = io.StringIO()
    with redirect_stdout(buf):
        ce.emit_dag_failure(dag_id="d", task_id="t", run_id="r", environment="prod")
    line = json.loads(buf.getvalue().strip())
    assert line["control_event"]["source"] == "composer"
    assert line["severity"] == "ERROR"


def test_emit_never_raises_on_an_unserialisable_event(monkeypatch):
    monkeypatch.setenv("CONTROL_EVENTS", "1")
    ce.emit({"severity": "ERROR", "bad": object()})


# --- vocabulary -------------------------------------------------------------

def test_source_must_be_in_the_shared_vocabulary():
    with pytest.raises(ValueError):
        ce.build(control_id="c", source="airflow", environment="dev")


def test_severity_must_be_in_the_shared_vocabulary():
    with pytest.raises(ValueError):
        ce.build(control_id="c", source="composer", environment="dev", severity="BAD")
