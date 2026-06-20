"""dataform pattern utilities: build the compilation + workflow-invocation requests for a tag or action."""
import logging

from airflow.decorators import task

from dags.utils import common_utils

logger = logging.getLogger(__name__)


def build_compilation_request(git_commitish: str, compilation_vars: dict, tags: list | None) -> dict:
    sanitized = {str(k): str(v) for k, v in (compilation_vars or {}).items() if v is not None}
    if tags:
        sanitized["execution_tags"] = ",".join(map(str, tags))
    return {
        "git_commitish": git_commitish,
        "code_compilation_config": {
            "default_database": common_utils.env_override("default", "DATAFORM_PROJECT"),
            "vars": sanitized,
        },
    }


def build_workflow_request(execution_type: str, names: list, include_deps: bool, include_dependents: bool) -> dict:
    compilation_result = "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}"
    key = "included_tags" if execution_type == "tag" else "included_targets"
    targets = [n["name"] for n in names] if execution_type == "tag" else names
    return {
        "compilation_result": compilation_result,
        "invocation_config": {
            key: targets,
            "transitive_dependencies_included": include_deps,
            "transitive_dependents_included": include_dependents,
            "fully_refresh_incremental_tables_enabled": False,
        },
    }


@task()
def build_execution_string(git_commitish: str, compilation_vars: dict, **kwargs):
    config = kwargs["ti"].xcom_pull(task_ids="read_config", key="config")
    schedules = config.get("schedules", [config])

    for schedule in schedules:
        execs = schedule.get("executions", {}) or {}
        execution_type = "tag" if "tag" in execs else "object" if "object" in execs else None
        if not execution_type:
            continue

        entities = execs[execution_type]
        include_deps = common_utils.str_to_bool(entities.get("include_dependencies", False))
        include_dependents = common_utils.str_to_bool(entities.get("include_dependents", False))
        do_full_refresh = common_utils.str_to_bool(entities.get("do_full_refresh", False))

        vars_with_refresh = dict(compilation_vars)
        vars_with_refresh["do_full_refresh"] = str(do_full_refresh).lower()

        tags = [n["name"] for n in entities.get("names", [])] if execution_type == "tag" else None
        compilation_request = build_compilation_request(git_commitish, vars_with_refresh, tags)
        workflow_request = build_workflow_request(
            execution_type, entities["names"], include_deps, include_dependents
        )
        kwargs["ti"].xcom_push("compilation_request", compilation_request)
        kwargs["ti"].xcom_push("workflow_request", workflow_request)
        return  # one execution block per schedule
