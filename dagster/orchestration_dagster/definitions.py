"""Single entry point Dagster loads (see [tool.dagster] in pyproject.toml).

Assembles:
  - pipeline assets + jobs + schedules built from YAML configs (framework/registry.py)
  - admin jobs + schedules (admin/jobs.py)
  - the run-failure backstop sensor (sensors/failure_sensor.py)
"""
from dagster import Definitions

from orchestration_dagster.framework.registry import build_pipeline_defs
from orchestration_dagster.admin.jobs import (
    run_retention_job,
    platform_health_job,
    config_audit_job,
    admin_schedules,
)
from orchestration_dagster.sensors.failure_sensor import pipeline_failure_sensor

pipeline_assets, pipeline_jobs, pipeline_schedules = build_pipeline_defs()

defs = Definitions(
    assets=pipeline_assets,
    jobs=[*pipeline_jobs, run_retention_job, platform_health_job, config_audit_job],
    schedules=[*pipeline_schedules, *admin_schedules],
    sensors=[pipeline_failure_sensor],
)
