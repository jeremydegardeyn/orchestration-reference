"""Admin jobs: platform housekeeping and health. Dagster analogue of the Composer admin DAGs.

  - run_retention_job   : prune run history older than the retention window (keeps the DB lean)
  - platform_health_job : summarize recent failed runs and post a digest
  - config_audit_job    : report how many pipelines are registered per pattern (sanity check)
"""
from datetime import datetime, timedelta, timezone

from dagster import (
    DagsterRunStatus,
    RunsFilter,
    ScheduleDefinition,
    job,
    op,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

from orchestration_dagster.framework.common import alerting
from orchestration_dagster.framework.config import load_configs
from orchestration_dagster.framework.registry import PATTERN_BUILDERS

RETENTION_DAYS = 15
HEALTH_LOOKBACK_HOURS = 6


@op
def prune_old_runs(context) -> int:
    """Delete run records older than the retention window."""
    cutoff = _utcnow() - timedelta(days=RETENTION_DAYS)
    instance = context.instance
    old = instance.get_runs(filters=RunsFilter(created_before=cutoff))
    count = 0
    for run in old:
        instance.delete_run(run.run_id)
        count += 1
    context.log.info("Pruned %d runs older than %d days", count, RETENTION_DAYS)
    return count


@op
def report_recent_failures(context) -> int:
    since = _utcnow() - timedelta(hours=HEALTH_LOOKBACK_HOURS)
    failed = context.instance.get_runs(
        filters=RunsFilter(statuses=[DagsterRunStatus.FAILURE], created_after=since)
    )
    if not failed:
        context.log.info("No failed runs in window.")
        return 0
    lines = "".join(f"<li>{r.job_name} — {r.run_id[:8]}</li>" for r in failed)
    alerting.notify_failure_lightweight(
        pipeline_id="admin.platform_health",
        step="report_recent_failures",
        run_id=f"last_{HEALTH_LOOKBACK_HOURS}h",
        message=f"{len(failed)} failed run(s):<ul>{lines}</ul>",
    )
    return len(failed)


@op
def audit_configs(context) -> dict:
    counts = {p: len(load_configs(p)) for p in PATTERN_BUILDERS}
    context.log.info("Registered pipelines per pattern: %s", counts)
    return counts


@job(tags={"pattern": "admin"})
def run_retention_job():
    prune_old_runs()


@job(tags={"pattern": "admin"})
def platform_health_job():
    report_recent_failures()


@job(tags={"pattern": "admin"})
def config_audit_job():
    audit_configs()


admin_schedules = [
    ScheduleDefinition(name="run_retention_schedule", job=run_retention_job, cron_schedule="0 2 * * *"),
    ScheduleDefinition(name="platform_health_schedule", job=platform_health_job, cron_schedule="0 */6 * * *"),
    ScheduleDefinition(name="config_audit_schedule", job=config_audit_job, cron_schedule="0 7 * * *"),
]
