"""Run-failure sensor: backstop alerting for ANY failed run.

The patterns already call the error-analyzer with a precise job/invocation id at the point of failure
(rich LLM alert). This sensor is the safety net for failures that happen *outside* that path — infra
errors, OOM-killed steps, import failures — so no failed run goes unannounced. It sends the lightweight
Teams ping; it does not duplicate the LLM analysis (the pattern already did that when it could).
"""
from dagster import DagsterRunStatus, RunFailureSensorContext, run_failure_sensor

from orchestration_dagster.framework.common import alerting


@run_failure_sensor(
    name="pipeline_failure_sensor",
    description="Posts a lightweight alert for any failed run as a backstop to per-pattern analysis.",
    minimum_interval_seconds=30,
)
def pipeline_failure_sensor(context: RunFailureSensorContext):
    run = context.dagster_run
    error = context.failure_event.message or "run failed"
    alerting.notify_failure_lightweight(
        pipeline_id=run.job_name,
        step="(run-level)",
        run_id=run.run_id,
        message=error,
    )
