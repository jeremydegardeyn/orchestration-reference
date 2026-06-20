"""Load-time factory: turn every YAML config into assets + per-pipeline jobs + schedules.

This is the Dagster analogue of the Composer dag_builder.py — except there's no codegen step. The
configs are read and the assets constructed in-process when the code location loads.
"""
from dagster import (
    AssetSelection,
    AssetsDefinition,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)

from orchestration_dagster.framework.config import PipelineConfig, load_configs
from orchestration_dagster.framework.patterns import gcs_to_bigquery, dataform

PATTERN_BUILDERS = {
    "gcs_to_bigquery": gcs_to_bigquery.build,
    "dataform": dataform.build,
}


def _build_one(config: PipelineConfig) -> AssetsDefinition:
    builder = PATTERN_BUILDERS[config.pattern]
    return builder(config)


def build_pipeline_defs() -> tuple[list[AssetsDefinition], list, list[ScheduleDefinition]]:
    assets: list[AssetsDefinition] = []
    jobs = []
    schedules: list[ScheduleDefinition] = []

    for pattern in PATTERN_BUILDERS:
        for config in load_configs(pattern):
            asset_def = _build_one(config)
            assets.append(asset_def)

            # One job per pipeline so it can be scheduled / triggered independently.
            job = define_asset_job(
                name=f"{config.pipeline_id}_job",
                selection=AssetSelection.assets(asset_def),
                tags={"pattern": config.pattern},
            )
            jobs.append(job)

            if config.cron:
                schedules.append(
                    ScheduleDefinition(
                        name=f"{config.pipeline_id}_schedule",
                        job=job,
                        cron_schedule=config.cron,
                        execution_timezone=config.schedules[0].timezone,
                    )
                )

    return assets, jobs, schedules
