# dagster

Config-driven data-pipeline orchestration on **Dagster**. Same patterns and the **same YAML config
schema** as the Composer repo — but built from the configs at **load time** (no codegen step), with
typed (Pydantic) configs and asset-native lineage.

## Layout

```
dagster/
├── terraform/                 # GKE + Cloud SQL (run storage) + Workload Identity + Artifact Registry
├── helm/values.yaml           # official Dagster chart values (Cloud SQL, user image, alert env)
├── Dockerfile                 # user-code image (hosts the code location via gRPC)
└── orchestration_dagster/
    ├── definitions.py         # entry point — assembles assets, jobs, schedules, sensor
    ├── framework/
    │   ├── config.py          # Pydantic models + YAML loader (validates at load time)
    │   ├── registry.py        # configs → assets + per-pipeline jobs + schedules (no codegen)
    │   ├── patterns/          # one factory per pattern
    │   │   ├── gcs_to_bigquery.py   # asset → launch Dataflow flex template
    │   │   └── dataform.py          # asset → Dataform tag/action invocation
    │   └── common/
    │       ├── gcp.py         # platform-agnostic launch/invoke helpers (shared logic)
    │       └── alerting.py    # error → analyzer service + lightweight Teams ping
    ├── configs/<pattern>/*.yaml     # ← add files here to create pipelines
    ├── admin/jobs.py          # retention, platform health, config audit (+ schedules)
    └── sensors/failure_sensor.py    # run-failure backstop alert
```

## How the framework works (vs Composer)

```
configs/dataform/country.yaml
        │  (code location loads)
        ▼
framework/registry.build_pipeline_defs()   →  asset "dataform__country"
                                               + job "dataform__country_job"
                                               + ScheduleDefinition (cron)
```

No generated files — the same YAML the Composer repo turns into a `.py` DAG, this repo turns into a
Dagster asset in-process. Configs are validated by Pydantic, so a malformed config fails the code
location load with a clear error instead of a silent DAG import failure.

## Add a pipeline

Drop a YAML in `configs/gcs_to_bigquery/` or `configs/dataform/` (copy an existing one) and reload the
code location. The asset, its job, and (if it has a cron) its schedule appear automatically.

## Local dev

```powershell
cd C:\claude\orchestration\dagster
py -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
$env:ERROR_ANALYZER_URL="https://error-analyzer-dev-xxxx.a.run.app"
dagster dev      # UI at http://localhost:3000
```

The Assets tab shows two groups (`gcs_to_bigquery`, `dataform`); Automation shows the per-pipeline and
admin schedules; the failure sensor is under Sensors.

## Error → LLM → alert

- Each pattern asset catches the precise Dataflow `job_id` / Dataform `workflow_invocation_id` at the
  point of failure and calls the shared **error-analyzer** with it (rich LLM root-cause alert).
- `sensors/failure_sensor.py` is a backstop: any failed run (including infra failures the pattern can't
  catch) gets a lightweight Teams ping so nothing fails silently.

See **[RUNBOOK.md](RUNBOOK.md)** to deploy to GKE.
