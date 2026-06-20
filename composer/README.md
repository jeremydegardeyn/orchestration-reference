# composer (Cloud Composer / Airflow)

Config-driven data-pipeline orchestration on **Cloud Composer 3**. A pipeline is a **YAML file**; the
framework generates the Airflow DAG from it. Two patterns ship: `gcs_to_bigquery` (Dataflow Flex
Template) and `dataform` (tag/action invocation). Failures are piped to the shared **error-analyzer**.

## Layout

```
composer/
├── terraform/                 # Composer 3 env, network, runtime SA, secret (dev/prod tfvars)
├── framework/                 # the build-time DAG generator
│   ├── dag_builder.py         #   YAML config → rendered DAG .py
│   ├── templates/             #   one Jinja template per pattern
│   │   ├── gcs_to_bigquery.py
│   │   └── dataform.py
│   └── utils/jinja_utils.py
├── dags/
│   ├── configs/<pattern>/*.yaml   # ← you add files here to create pipelines
│   ├── utils/                     # runtime utils shared by all patterns
│   │   ├── common_utils.py        #   config load, schedule, terminator, failure callback
│   │   ├── gcs_to_bigquery_utils.py
│   │   ├── dataform_utils.py
│   │   └── alerting.py            #   error → LLM service + lightweight Teams ping
│   └── admin/                     # admin DAGs (agent_analysis, polling, cleanup, health)
├── env/<env>.env             # build-time env baked into generated DAGs (sourced by CI)
└── requirements.txt

CI/CD lives in GitHub Actions at ../.github/workflows/composer-deploy.yml (see ../cicd/README.md).
```

## How the framework works

```
dags/configs/dataform/country.yaml
        │  (CI runs framework/dag_builder.py)
        ▼
framework/templates/dataform.py  ──render──►  dags/dataform__country.py
        │  (CI rsyncs dags/ to the Composer bucket)
        ▼
Airflow picks up dataform__country  ──►  compiles repo ──► workflow invocation
```

Because generation happens in CI, the **deployed artifact is a plain Airflow DAG** you can open and read
— at the cost of a codegen step. (Contrast with the Dagster repo, which builds pipelines from the same
YAML at load time with no codegen.)

## Add a pipeline

1. Drop a YAML in `dags/configs/gcs_to_bigquery/` or `dags/configs/dataform/` (copy an existing one).
2. Commit. CI runs `dag_builder.py` and rsyncs. The new DAG appears in Airflow (paused on creation).

## Local DAG generation (smoke test)

```powershell
cd C:\claude\orchestration\composer
py -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install jinja2 pyyaml
python framework/dag_builder.py
# inspect generated dags/gcs_to_bigquery__*.py and dags/dataform__*.py
```

## Error → LLM → alert

- Every pattern DAG sets `on_failure_callback=common_utils.failure_notification`, which fires an
  immediate lightweight Teams ping (so humans know fast even if the LLM is down).
- On core-job failure, the DAG also triggers `admin__agent_analysis`, which calls the shared
  **error-analyzer** Cloud Run service. That service pulls the Dataflow/Dataform logs, analyzes them with
  Gemini, and posts the rich root-cause alert.

See **[RUNBOOK.md](RUNBOOK.md)** to deploy.
