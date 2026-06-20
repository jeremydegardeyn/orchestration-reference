# orchestration

A reference implementation of the **same data-pipeline orchestration capability built two ways** —
once on **Cloud Composer (Airflow)** and once on **Dagster** — so the platform choice can be made
against working code instead of slideware.

Both platforms implement an identical, **config-driven framework**: you describe a pipeline in a small
YAML file, and the framework turns it into a runnable, scheduled pipeline. No bespoke DAG/asset code per
feed. The two patterns that ship today are the minimum any ingestion platform needs:

| Pattern | What it does |
|---|---|
| `gcs_to_bigquery` | Lands files from GCS into BigQuery by launching a **Dataflow Flex Template** (with a DTS fallback path) |
| `dataform` | Executes a **Dataform tag or action** (compilation result → workflow invocation) |

Both platforms also share:
- **Common utilities** used by every pattern (config loading, scheduling, GCP helpers, alerting).
- **Admin pipelines** for housekeeping and platform health.
- **Error → LLM → alert**: every failure is piped to a shared **ADK error-analyzer agent** that pulls the
  underlying job logs (Dataflow / Dataform), explains the failure, and posts the analysis to the alert
  channel (Teams/email).

```
orchestration/
├── README.md                  ← you are here
├── PLATFORM_TRADEOFFS.md      ← Composer vs Dagster decision guide
├── error-analyzer/            ← shared ADK agent, deployed once, called by both platforms
├── composer/                  ← Cloud Composer (Airflow) implementation + Terraform + runbook
├── dagster/                   ← Dagster implementation + Terraform + runbook
├── cicd/                      ← Workload Identity Federation bootstrap + CI/CD docs (replaces Harness)
└── .github/workflows/         ← GitHub Actions: PR validation + per-component deploys
```

## How the pieces fit

```
        ┌─────────────────┐     ┌─────────────────┐
        │ composer/  (TF)  │     │ dagster/   (TF)  │
        │  Airflow on GCP │     │  Dagster on GKE │
        └────────┬────────┘     └────────┬────────┘
                 │  YAML config → pattern │
                 │  gcs_to_bigquery       │
                 │  dataform              │
                 └───────────┬────────────┘
                             │ on failure
                             ▼
                 ┌───────────────────────┐
                 │  error-analyzer (ADK) │  ← shared, deployed once
                 │  pulls job logs, LLM  │
                 │  analysis → Teams      │
                 └───────────────────────┘
```

## Where to start

1. Read **[PLATFORM_TRADEOFFS.md](PLATFORM_TRADEOFFS.md)** to pick a platform.
2. Deploy the shared **[error-analyzer](error-analyzer/README.md)** (both platforms depend on it).
3. Follow the runbook for your platform: **[composer/RUNBOOK.md](composer/RUNBOOK.md)** or
   **[dagster/RUNBOOK.md](dagster/RUNBOOK.md)**.

## Status / provenance

This is a reference architecture distilled from a production LL Bean EDP-style Composer repo, a Composer 3
Terraform baseline, and an ADK error-analysis agent. It has been restructured and modernized — treat the
code as a strong starting skeleton, not a drop-in production deployment. Project/region defaults point at
`your-gcp-project-id` / `us-central1`; change them in the Terraform `*.tfvars` and the env files.
