# Composer (Airflow) vs Dagster — decision guide

Both repos in this project implement the **same patterns** (`gcs_to_bigquery`, `dataform`), the **same
config-driven framework idea**, the **same admin pipelines**, and the **same error→LLM→alert** path. So
the choice is not about capability — it's about operating model, developer ergonomics, and cost.

## TL;DR

| If you want… | Choose |
|---|---|
| Fully managed, GCP-native, minimal platform ops, Airflow ecosystem | **Cloud Composer** |
| Data-asset/lineage-first model, strong local dev, typed configs, cheaper self-host | **Dagster** |
| Lowest possible cost at small scale | **Dagster on a VM/GKE** |
| "Nobody on the team should babysit the scheduler" | **Cloud Composer** |

## Side-by-side

| Dimension | Cloud Composer (Airflow) | Dagster |
|---|---|---|
| **Hosting** | Managed by Google (GKE + Cloud SQL + web server under the hood) | Self-hosted: GKE, VM, or Cloud Run; or Dagster+ (managed) |
| **Baseline cost (small)** | ~$150–350/mo floor, always on | ~$25/mo (1 VM) to ~$70/mo (small GKE); Dagster+ usage-based |
| **Core abstraction** | Tasks in a DAG (imperative dependencies) | Software-defined **assets** (declarative, lineage-native) + ops/jobs |
| **Config-driven pipelines** | Jinja-templated DAG files generated from YAML at build time | Asset/job **factories** that read YAML at load time (no codegen step) |
| **Scheduling** | Airflow scheduler + Dataset-driven triggers | Schedules + sensors + declarative automation/freshness |
| **Local dev** | Painful (needs Composer-like env); we emulate via the build script | First-class: `dagster dev` runs the full UI + daemon locally |
| **Backfills** | Manual, per-DAG, clunky | First-class partitioned backfills in the UI |
| **Typing / validation** | Configs validated by our builder; runtime is dynamic Python | Pydantic-typed configs + asset checks |
| **Upgrades** | Google manages image versions; you pin `image_version` | You own the image and Dagster version |
| **Ecosystem** | Huge Airflow provider catalog | Smaller but growing; good GCP/dbt/Dataform support |
| **GCP IAM integration** | Native (Composer service account, Workload Identity) | You wire Workload Identity yourself (Terraform here does it) |
| **Failure handling in this repo** | `on_failure_callback` → admin `agent_analysis` DAG → ADK agent | Failure **sensor/hook** → ADK agent |

## The framework difference that matters most

Both turn a YAML file into a pipeline, but the mechanism differs:

- **Composer** uses **build-time code generation**: `framework/dag_builder.py` renders a Jinja template per
  config into a concrete `*.py` DAG file that is synced to the Composer bucket. Pro: the deployed artifact is
  a plain Airflow DAG you can inspect. Con: there's a codegen step in CI, and generated files can drift.
- **Dagster** uses **load-time factories**: `framework/registry.py` reads the same YAML and constructs assets
  in-process when the code location loads. Pro: no codegen, configs are typed, one source of truth. Con: the
  "DAG" only exists once the code loads (less to eyeball as a static file).

If your team thinks in **tasks and operators**, Composer will feel natural. If they think in **datasets and
lineage**, Dagster will.

## Cost detail (small prod workload, us-central1, list prices)

- **Composer 3 small**: scheduler + web server + 1 worker + Cloud SQL run continuously. Realistically
  **$150–350/mo** even when idle. You cannot scale the floor to zero.
- **Dagster self-host**: dominated by whatever you leave running. One `e2-medium` VM with Postgres in a
  container is **~$25/mo**; a small GKE Standard node pool with the free zonal control plane is **~$30–70/mo**.
  The webserver + daemon are the only always-on pieces.
- **Dagster+ Serverless**: usage-based; free solo tier, Pro is per-minute compute + per-step. Closest
  apples-to-apples to "managed Composer" without running infra yourself.

## Reliability notes

- **Composer**: HA is Google's problem; you get managed backups of the metadata DB and a maintained GKE. The
  failure mode you own is bad DAGs and quota.
- **Dagster**: HA is *your* problem. The Terraform here provisions a Cloud SQL Postgres (not SQLite) and runs
  the webserver and daemon as separate GKE deployments so a webserver restart doesn't stop scheduling. A
  single-VM deploy is cheaper but a single point of failure — fine for dev, think twice for prod.

## Recommendation

- **Start on Composer** if the team is small, Airflow-literate, and "don't make me run a scheduler" is worth
  the ~$200/mo floor.
- **Choose Dagster** if you want lineage-first development, cheap self-hosting, strong local iteration, and
  you're comfortable owning the deployment (the Terraform here gets you most of the way).
- Either way, the **error-analyzer** and the **pattern contracts (the YAML schema)** are identical, so a
  migration later is mostly re-homing configs, not a rewrite.
