# Composer deploy runbook

Project `your-gcp-project-id`, region `us-central1`. Adjust in `terraform/env/*.tfvars`.

## Prerequisites

- `gcloud` authed with rights to create Composer, networks, service accounts, secrets.
- The shared **error-analyzer** deployed (you need its URL). See `../error-analyzer/README.md`.
- A Dataflow Flex Template built and staged at `gs://<DATAFLOW_BUCKET>/templates/gcs_to_bigquery.json`.
- A Dataform repository (`DATAFORM_REPO`) connected to your Git remote.

## 1. Enable APIs (one-time)

```bash
gcloud services enable composer.googleapis.com dataflow.googleapis.com \
  dataform.googleapis.com secretmanager.googleapis.com run.googleapis.com \
  --project your-gcp-project-id
```

## 2. Provision Composer

```bash
cd terraform
terraform init
terraform apply -var-file=env/dev.tfvars      # creates network, SA, secret, Composer 3 env (~25 min)
```

Capture outputs:
```bash
terraform output dag_gcs_prefix          # -> gs://.../dags  (CI sync target)
terraform output runtime_service_account # -> add as invoker on the error-analyzer
terraform output airflow_uri             # -> the Airflow UI
```

## 3. Wire the secret + analyzer

```bash
# Put the Teams webhook into the secret Terraform created.
printf '%s' "$TEAMS_WEBHOOK_URL" | gcloud secrets versions add edp-alert-teams-webhook --data-file=-

# Grant the Composer runtime SA invoker on the analyzer (or pass it to the analyzer's TF var).
gcloud run services add-iam-policy-binding error-analyzer-dev \
  --region us-central1 --member "serviceAccount:$(terraform output -raw runtime_service_account)" \
  --role roles/run.invoker
```

Set `ERROR_ANALYZER_URL` in `terraform/env/dev.tfvars` (airflow_env_variables) to the analyzer URL and
re-apply, or set it as an Airflow variable.

## 4. Deploy DAGs

Normally CI does this: push to `main` (or *Run workflow* for qa/prod) triggers
`.github/workflows/composer-deploy.yml`, which builds env-specific DAGs and rsyncs them. See
`../cicd/README.md` for the one-time WIF + GitHub Environment setup.

To deploy manually (e.g. first bootstrap, before CI is wired):
```bash
set -a && source env/dev.env && set +a
pip install jinja2 pyyaml && python -m framework.dag_builder
gcloud storage rsync --recursive dags $(terraform -chdir=terraform output -raw dag_gcs_prefix)
```

## 5. Verify

- Open `airflow_uri`. Confirm `gcs_to_bigquery__*`, `dataform__*`, and `admin__*` DAGs imported with no
  errors (check the "DAG Import Errors" banner).
- Unpause one pattern DAG and trigger a manual run.
- Force a failure (e.g. point a config at a missing bucket) and confirm: lightweight Teams ping fires,
  `admin__agent_analysis` runs, and the analyzer posts a root-cause alert.

## Rollback / common issues

- **DAG import errors**: almost always a bad config or a template var with no env value. Fix the config /
  `airflow_env_variables` and re-sync.
- **Dataflow launch 403**: the runtime SA lacks `dataflow.developer` or `actAs` on the worker SA.
- **Analyzer 403**: runtime SA isn't an invoker, or the ID-token audience doesn't match the service URL.
- **Composer env stuck**: check the `ServiceAgentV2Ext` binding on the Composer service agent.
- **Roll back DAGs**: re-sync the previous commit's generated `dags/` (rsync is the deploy unit).
