# Dagster deploy runbook (GKE)

Project `your-gcp-project-id`, region `us-central1`, zone `us-central1-c`. The split: **Terraform**
provisions cloud infra (GKE, Cloud SQL, identity, Artifact Registry); the **Dagster Helm chart** installs
Dagster itself.

## Prerequisites

- `gcloud`, `kubectl`, `helm` installed; `gcloud` authed.
- The shared **error-analyzer** deployed (need its URL). See `../error-analyzer/README.md`.
- Dataflow Flex Template staged at `gs://<DATAFLOW_BUCKET>/templates/gcs_to_bigquery.json`.
- A Dataform repository connected to Git.

## 1. Enable APIs (one-time)

```bash
gcloud services enable container.googleapis.com sqladmin.googleapis.com \
  artifactregistry.googleapis.com dataflow.googleapis.com dataform.googleapis.com \
  run.googleapis.com --project your-gcp-project-id
```

## 2. Provision infra

```bash
cd terraform
terraform init
terraform apply -var="db_password=$(openssl rand -base64 24)" -var="env=dev"
```
Capture outputs:
```bash
terraform output runtime_service_account   # add as analyzer invoker; annotate the KSA
terraform output sql_connection_name        # for the Cloud SQL proxy sidecar
terraform output artifact_registry          # image push target
```

## 3. Build + push the user-code image

```bash
AR=$(terraform -chdir=terraform output -raw artifact_registry)
gcloud builds submit --tag $AR/dagster-orchestration:$(git rev-parse --short HEAD)
```

## 4. Cluster credentials + namespace + secrets

```bash
gcloud container clusters get-credentials dagster-dev --zone us-central1-c
kubectl create namespace dagster

# Cloud SQL password secret (key name the chart expects)
kubectl -n dagster create secret generic dagster-postgresql-secret \
  --from-literal=postgresql-password="$DB_PASSWORD"

# Pipeline runtime env (consumed by the user-code deployment)
kubectl -n dagster create configmap dagster-pipeline-env \
  --from-literal=ENV=dev \
  --from-literal=ERROR_ANALYZER_URL="$ANALYZER_URL" \
  --from-literal=ALERT_TEAMS_CHANNEL="$TEAMS_WEBHOOK" \
  --from-literal=WORK_PROJECT=your-gcp-project-id \
  --from-literal=RAW_PROJECT=your-gcp-project-id \
  --from-literal=DATAFLOW_BUCKET=jsd-dataflow-dev \
  --from-literal=DATAFLOW_REGION=us-central1 \
  --from-literal=DATAFLOW_SUBNETWORK=regions/us-central1/subnetworks/default \
  --from-literal=INGESTION_SA=sa-dagster-dev@your-gcp-project-id.iam.gserviceaccount.com \
  --from-literal=DATAFORM_PROJECT=your-gcp-project-id \
  --from-literal=DATAFORM_REGION=us-central1 \
  --from-literal=DATAFORM_REPO=edp-dataform \
  --from-literal=DATAFORM_GIT_COMMITISH=main
```

## 5. Install Dagster (Helm)

Edit `helm/values.yaml`: set the image repo/tag, the `sql_host` (or use a Cloud SQL proxy sidecar), and
the KSA annotation to the `runtime_service_account` output. Then:
```bash
helm repo add dagster https://dagster-io.github.io/helm
helm repo update
helm upgrade --install dagster dagster/dagster -n dagster -f helm/values.yaml
```

## 6. Grant analyzer invoker + verify

```bash
gcloud run services add-iam-policy-binding error-analyzer-dev --region us-central1 \
  --member "serviceAccount:$(terraform -chdir=terraform output -raw runtime_service_account)" \
  --role roles/run.invoker

# Reach the UI (OSS has no auth — port-forward, don't expose publicly)
kubectl -n dagster port-forward svc/dagster-dagster-webserver 3000:80
```
Open http://localhost:3000 → Assets → materialize `dataform__country`. Force a failure (bad config) and
confirm the analyzer posts a root-cause alert and the failure sensor pings.

## Resilience notes

- Cloud SQL is `REGIONAL` (HA) in prod via the `env` switch; the webserver and daemon are separate
  deployments so a webserver restart never stops scheduling.
- The daemon is the critical always-on pod (schedules/sensors/run queue). Watch it: `kubectl -n dagster
  get pods -l component=dagster-daemon`.

## Common issues

- **Pods can't reach BigQuery/Dataflow**: Workload Identity not wired — check the KSA annotation matches
  `runtime_service_account` and `workloadIdentityUser` binding exists (Terraform creates it).
- **Run storage errors / runs not persisting**: `sql_host` wrong in values.yaml or the password secret
  missing; prefer a Cloud SQL proxy sidecar over a public IP.
- **Analyzer 403**: runtime SA not an invoker, or ID-token audience ≠ service URL.
- **Rollback**: `helm rollback dagster` for the platform; redeploy the previous image tag for code.
