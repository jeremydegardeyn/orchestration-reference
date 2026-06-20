# error-analyzer (shared)

An **ADK (Agent Development Kit) agent** that both orchestration platforms call when a pipeline fails.
It pulls the underlying GCP job logs (Dataflow or Dataform), produces a root-cause analysis with a
Gemini model on Vertex AI, and posts the result to the alert channel (Teams, email fallback).

Deployed **once** as a private Cloud Run service; Composer and Dagster both POST to `/analyze`.

## Agent topology

```
root_agent (router, reads `service` field)
├── dataflow_agent  → get_dataflow_job_messages(project, region, job_id)
├── dataform_agent  → get_dataform_failed_actions(project, region, repo, invocation_id)
└── alerting_agent  → send_alert(...)   # called exactly once
```

There is also a **deterministic fallback** (`analyze_and_alert`) that runs if the LLM call fails, so an
alert is *always* delivered even when Vertex is unavailable.

## Contract

`POST /analyze`
```json
{
  "service": "dataflow",
  "project_id": "your-gcp-project-id",
  "region": "us-central1",
  "pipeline_id": "gcs_to_bigquery__ga4_clickstream",
  "task_id": "start_dataflow_job",
  "job_id": "2026-06-19_12_00_00-123456"
}
```
For `service: "dataform"` supply `repository_id` and `workflow_invocation_id` instead of `job_id`.

## Local run

```powershell
cd C:\claude\orchestration\error-analyzer
py -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
copy env\dev.env.example env\dev.env   # fill in ALERT_TEAMS_CHANNEL
uvicorn error_analyzer.server:app --port 8080
```

## Deploy

```bash
# 1. Build + push the image to Artifact Registry
gcloud builds submit --tag us-central1-docker.pkg.dev/$PROJECT/edp/error-analyzer:$(git rev-parse --short HEAD)

# 2. Store the Teams webhook as a secret (one-time)
printf '%s' "$TEAMS_WEBHOOK_URL" | gcloud secrets create edp-alert-teams-webhook --data-file=-

# 3. Terraform
cd terraform
terraform init
terraform apply \
  -var="image=us-central1-docker.pkg.dev/$PROJECT/edp/error-analyzer:$TAG" \
  -var='invoker_members=["serviceAccount:sa-jsd-composer-dev@PROJECT.iam.gserviceaccount.com","serviceAccount:sa-dagster-dev@PROJECT.iam.gserviceaccount.com"]'
```

The `service_url` output is what you set as `ERROR_ANALYZER_URL` in each platform's env.

## Notes / modernization vs the reference

- Single notifier (`tools/notifier.py`) replaces the duplicated Teams/email senders that were scattered
  across the Composer `common_utils`. Both platforms now alert through the same code path and format.
- Private ingress + IAM invoker only — the agent is never publicly reachable.
- Model id is env-driven (`LLM_MODEL`), defaulting to `gemini-2.5-pro`; swap per environment.
