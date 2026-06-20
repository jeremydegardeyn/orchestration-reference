# Deploys the error-analyzer as a private Cloud Run service that both orchestration
# platforms call on failure. Private ingress + IAM invoker only; no public access.

terraform {
  required_version = ">= 1.6"
  required_providers {
    google = { source = "hashicorp/google", version = "~> 6.0" }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Runtime identity for the agent. Needs to read Dataflow/Dataform + call Vertex.
resource "google_service_account" "analyzer" {
  account_id   = "sa-error-analyzer-${var.env}"
  display_name = "EDP error-analyzer (${var.env})"
}

locals {
  analyzer_roles = [
    "roles/dataflow.viewer",
    "roles/dataform.viewer",
    "roles/aiplatform.user",
    "roles/logging.viewer",
  ]
}

resource "google_project_iam_member" "analyzer_roles" {
  for_each = toset(local.analyzer_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.analyzer.email}"
}

resource "google_cloud_run_v2_service" "analyzer" {
  name     = "error-analyzer-${var.env}"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.analyzer.email
    scaling {
      min_instance_count = 0
      max_instance_count = 3
    }
    containers {
      image = var.image
      ports { container_port = 8080 }

      env {
        name  = "ENV"
        value = var.env
      }
      env {
        name  = "GOOGLE_CLOUD_PROJECT"
        value = var.project_id
      }
      env {
        name  = "GOOGLE_CLOUD_LOCATION"
        value = var.region
      }
      env {
        name  = "GOOGLE_GENAI_USE_VERTEXAI"
        value = "1"
      }
      env {
        name  = "LLM_MODEL"
        value = var.llm_model
      }
      env {
        name  = "ALERTS_ON_OFF"
        value = "ON"
      }
      # Teams webhook is a secret — sourced from Secret Manager.
      env {
        name = "ALERT_TEAMS_CHANNEL"
        value_source {
          secret_key_ref {
            secret  = var.teams_webhook_secret_id
            version = "latest"
          }
        }
      }
    }
  }
}

# Allow the orchestrator service accounts to invoke the analyzer.
resource "google_cloud_run_v2_service_iam_member" "invokers" {
  for_each = toset(var.invoker_members)
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.analyzer.name
  role     = "roles/run.invoker"
  member   = each.value
}
