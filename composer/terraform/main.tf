# Production Cloud Composer 3 environment.
# Hardened from the composer_v3 baseline: private environment, least-privilege runtime SA,
# env-sized workloads, resilient worker autoscaling, and a Teams-webhook secret.

locals {
  composer_sa_name = "sa-jsd-composer-${var.env}"

  # Least privilege: the DAG runtime identity. Add dataflow/dataform/run.invoker so the
  # patterns can launch jobs and call the shared error-analyzer.
  composer_roles = [
    "roles/composer.worker",
    "roles/iam.serviceAccountUser",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
    "roles/dataflow.developer",
    "roles/dataform.editor",
    "roles/run.invoker",            # call error-analyzer
    "roles/secretmanager.secretAccessor",
  ]
}

# ---------------------------------------------------------------------------
# Network (private)
# ---------------------------------------------------------------------------
resource "google_compute_network" "composer" {
  name                    = "composer-network-${var.env}"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "composer" {
  name          = "composer-subnetwork-${var.env}"
  ip_cidr_range = var.subnet_cidr
  region        = var.region
  network       = google_compute_network.composer.id

  private_ip_google_access = true
}

# ---------------------------------------------------------------------------
# Runtime service account
# ---------------------------------------------------------------------------
resource "google_service_account" "composer" {
  account_id   = local.composer_sa_name
  display_name = "Composer DAG runtime (${var.env})"
}

resource "google_project_iam_member" "composer_roles" {
  for_each = toset(local.composer_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.composer.email}"
}

# Composer service agent extension (required for Composer 2/3 to manage the env).
resource "google_project_iam_member" "composer_agent_ext" {
  project = var.project_id
  role    = "roles/composer.ServiceAgentV2Ext"
  member  = "serviceAccount:service-${var.project_number}@cloudcomposer-accounts.iam.gserviceaccount.com"
}

# ---------------------------------------------------------------------------
# Secret: Teams webhook (consumed by DAG alerting + the analyzer)
# ---------------------------------------------------------------------------
resource "google_secret_manager_secret" "teams_webhook" {
  secret_id = "edp-alert-teams-webhook"
  replication { auto {} }
}

# ---------------------------------------------------------------------------
# Composer 3 environment
# ---------------------------------------------------------------------------
resource "google_composer_environment" "this" {
  provider = google-beta
  name     = "jsd-composer-${var.env}"
  region   = var.region

  config {
    enable_private_environment = true
    environment_size           = var.environment_size

    software_config {
      image_version = var.image_version
      pypi_packages = {
        # Patterns + alerting need these at runtime.
        "google-cloud-dataform"        = ""
        "google-cloud-dataflow-client" = ""
        "croniter"                     = ""
      }
      env_variables = var.airflow_env_variables
    }

    workloads_config {
      scheduler {
        cpu        = var.scheduler.cpu
        memory_gb  = var.scheduler.memory_gb
        storage_gb = var.scheduler.storage_gb
        count      = var.scheduler.count
      }
      web_server {
        cpu        = 1
        memory_gb  = 4
        storage_gb = 4
      }
      worker {
        cpu        = var.worker.cpu
        memory_gb  = var.worker.memory_gb
        storage_gb = var.worker.storage_gb
        min_count  = var.worker.min_count
        max_count  = var.worker.max_count
      }
      triggerer {
        cpu       = 1
        memory_gb = 2
        count     = 1
      }
      dag_processor {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 2
        count      = 1
      }
    }

    maintenance_window {
      start_time = "1900-01-01T01:00:00Z"
      end_time   = "1900-01-01T05:00:00Z"
      recurrence = "FREQ=DAILY"
    }

    node_config {
      network         = google_compute_network.composer.id
      subnetwork      = google_compute_subnetwork.composer.id
      service_account = google_service_account.composer.email
      tags            = ["composer", var.env]
    }
  }

  depends_on = [google_project_iam_member.composer_roles]
}
