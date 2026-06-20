# Production Dagster on GKE.
# Provisions the *infrastructure* (GKE cluster, Cloud SQL Postgres for run storage, runtime identity
# with Workload Identity, Artifact Registry). Dagster itself is installed via the official Helm chart
# (see helm/values.yaml + RUNBOOK.md) — the standard split of cloud infra (Terraform) vs app (Helm).

locals {
  ksa_name = "dagster" # Kubernetes service account the Helm chart pods run as
}

# ---------------------------------------------------------------------------
# Artifact Registry for the user-code image
# ---------------------------------------------------------------------------
resource "google_artifact_registry_repository" "edp" {
  repository_id = "edp"
  location      = var.region
  format        = "DOCKER"
}

# ---------------------------------------------------------------------------
# GKE Standard cluster. The first zonal cluster's control plane is free per billing account.
# ---------------------------------------------------------------------------
resource "google_container_cluster" "dagster" {
  name     = "dagster-${var.env}"
  location = var.zone # zonal = free control plane

  remove_default_node_pool = true
  initial_node_count       = 1

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }
  release_channel { channel = "REGULAR" }
}

resource "google_container_node_pool" "primary" {
  name       = "primary"
  cluster    = google_container_cluster.dagster.id
  location   = var.zone
  node_count = var.node_count

  node_config {
    machine_type    = var.node_machine_type
    service_account = google_service_account.dagster_node.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    workload_metadata_config { mode = "GKE_METADATA" }
  }
  autoscaling {
    min_node_count = var.node_count
    max_node_count = var.node_count + 2
  }
}

# Node pool identity (separate from the workload identity).
resource "google_service_account" "dagster_node" {
  account_id   = "sa-dagster-node-${var.env}"
  display_name = "Dagster GKE nodes (${var.env})"
}

# ---------------------------------------------------------------------------
# Runtime identity (Workload Identity): what the Dagster pods act as on GCP.
# ---------------------------------------------------------------------------
resource "google_service_account" "dagster" {
  account_id   = "sa-dagster-${var.env}"
  display_name = "Dagster runtime (${var.env})"
}

locals {
  dagster_roles = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
    "roles/dataflow.developer",
    "roles/dataform.editor",
    "roles/run.invoker",                 # call error-analyzer
    "roles/cloudsql.client",             # connect to run-storage Postgres
    "roles/iam.serviceAccountUser",
  ]
}

resource "google_project_iam_member" "dagster_roles" {
  for_each = toset(local.dagster_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.dagster.email}"
}

# Bind the GSA to the in-cluster KSA (Workload Identity).
resource "google_service_account_iam_member" "wi" {
  service_account_id = google_service_account.dagster.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${var.namespace}/${local.ksa_name}]"
}

# ---------------------------------------------------------------------------
# Cloud SQL Postgres — Dagster run/event/schedule storage (NOT SQLite, so the
# webserver and daemon can both reach shared, durable state).
# ---------------------------------------------------------------------------
resource "google_sql_database_instance" "dagster" {
  name             = "dagster-${var.env}"
  database_version = "POSTGRES_16"
  region           = var.region

  settings {
    tier              = var.db_tier
    availability_type = var.env == "prod" ? "REGIONAL" : "ZONAL" # HA in prod
    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = var.env == "prod"
    }
    ip_configuration { ipv4_enabled = true }
  }
  deletion_protection = var.env == "prod"
}

resource "google_sql_database" "dagster" {
  name     = "dagster"
  instance = google_sql_database_instance.dagster.name
}

resource "google_sql_user" "dagster" {
  name     = "dagster"
  instance = google_sql_database_instance.dagster.name
  password = var.db_password
}
