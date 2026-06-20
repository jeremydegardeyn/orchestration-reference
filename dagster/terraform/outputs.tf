output "cluster_name" {
  value = google_container_cluster.dagster.name
}

output "runtime_service_account" {
  description = "Add this as an invoker on the error-analyzer; annotate the KSA with it for Workload Identity."
  value       = google_service_account.dagster.email
}

output "sql_connection_name" {
  description = "Cloud SQL connection name for the Dagster Postgres (use with the proxy/sidecar)."
  value       = google_sql_database_instance.dagster.connection_name
}

output "sql_host" {
  value = google_sql_database_instance.dagster.public_ip_address
}

output "artifact_registry" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.edp.repository_id}"
}
