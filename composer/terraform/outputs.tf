output "composer_env_name" {
  value = google_composer_environment.this.name
}

output "dag_gcs_prefix" {
  description = "GCS path DAGs are synced to. CI rsyncs generated DAGs + configs here."
  value       = google_composer_environment.this.config[0].dag_gcs_prefix
}

output "airflow_uri" {
  value = google_composer_environment.this.config[0].airflow_uri
}

output "runtime_service_account" {
  description = "Add this as an invoker on the error-analyzer Cloud Run service."
  value       = google_service_account.composer.email
}
