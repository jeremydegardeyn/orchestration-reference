output "service_url" {
  description = "Private URL of the error-analyzer Cloud Run service. Both platforms POST /analyze here."
  value       = google_cloud_run_v2_service.analyzer.uri
}

output "service_account_email" {
  value = google_service_account.analyzer.email
}
