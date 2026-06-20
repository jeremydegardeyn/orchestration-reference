# These two values go into each GitHub Environment as variables (WIF_PROVIDER, DEPLOY_SA).

output "wif_provider" {
  description = "Set as GitHub Environment variable WIF_PROVIDER (workload_identity_provider input)."
  value       = google_iam_workload_identity_pool_provider.github.name
}

output "deploy_service_account" {
  description = "Set as GitHub Environment variable DEPLOY_SA (service_account input)."
  value       = google_service_account.ci_deployer.email
}
