# Bootstrap: keyless GitHub Actions → GCP auth via Workload Identity Federation, plus the CI deploy
# service account. This is what replaces Harness's stored GCP credentials — no service-account keys
# anywhere; GitHub's OIDC token is exchanged for short-lived GCP credentials at runtime.
#
# Apply this ONCE per project. Outputs feed the GitHub Environment variables (see cicd/README.md).

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

resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github-actions"
  display_name              = "GitHub Actions"
  description               = "Keyless OIDC federation for GitHub Actions deploys"
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github"
  display_name                       = "GitHub OIDC"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
  }

  # Only tokens from YOUR repo can use this provider.
  attribute_condition = "assertion.repository == '${var.github_repo}'"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

# The identity GitHub Actions impersonates to deploy.
resource "google_service_account" "ci_deployer" {
  account_id   = "sa-ci-deployer"
  display_name = "GitHub Actions CI/CD deployer"
}

locals {
  # Least-privilege-ish set to deploy all three components.
  deployer_roles = [
    "roles/storage.admin",            # rsync generated DAGs to the Composer bucket
    "roles/composer.user",            # describe Composer env (resolve dag bucket)
    "roles/artifactregistry.writer",  # push images
    "roles/container.developer",      # deploy to GKE (Dagster)
    "roles/run.admin",                # deploy error-analyzer Cloud Run
    "roles/iam.serviceAccountUser",   # actAs runtime service accounts
    "roles/cloudbuild.builds.editor", # optional: gcloud builds submit
  ]
}

resource "google_project_iam_member" "deployer_roles" {
  for_each = toset(local.deployer_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.ci_deployer.email}"
}

# Allow the GitHub repo's OIDC identities to impersonate the deployer SA.
# Restricting to specific branches/environments can be done by narrowing the principalSet.
resource "google_service_account_iam_member" "wif_impersonation" {
  service_account_id = google_service_account.ci_deployer.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.github_repo}"
}
