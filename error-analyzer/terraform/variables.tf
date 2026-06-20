variable "project_id" {
  type    = string
  default = "your-gcp-project-id"
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "env" {
  type    = string
  default = "dev"
}

variable "image" {
  type        = string
  description = "Fully-qualified container image, e.g. us-central1-docker.pkg.dev/PROJECT/edp/error-analyzer:TAG"
}

variable "llm_model" {
  type    = string
  default = "gemini-2.5-pro"
}

variable "teams_webhook_secret_id" {
  type        = string
  description = "Secret Manager secret id holding the Teams incoming-webhook URL."
  default     = "edp-alert-teams-webhook"
}

variable "invoker_members" {
  type        = list(string)
  description = "IAM members (orchestrator service accounts) allowed to call the analyzer."
  default     = []
}
