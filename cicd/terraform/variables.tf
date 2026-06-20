variable "project_id" {
  type    = string
  default = "your-gcp-project-id"
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "github_repo" {
  type        = string
  description = "owner/repo that is allowed to deploy, e.g. jeremydegardeyn/orchestration"
}
