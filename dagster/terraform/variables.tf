variable "project_id" {
  type    = string
  default = "your-gcp-project-id"
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "zone" {
  type    = string
  default = "us-central1-c"
}

variable "env" {
  type    = string
  default = "dev"
}

variable "namespace" {
  type        = string
  description = "Kubernetes namespace the Dagster Helm release runs in."
  default     = "dagster"
}

variable "node_count" {
  type    = number
  default = 1
}

variable "node_machine_type" {
  type    = string
  default = "e2-standard-2"
}

variable "db_tier" {
  type    = string
  default = "db-g1-small" # bump to db-custom-* in prod
}

variable "db_password" {
  type      = string
  sensitive = true
}
