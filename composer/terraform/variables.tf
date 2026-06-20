variable "project_id" {
  type    = string
  default = "your-gcp-project-id"
}

variable "project_number" {
  type    = string
  default = "000000000000"
}

variable "region" {
  type    = string
  default = "us-central1" # keep off "US" to avoid Composer internal SQL trouble
}

variable "env" {
  type    = string
  default = "dev"
}

variable "subnet_cidr" {
  type    = string
  default = "10.228.128.0/24"
}

variable "image_version" {
  type    = string
  default = "composer-3-airflow-2.11.1-build.5"
}

variable "environment_size" {
  type    = string
  default = "ENVIRONMENT_SIZE_SMALL"
}

variable "scheduler" {
  type = object({ cpu = number, memory_gb = number, storage_gb = number, count = number })
  default = { cpu = 1, memory_gb = 4, storage_gb = 4, count = 1 }
}

variable "worker" {
  type = object({ cpu = number, memory_gb = number, storage_gb = number, min_count = number, max_count = number })
  # min_count=2 in prod for resilience; dev can drop to 1 via tfvars.
  default = { cpu = 1, memory_gb = 4, storage_gb = 4, min_count = 1, max_count = 4 }
}

variable "airflow_env_variables" {
  type        = map(string)
  description = "Airflow env vars surfaced to DAGs (ENV, ERROR_ANALYZER_URL, project ids, etc.)."
  default     = {}
}
