env              = "prod"
environment_size = "ENVIRONMENT_SIZE_MEDIUM"
# Resilience: 2 schedulers + min 2 workers so a single pod restart doesn't pause scheduling.
scheduler        = { cpu = 2, memory_gb = 8, storage_gb = 4, count = 2 }
worker           = { cpu = 2, memory_gb = 8, storage_gb = 8, min_count = 2, max_count = 8 }

airflow_env_variables = {
  ENV                           = "prod"
  ALERTS_ON_OFF                 = "ON"
  ERROR_ANALYZER_URL            = "https://error-analyzer-prod-xxxx.a.run.app"
  INGESTION_PROJECT             = "your-gcp-project-id"
  INGESTION_REGION              = "us-central1"
  RAW_PROJECT                   = "your-gcp-project-id"
  WORK_PROJECT                  = "your-gcp-project-id"
  DATAFORM_PROJECT              = "your-gcp-project-id"
  DATAFORM_REGION               = "us-central1"
  DATAFORM_REPO                 = "edp-dataform"
  DATAFORM_GIT_COMMITISH        = "main"
  DATAFLOW_BUCKET               = "jsd-dataflow-prod"
  DATAFLOW_REGION               = "us-central1"
  DATAFLOW_SUBNETWORK           = "regions/us-central1/subnetworks/composer-subnetwork-prod"
  AIRFLOW_POLL_INTERVAL_MINUTES = "15"
}
