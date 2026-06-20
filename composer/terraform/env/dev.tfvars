env              = "dev"
environment_size = "ENVIRONMENT_SIZE_SMALL"
worker           = { cpu = 1, memory_gb = 4, storage_gb = 4, min_count = 1, max_count = 3 }

airflow_env_variables = {
  ENV                          = "dev"
  ALERTS_ON_OFF                = "ON"
  ERROR_ANALYZER_URL           = "https://error-analyzer-dev-xxxx.a.run.app"
  INGESTION_PROJECT            = "your-gcp-project-id"
  INGESTION_REGION             = "us-central1"
  RAW_PROJECT                  = "your-gcp-project-id"
  WORK_PROJECT                 = "your-gcp-project-id"
  DATAFORM_PROJECT             = "your-gcp-project-id"
  DATAFORM_REGION              = "us-central1"
  DATAFORM_REPO                = "edp-dataform"
  DATAFORM_GIT_COMMITISH       = "main"
  DATAFLOW_BUCKET              = "jsd-dataflow-dev"
  DATAFLOW_REGION              = "us-central1"
  DATAFLOW_SUBNETWORK          = "regions/us-central1/subnetworks/composer-subnetwork-dev"
  AIRFLOW_POLL_INTERVAL_MINUTES = "15"
}
