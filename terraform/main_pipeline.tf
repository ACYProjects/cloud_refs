provider "google" {
  project = "project_id"
  region  = "us-central1"
  zone    = "us-central1-a"
}

resource "google_storage_bucket" "gcs_bucket" {
  name     = "bucket_name"
  location = "US"
}

resource "google_storage_bucket" "temp_bucket" {
  name     = "bucket_tmp_name"
  location = "US"
}

resource "google_bigquery_dataset" "bq_dataset" {
  dataset_id = "dataset+id"
}

resource "google_bigquery_table" "bq_table" {
  dataset_id = google_bigquery_dataset.bq_dataset.dataset_id
  table_id   = "table_id"

  schema = <<EOF
[
  {
    "name": "column1",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "column2",
    "type": "INTEGER",
    "mode": "NULLABLE"
  }
]
EOF
}

resource "google_dataflow_job" "dataflow_job" {
  name        = "<YOUR_DATAFLOW_JOB_NAME>"
  region      = "us-central1"
  template_gcs_path = "gs://${google_storage_bucket.gcs_bucket.name}/path/to/your/custom_template"
  temp_gcs_location = "gs://${google_storage_bucket.temp_bucket.name}/temp"

  parameters = {
    inputFile = "gs://path/to/your/parquet/file.parquet"
    outputTable = "${google_bigquery_dataset.bq_dataset.dataset_id}.${google_bigquery_table.bq_table.table_id}"
    tempLocation = "gs://${google_storage_bucket.temp_bucket.name}/temp"
  }

  on_delete = "cancel"
}
