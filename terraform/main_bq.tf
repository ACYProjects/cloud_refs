resource "google_bigquery_dataset" "dataset" {
  dataset_id = "my_dataset"
}

resource "google_bigquery_table" "table1" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "table1"

  schema = <<EOF
[
  {
    "name": "col1",
    "type": "STRING"
  },
  {
    "name": "col2",
    "type": "INTEGER"
  }
]
EOF
}

resource "google_bigquery_table" "table2" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "table2"

  schema = <<EOF
[
  {
    "name": "col1",
    "type": "FLOAT"
  },
  {
    "name": "col2",
    "type": "BOOLEAN"
  }
]
EOF
}

resource "google_bigquery_table" "table3" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "table3"

  schema = <<EOF
[
  {
    "name": "col1",
    "type": "TIMESTAMP"
  },
  {
    "name": "col2",
    "type": "DATE"
  }
]
EOF
}
