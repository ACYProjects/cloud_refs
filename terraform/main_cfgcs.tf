terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.34.0"
    }
  }
}

resource "random_id" "bucket_prefix" {
  byte_length = 8
}

resource "google_storage_bucket" "bucket" {
  name                        = "${random_id.bucket_prefix.hex}-gcf-source"
  location                    = "US"
  uniform_bucket_level_access = true
}

resource "google_storage_bucket_object" "object" {
  name   = "function-source.zip"
  bucket = google_storage_bucket.bucket.name
  source = "function-source.zip" 
}

resource "google_cloudfunctions2_function" "function" {
  name        = "function-v2"
  location    = "us-central1"
  description = "a new function"

  build_config {
    runtime     = "python39" 
    entry_point = "gcs_create_file" 
    source {
      storage_source {
        bucket = google_storage_bucket.bucket.name
        object = google_storage_bucket_object.object.name
      }
    }
  }

  service_config {
    max_instance_count = 2
    available_memory   = "256M"
    timeout_seconds    = 540
  }
}

output "function_uri" {
  value = google_cloudfunctions2_function.function.service_config[0].uri
}
