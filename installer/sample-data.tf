resource "random_id" "bucket_suffix" {
  byte_length = 8
}

resource "google_storage_bucket" "sample_data_upload_bucket" {
  count                       = var.create_bq_sample_data ? 1 : 0
  project                     = var.project_id
  name                        = "tightlock-sample-data-${random_id.bucket_suffix.hex}"
  location                    = "US"
  uniform_bucket_level_access = true
  force_destroy               = true
  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket_object" "ga4_app_sample_data_upload" {
  count  = var.create_bq_sample_data ? 1 : 0
  name   = "ga4_app_sample_data.csvh"
  bucket = google_storage_bucket.sample_data_upload_bucket[0].name
  source = "../sample_data/ga4_app_sample_data.csvh"
}

resource "google_storage_bucket_object" "ga4_web_sample_data_upload" {
  count  = var.create_bq_sample_data ? 1 : 0
  name   = "ga4_web_sample_data.csvh"
  bucket = google_storage_bucket.sample_data_upload_bucket[0].name
  source = "../sample_data/ga4_web_sample_data.csvh"
}

resource "google_bigquery_dataset" "sample_data" {
  count                       = var.create_bq_sample_data ? 1 : 0
  project                     = var.project_id
  dataset_id                  = "tightlock_sample_data"
  friendly_name               = "Tightlock Sample Data"
  description                 = "Sample Data to test Tightlock"
  location                    = "US"
  default_table_expiration_ms = 5184000000
  delete_contents_on_destroy  = true
}

resource "google_bigquery_table" "ga4_app_sample_data_table" {
  count               = var.create_bq_sample_data ? 1 : 0
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.sample_data[0].dataset_id
  table_id            = "ga4_app_sample_data"
  deletion_protection = false
  external_data_configuration {
    autodetect    = true
    source_format = "CSV"
    source_uris = [
      format("%s/%s", google_storage_bucket.sample_data_upload_bucket[0].url, google_storage_bucket_object.ga4_app_sample_data_upload[0].output_name)
    ]
  }
}

resource "google_bigquery_table" "ga4_web_sample_data_table" {
  count               = var.create_bq_sample_data ? 1 : 0
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.sample_data[0].dataset_id
  table_id            = "ga4_web_sample_data"
  deletion_protection = false
  external_data_configuration {
    autodetect    = true
    source_format = "CSV"
    source_uris = [
      format("%s/%s", google_storage_bucket.sample_data_upload_bucket[0].url, google_storage_bucket_object.ga4_web_sample_data_upload[0].output_name)
    ]
  }
}
