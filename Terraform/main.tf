terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  project = "yahoo-finance-455223"
  region = "us-central1"
  credentials = "yahoo-finance-455223-48f5e10a2e23.json"
}

resource "google_storage_bucket" "yfinance_data_lake" {
  name          = "yfinance-data-lake"
  location      = "us-central1"
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "yfinance_dataset" {
  dataset_id = "yfinance_data"
  project    = "yahoo-finance-455223"
  location   = "us-central1"
}