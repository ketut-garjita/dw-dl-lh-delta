# GCP Terraform skeleton - replace variables before apply
provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "lake" {
  name     = "${var.project_id}-lake-bucket"
  location = var.region
  force_destroy = true
}
