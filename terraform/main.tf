locals {
  raw_bucket = "${var.prefix}-raw-${var.project_id}"
}

resource "google_storage_bucket" "raw" {
  name                        = local.raw_bucket
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true # delete bucket with objects in it (use with caution in production environments)

  lifecycle_rule {
    action { type = "Delete" }
    condition { age = 30 } # Auto-delete data after 7 days to save money
  }
}
