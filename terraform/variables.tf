variable "project_id" {
  description = "The GCP project ID where resources will be provisioned."
  type        = string
  default     = "common-crawl-deduplication"
}

variable "region" {
  description = "The GCP region where resources will be provisioned."
  type        = string
  default     = "us-east1"
}

variable "prefix" {
  type    = string
  default = "ccdp" # common crawl deduplication pipeline
}

variable "credentials_file" {
  description = "Path to the JSON service account key file for authentication."
  type        = string
  default     = "keys/common-crawl-deduplication-XXXXXXXXXXXX.json"
}
