variable "credentials" {
  description = "My credentials"
  default     = "/home/user/.keys/gcp-creds.json"
}

variable "project" {
  description = "Project"
  default     = "ny-rides-yadayaza"
}

variable "location" {
  description = "Project Location"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "ny_rides_yadayaza_dataset"
}

variable "gcs_bucket_name" {
  description = "My Goggle Cloud Bucket Name"
  default     = "ny_rides_yadayaza_terra_bucket"
}

variable "gcs_storage_class" {
  description = "Bucket storage class"
  default     = "STANDARD"
}