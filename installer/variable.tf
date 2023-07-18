# user-provided API key
variable "api_key" {
  type        = string
  description = "API Key for generating the front end connection key."
}

variable "project_id" {
  type        = string
  description = "Google Cloud Project ID"
}

variable "create_bq_sample_data" {
  type        = bool
  description = "If set to true a BigQuery dataset and tables will be created with sample data uploaded via a Cloud Bucket."
}

variable "create_tightlock_network" {
  type        = bool
  description = "If set to true, the default Tightlock network and firewall will be created. Set it to false if deploying on a cloud project that already has a Tightlock deployment."
}

variable "compute_engine_zone" {
  type        = string
  description = "The zone that the machine should be created in."
  default     = "us-central1-a"
}

variable "compute_address_region" {
  type        = string
  description = "The Region in which the created address should reside."
  default     = "us-central1"
}
