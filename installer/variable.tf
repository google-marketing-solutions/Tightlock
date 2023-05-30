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
