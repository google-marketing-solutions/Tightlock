# user-provided API key
variable "api_key" {
  type        = string
  description = "API Key for generating the front end connection key."
}

variable "project_id" {
  type        = string
  description = "Google Cloud Project ID"
}
