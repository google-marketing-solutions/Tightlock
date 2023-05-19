# user-provided API key
variable "apiKey" {
  type        = string
  description = "API Key for generating the front end connection key."
}

variable "project_id" {
  type        = string
  description = "Google Cloud Project ID"
}
