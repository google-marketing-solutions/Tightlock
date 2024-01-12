# user-provided API key
variable "api_key" {
  type        = string
  description = "API Key for generating the front end connection key."
}

variable "access_key" {
  type        = string
  description = "AWS Access Key"
}

variable "secret_key" {
  type        = string
  description = "AWS Secret Key"
}

variable "create_tightlock_network" {
  type        = bool
  description = "If set to true, the default Tightlock network and firewall will be created. Set it to false if deploying on a cloud project that already has a Tightlock deployment."
}

variable "aws_availability_zone" {
  type        = string
  description = "The zone that the machine should be created in."
  default     = "us-east-2a"
}

variable "aws_region" {
  type        = string
  description = "The Region in which the created address should reside."
  default     = "us-east-2"
}
