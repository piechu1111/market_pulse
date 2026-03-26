variable "aws_region" {
  type    = string
  default = "eu-central-1"
}

variable "project_name" {
  type    = string
  default = "market-pulse"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "artifact_version" {
  description = "Version of lambda artifacts stored in S3"
  type        = string
}