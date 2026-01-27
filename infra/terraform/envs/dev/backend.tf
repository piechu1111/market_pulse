terraform {
  backend "s3" {
    bucket  = "market-pulse-terraform-state-eu-central-1"
    key     = "dev/terraform.tfstate"
    region  = "eu-central-1"
    encrypt = true
  }
}