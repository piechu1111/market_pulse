locals {
  common_tags = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
  }

  lambda_common_env = {
    ENV = var.environment

    ALPHA_API_URL = "https://www.alphavantage.co/query"
    LOG_LEVEL     = "INFO"

    # S3 (dev only, look s3.tf)
    S3_BRONZE_BUCKET = aws_s3_bucket.data_lake.bucket
    S3_BRONZE_PREFIX = "data/bronze"

    # placeholder for now, set real value in AWS console
    ALPHA_API_KEY     = "CHANGE ME"
  }
}