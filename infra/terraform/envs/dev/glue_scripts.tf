############################
# upload Glue scripts to S3
############################

locals {
  glue_repo_root = abspath("${path.module}/../../../../src/glue")
}

# Silver

resource "aws_s3_object" "glue_silver_bronze_to_silver" {
  bucket = aws_s3_bucket.data_lake.bucket
  key    = "artifacts/glue/silver/bronze_to_silver.py"
  source = "${local.glue_repo_root}/silver/bronze_to_silver.py"
  etag   = filemd5("${local.glue_repo_root}/silver/bronze_to_silver.py")
}

# Gold

resource "aws_s3_object" "glue_gold_daily_ohlcv" {
  bucket = aws_s3_bucket.data_lake.bucket
  key    = "artifacts/glue/gold/daily_ohlcv.py"
  source = "${local.glue_repo_root}/gold/daily_ohlcv.py"
  etag   = filemd5("${local.glue_repo_root}/gold/daily_ohlcv.py")
}

resource "aws_s3_object" "glue_gold_daily_features" {
  bucket = aws_s3_bucket.data_lake.bucket
  key    = "artifacts/glue/gold/daily_features.py"
  source = "${local.glue_repo_root}/gold/daily_features.py"
  etag   = filemd5("${local.glue_repo_root}/gold/daily_features.py")
}

resource "aws_s3_object" "glue_gold_daily_features_dim" {
  bucket = aws_s3_bucket.data_lake.bucket
  key    = "artifacts/glue/gold/daily_features_dim.py"
  source = "${local.glue_repo_root}/gold/daily_features_dim.py"
  etag   = filemd5("${local.glue_repo_root}/gold/daily_features_dim.py")
}

resource "aws_s3_object" "glue_gold_bubble_signals" {
  bucket = aws_s3_bucket.data_lake.bucket
  key    = "artifacts/glue/gold/bubble_signals_daily.py"
  source = "${local.glue_repo_root}/gold/bubble_signals_daily.py"
  etag   = filemd5("${local.glue_repo_root}/gold/bubble_signals_daily.py")
}