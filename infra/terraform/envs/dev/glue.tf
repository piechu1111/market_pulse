#############
# Glue Jobs
#############

locals {
  dev_bucket = aws_s3_bucket.data_lake.bucket

  # dev paths (with data/... prefix)
  bronze_path = "s3://${local.dev_bucket}/data/bronze/alpha_vantage/intraday_1min/"
  silver_path = "s3://${local.dev_bucket}/data/silver/alpha_vantage/intraday_1min/"

  gold_daily_path         = "s3://${local.dev_bucket}/data/gold/daily_ohlcv/"
  gold_features_path      = "s3://${local.dev_bucket}/data/gold/daily_features/"
  gold_features_dim_path  = "s3://${local.dev_bucket}/data/gold/daily_features_dim/"
  gold_bubble_signals_path = "s3://${local.dev_bucket}/data/gold/bubble_signals_daily/"
  # file needs to be uploaded first
  dim_symbol_path = "s3://${local.dev_bucket}/data/meta/dim_symbol/v1/"

  glue_common_args = {
    "--job-language"         = "python"
    "--job-bookmark-option"  = "job-bookmark-disable"
    "--enable-metrics"       = "true"
    "--TempDir"              = "s3://${local.dev_bucket}/artifacts/glue/temporary/"
  }
}

# SILVER: alpha-vantage-bronze-to-silver

resource "aws_glue_job" "silver_bronze_to_silver" {
  name     = "alpha-vantage-bronze-to-silver-tf"
  role_arn = aws_iam_role.glue.arn

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 3

  timeout = 150
  max_retries = 0

  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${local.dev_bucket}/${aws_s3_object.glue_silver_bronze_to_silver.key}"
  }

  default_arguments = merge(
    local.glue_common_args,
    {
      "--enable-auto-scaling" = "true"

      "--BRONZE_PATH"      = local.bronze_path
      "--SILVER_PATH"      = local.silver_path

      # set defaults, Step Functions can override per run
      "--YEAR_MONTH_START" = "2026-01"
      "--YEAR_MONTH_END"   = "2026-02"
    }
  )

  tags = local.common_tags
}

# GOLD: daily_ohlcv

resource "aws_glue_job" "gold_daily_ohlcv" {
  name     = "gold.daily_ohlcv_tf"
  role_arn = aws_iam_role.glue.arn

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  timeout = 120
  max_retries = 0

  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${local.dev_bucket}/${aws_s3_object.glue_gold_daily_ohlcv.key}"
  }

  default_arguments = merge(
    local.glue_common_args,
    {
      "--GOLD_DAILY_PATH" = local.gold_daily_path
      "--SILVER_PATH"     = local.silver_path
      "--YEAR_START"      = "2011"
      "--YEAR_END"        = "2025"
    }
  )

  tags = local.common_tags
}

# GOLD: daily_features

resource "aws_glue_job" "gold_daily_features" {
  name     = "gold.daily_features_tf"
  role_arn = aws_iam_role.glue.arn

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  timeout = 120
  max_retries = 0

  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${local.dev_bucket}/${aws_s3_object.glue_gold_daily_features.key}"
  }

  default_arguments = merge(
    local.glue_common_args,
    {
      "--DAILY_PATH"    = local.gold_daily_path
      "--FEATURES_PATH" = local.gold_features_path
      "--YEAR_START"    = "2021"
      "--YEAR_END"      = "2024"
    }
  )

  tags = local.common_tags
}

# GOLD: daily_features_dim

resource "aws_glue_job" "gold_daily_features_dim" {
  name     = "gold.daily_features_dim_tf"
  role_arn = aws_iam_role.glue.arn

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  timeout = 120
  max_retries = 0

  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${local.dev_bucket}/${aws_s3_object.glue_gold_daily_features_dim.key}"
  }

  default_arguments = merge(
    local.glue_common_args,
    {
      "--DAILY_FEATURES_PATH" = local.gold_features_path
      "--DIM_SYMBOL_PATH"     = local.dim_symbol_path
      "--OUTPUT_PATH"         = local.gold_features_dim_path
      "--YEAR_START"          = "2000"
      "--YEAR_END"            = "2024"
    }
  )

  tags = local.common_tags
}

# GOLD: bubble_signals_daily

resource "aws_glue_job" "gold_bubble_signals_daily" {
  name     = "gold.bubble_signals_daily_tf"
  role_arn = aws_iam_role.glue.arn

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  timeout = 120
  max_retries = 0

  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${local.dev_bucket}/${aws_s3_object.glue_gold_bubble_signals.key}"
  }

  default_arguments = merge(
    local.glue_common_args,
    {
      "--DIM_SYMBOL_PATH" = local.dim_symbol_path
      "--FEATURES_PATH"   = local.gold_features_path
      "--OUTPUT_PATH"     = local.gold_bubble_signals_path
      "--YEAR_START"      = "2000"
      "--YEAR_END"        = "2024"
    }
  )

  tags = local.common_tags
}
