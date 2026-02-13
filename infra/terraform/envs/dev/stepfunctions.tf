#################
# Step Functions
#################

locals {
  stepfunctions_templates_dir = "${path.module}/../../../../src/stepfunctions"

  # GOLD definition (uses TF Glue job names)
  gold_definition_tf = templatefile(
    "${local.stepfunctions_templates_dir}/gold.asl.json.tmpl",
    {
      gold_daily_ohlcv_job_name        = aws_glue_job.gold_daily_ohlcv.name
      gold_daily_features_job_name     = aws_glue_job.gold_daily_features.name
      gold_daily_features_dim_job_name = aws_glue_job.gold_daily_features_dim.name
      gold_bubble_signals_job_name     = aws_glue_job.gold_bubble_signals_daily.name
    }
  )

  # INGEST definition (uses TF lambdas, TF silver job, TF gold SM arn, and dev S3 paths)
  ingest_definition_tf = templatefile(
    "${local.stepfunctions_templates_dir}/ingest.asl.json.tmpl",
    {
      planner_lambda_arn = aws_lambda_function.planner.arn
      worker_lambda_arn  = aws_lambda_function.worker.arn

      silver_job_name = aws_glue_job.silver_bronze_to_silver.name

      # dev paths from glue.tf locals
      bronze_path              = local.bronze_path
      silver_path              = local.silver_path
      gold_daily_path          = local.gold_daily_path
      gold_features_path       = local.gold_features_path
      dim_symbol_path          = local.dim_symbol_path
      gold_features_dim_path   = local.gold_features_dim_path
      gold_bubble_signals_path = local.gold_bubble_signals_path

      # points to the TF gold state machine
      gold_state_machine_arn = aws_sfn_state_machine.gold_tf.arn
    }
  )
}

resource "aws_cloudwatch_log_group" "sfn_ingest_tf" {
  name              = "/aws/vendedlogs/states/MarketPulseIngestMachineTF"
  retention_in_days = 30
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "sfn_gold_tf" {
  name              = "/aws/vendedlogs/states/MarketPulseGoldMachineTF"
  retention_in_days = 30
  tags              = local.common_tags
}

resource "aws_sfn_state_machine" "gold_tf" {
  name     = "MarketPulseGoldMachineTF"
  role_arn = aws_iam_role.stepfunctions.arn

  definition = local.gold_definition_tf

  logging_configuration {
    level                  = "ALL"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.sfn_gold_tf.arn}:*"
  }

  tags = local.common_tags
}

resource "aws_sfn_state_machine" "ingest_tf" {
  name     = "MarketPulseIngestMachineTF"
  role_arn = aws_iam_role.stepfunctions.arn

  # This depends on gold_tf due to gold_state_machine_arn usage in template variables
  definition = local.ingest_definition_tf

  logging_configuration {
    level                  = "ALL"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.sfn_ingest_tf.arn}:*"
  }

  tags = local.common_tags
}