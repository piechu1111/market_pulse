############################
# Lambda artifact locations
############################

locals {
  lambda_artifact_prefix = "artifacts/lambda/${var.artifact_version}"

  lambda_artifact_keys = {
    planner = "${local.lambda_artifact_prefix}/planner.zip"
    worker  = "${local.lambda_artifact_prefix}/worker.zip"
  }

  lambda_log_group_names = {
    planner = "/aws/lambda/${var.project_name}-${var.environment}-planner"
    worker  = "/aws/lambda/${var.project_name}-${var.environment}-worker"
  }
}

#####################################
# CloudWatch log groups for Lambdas
#####################################

resource "aws_cloudwatch_log_group" "lambda_planner" {
  name              = local.lambda_log_group_names.planner
  retention_in_days = 30

  tags = local.common_tags
}

resource "aws_cloudwatch_log_group" "lambda_worker" {
  name              = local.lambda_log_group_names.worker
  retention_in_days = 30

  tags = local.common_tags
}

##################
# Lambda: Planner
##################

resource "aws_lambda_function" "planner" {
  function_name = "${var.project_name}-${var.environment}-planner"
  role          = aws_iam_role.lambda_planner.arn

  runtime = "python3.11"
  handler = "handler.handler"

  s3_bucket = aws_s3_bucket.data_lake.bucket
  s3_key    = local.lambda_artifact_keys.planner

  timeout     = 30
  memory_size = 256

  architectures = ["x86_64"]

  environment {
    variables = local.lambda_common_env
  }

  tags = local.common_tags

  depends_on = [
    aws_iam_role_policy.lambda_planner_logs,
    aws_iam_role_policy.lambda_planner_s3,
    aws_cloudwatch_log_group.lambda_planner
  ]
}

#################
# Lambda: Worker
#################

resource "aws_lambda_function" "worker" {
  function_name = "${var.project_name}-${var.environment}-worker"
  role          = aws_iam_role.lambda_worker.arn

  runtime = "python3.11"
  handler = "handler.handler"

  s3_bucket = aws_s3_bucket.data_lake.bucket
  s3_key    = local.lambda_artifact_keys.worker

  timeout     = 60
  memory_size = 512

  architectures = ["x86_64"]

  environment {
    variables = local.lambda_common_env
  }

  tags = local.common_tags

  depends_on = [
    aws_iam_role_policy.lambda_worker_logs,
    aws_iam_role_policy.lambda_worker_s3,
    aws_cloudwatch_log_group.lambda_worker
  ]
}