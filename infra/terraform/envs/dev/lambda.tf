##########################################
# Lambda packaging (zip from repo source)
##########################################

data "archive_file" "planner_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../../../src/lambdas/planner"
  output_path = "${path.module}/build/planner.zip"
}

data "archive_file" "worker_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../../../src/lambdas/worker"
  output_path = "${path.module}/build/worker.zip"
}

##################
# Lambda: Planner
##################

resource "aws_lambda_function" "planner" {
  function_name = "${var.project_name}-${var.environment}-planner"
  role          = aws_iam_role.lambda_planner.arn

  runtime = "python3.11"
  handler = "handler.lambda_handler"

  filename         = data.archive_file.planner_zip.output_path
  source_code_hash = data.archive_file.planner_zip.output_base64sha256

  timeout      = 30
  memory_size  = 256
  # pin Lambda architecture to avoid implicit defaults and ensure reproducible builds
  # x86_64 safer than arm64 - for now
  architectures = ["x86_64"]

  environment {
    variables = local.lambda_common_env
  }

  tags = local.common_tags
}

#################
# Lambda: Worker
#################

resource "aws_lambda_function" "worker" {
  function_name = "${var.project_name}-${var.environment}-worker"
  role          = aws_iam_role.lambda_worker.arn

  runtime = "python3.11"
  handler = "handler.lambda_handler"

  filename         = data.archive_file.worker_zip.output_path
  source_code_hash = data.archive_file.worker_zip.output_base64sha256

  timeout      = 60
  memory_size  = 512
  # same reason as before
  architectures = ["x86_64"]

  environment {
    variables = local.lambda_common_env
  }

  tags = local.common_tags
}
