############################
# IAM Assume Role Policies
############################

data "aws_iam_policy_document" "assume_lambda" {
  statement {
    effect = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "assume_glue" {
  statement {
    effect = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "assume_stepfunctions" {
  statement {
    effect = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

#############################
# S3 Access Policy Documents
#############################

# common prefixes that could be used in the dev bucket:
# - data/... (bronze/silver/gold/meta)
# - artifacts/... (lambda zips, glue scripts, layers)
# - config/... (optional configs)
locals {
  s3_allowed_prefixes = [
    "data/*",
    "artifacts/*",
    "config/*"
  ]
}

data "aws_iam_policy_document" "s3_dev_rw" {
  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data_lake.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = local.s3_allowed_prefixes
    }
  }

  # Read/write objects under allowed prefixes
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListBucketMultipartUploads",
      "s3:ListMultipartUploadParts"
    ]
    resources = [
      for p in local.s3_allowed_prefixes :
      "${aws_s3_bucket.data_lake.arn}/${p}"
    ]
  }
}

############################
# CloudWatch Logs policies
############################

data "aws_iam_policy_document" "cloudwatch_logs" {
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["*"]
  }
}

###############
# Lambda roles
###############

resource "aws_iam_role" "lambda_planner" {
  name               = "${var.project_name}-${var.environment}-lambda-planner"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy" "lambda_planner_logs" {
  name   = "${var.project_name}-${var.environment}-lambda-planner-logs"
  role   = aws_iam_role.lambda_planner.id
  policy = data.aws_iam_policy_document.cloudwatch_logs.json
}

resource "aws_iam_role_policy" "lambda_planner_s3" {
  name   = "${var.project_name}-${var.environment}-lambda-planner-s3"
  role   = aws_iam_role.lambda_planner.id
  policy = data.aws_iam_policy_document.s3_dev_rw.json
}

resource "aws_iam_role" "lambda_worker" {
  name               = "${var.project_name}-${var.environment}-lambda-worker"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy" "lambda_worker_logs" {
  name   = "${var.project_name}-${var.environment}-lambda-worker-logs"
  role   = aws_iam_role.lambda_worker.id
  policy = data.aws_iam_policy_document.cloudwatch_logs.json
}

resource "aws_iam_role_policy" "lambda_worker_s3" {
  name   = "${var.project_name}-${var.environment}-lambda-worker-s3"
  role   = aws_iam_role.lambda_worker.id
  policy = data.aws_iam_policy_document.s3_dev_rw.json
}

#############
# Glue role
#############

resource "aws_iam_role" "glue" {
  name               = "${var.project_name}-${var.environment}-glue"
  assume_role_policy = data.aws_iam_policy_document.assume_glue.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy" "glue_logs" {
  name   = "${var.project_name}-${var.environment}-glue-logs"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.cloudwatch_logs.json
}

resource "aws_iam_role_policy" "glue_s3" {
  name   = "${var.project_name}-${var.environment}-glue-s3"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.s3_dev_rw.json
}

# Glue also typically needs access to Glue Catalog APIs.
data "aws_iam_policy_document" "glue_catalog_min" {
  statement {
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:BatchCreatePartition",
      "glue:BatchDeletePartition",
      "glue:BatchGetPartition"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "glue_catalog" {
  name   = "${var.project_name}-${var.environment}-glue-catalog"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_catalog_min.json
}

######################
# Step Functions role
######################

resource "aws_iam_role" "stepfunctions" {
  name               = "${var.project_name}-${var.environment}-stepfunctions"
  assume_role_policy = data.aws_iam_policy_document.assume_stepfunctions.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy" "stepfunctions_logs" {
  name   = "${var.project_name}-${var.environment}-stepfunctions-logs"
  role   = aws_iam_role.stepfunctions.id
  policy = data.aws_iam_policy_document.cloudwatch_logs.json
}

# for now, allow Step Functions to invoke Lambdas and start Glue jobs broadly
data "aws_iam_policy_document" "stepfunctions_invoke_and_glue" {
  statement {
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = ["*"]
  }

  statement {
    effect = "Allow"
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:GetJob"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "stepfunctions_invoke_and_glue" {
  name   = "${var.project_name}-${var.environment}-stepfunctions-invoke-glue"
  role   = aws_iam_role.stepfunctions.id
  policy = data.aws_iam_policy_document.stepfunctions_invoke_and_glue.json
}