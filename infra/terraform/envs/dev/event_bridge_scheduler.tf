########################
# EventBridge Scheduler
########################

resource "aws_scheduler_schedule" "ingestion_tf" {
  name       = "${var.project_name}-${var.environment}-ingestion-tf"
  group_name = "default"

  # legacy: cron(0 12 ? * 1 *) = Monday 12:00 UTC
  schedule_expression = "cron(0 12 ? * 1 *)"

  flexible_time_window {
    mode = "OFF"
  }

  # for now
  state = "DISABLED"

  target {
    arn      = aws_sfn_state_machine.ingest_tf.arn
    role_arn = aws_iam_role.scheduler_to_sfn.arn

    input = jsonencode({
      symbols_s3_uri = "s3://${aws_s3_bucket.data_lake.bucket}/data/meta/dim_symbol/v1/dim_symbol.csv"
      batch_size     = 50
    })
  }
}