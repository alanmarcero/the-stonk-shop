resource "aws_scheduler_schedule" "hourly" {
  name       = "ema-scanner-hourly"
  group_name = "default"

  schedule_expression          = "cron(0 10-16 ? * MON-FRI *)"
  schedule_expression_timezone = "America/New_York"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.orchestrator.arn
    role_arn = aws_iam_role.scheduler.arn
  }
}

resource "aws_scheduler_schedule" "weekly_snapshot" {
  name       = "ema-scanner-weekly-snapshot"
  group_name = "default"

  schedule_expression          = "cron(0 3 ? * MON *)" 
  schedule_expression_timezone = "America/New_York"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.orchestrator.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ snapshot = true })
  }
}
