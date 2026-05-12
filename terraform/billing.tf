data "aws_sns_topic" "billing" {
  name = "billing-alarm"
}

resource "aws_cloudwatch_metric_alarm" "billing_10_usd" {
  alarm_name          = "billing-alarm-10-usd"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "EstimatedCharges"
  namespace           = "AWS/Billing"
  period              = "21600"
  statistic           = "Maximum"
  threshold           = "10"
  alarm_description   = "Billing alarm for when estimated charges cross 10 USD"
  alarm_actions       = [data.aws_sns_topic.billing.arn]
  
  dimensions = {
    Currency = "USD"
  }
}
