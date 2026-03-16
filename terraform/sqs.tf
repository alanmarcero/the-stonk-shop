resource "aws_sqs_queue" "batches" {
  name                       = "ema-scanner-batches"
  visibility_timeout_seconds = 180
  message_retention_seconds  = 86400
}
