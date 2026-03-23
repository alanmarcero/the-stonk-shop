resource "aws_sqs_queue" "batches" {
  name                       = "ema-scanner-batches"
  visibility_timeout_seconds = 300
  message_retention_seconds  = 3000
}
