data "archive_file" "orchestrator" {
  type        = "zip"
  source_dir  = "${path.module}/../src/orchestrator"
  output_path = "${path.module}/.build/orchestrator.zip"
}

data "archive_file" "worker" {
  type        = "zip"
  source_dir  = "${path.module}/../src/worker"
  output_path = "${path.module}/.build/worker.zip"
}

resource "aws_cloudwatch_log_group" "orchestrator" {
  name              = "/aws/lambda/${aws_lambda_function.orchestrator.function_name}"
  retention_in_days = 14
  
  lifecycle {
    ignore_changes = [tags, tags_all]
  }
}

resource "aws_cloudwatch_log_group" "worker" {
  name              = "/aws/lambda/${aws_lambda_function.worker.function_name}"
  retention_in_days = 14

  lifecycle {
    ignore_changes = [tags, tags_all]
  }
}

resource "aws_lambda_function" "orchestrator" {
  function_name    = "ema-scanner-orchestrator"
  role             = aws_iam_role.orchestrator.arn
  runtime          = "python3.12"
  handler          = "app.lambda_handler"
  memory_size      = 128
  timeout          = 30
  filename         = data.archive_file.orchestrator.output_path
  source_code_hash = data.archive_file.orchestrator.output_base64sha256

  environment {
    variables = {
      BUCKET_NAME = aws_s3_bucket.scanner.id
      QUEUE_URL   = aws_sqs_queue.batches.url
      DEV_KEY     = var.dev_key
    }
  }
}

resource "aws_lambda_function" "worker" {
  function_name                  = "ema-scanner-worker"
  role                           = aws_iam_role.worker.arn
  runtime                        = "python3.12"
  handler                        = "app.lambda_handler"
  memory_size                    = 256
  timeout                        = 300


  filename                       = data.archive_file.worker.output_path
  source_code_hash               = data.archive_file.worker.output_base64sha256

  environment {
    variables = {
      BUCKET_NAME      = aws_s3_bucket.scanner.id
      DISTRIBUTION_ID  = aws_cloudfront_distribution.results.id
      CODE_HASH        = data.archive_file.worker.output_base64sha256
    }
  }
}

resource "aws_lambda_function_url" "orchestrator_url" {
  function_name      = aws_lambda_function.orchestrator.function_name
  authorization_type = "NONE"

  cors {
    allow_credentials = true
    allow_origins     = ["https://${aws_cloudfront_distribution.results.domain_name}"]
    allow_methods     = ["POST", "GET"]
    allow_headers     = ["*"]
    expose_headers    = ["*"]
    max_age           = 86400
  }
}

resource "aws_lambda_event_source_mapping" "worker_sqs" {
  event_source_arn = aws_sqs_queue.batches.arn
  function_name    = aws_lambda_function.worker.arn
  batch_size       = 1

  scaling_config {
    maximum_concurrency = 5
  }
}
