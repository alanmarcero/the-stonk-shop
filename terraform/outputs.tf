output "cloudfront_url" {
  value = "https://${aws_cloudfront_distribution.results.domain_name}/results/latest.json"
}

output "web_app_url" {
  value = "https://${aws_cloudfront_distribution.results.domain_name}/"
}

output "bucket_name" {
  value = aws_s3_bucket.scanner.id
}

output "orchestrator_arn" {
  value = aws_lambda_function.orchestrator.arn
  sensitive = true
}

output "orchestrator_url" {
  value = aws_lambda_function_url.orchestrator_url.function_url
  sensitive = true
}

output "worker_arn" {
  value = aws_lambda_function.worker.arn
  sensitive = true
}

output "cloudfront_distribution_id" {
  value = aws_cloudfront_distribution.results.id
}
