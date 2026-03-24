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
}

output "worker_arn" {
  value = aws_lambda_function.worker.arn
}

output "cloudfront_distribution_id" {
  value = aws_cloudfront_distribution.results.id
}

output "dev_key" {
  value     = var.dev_key
  sensitive = true
}
