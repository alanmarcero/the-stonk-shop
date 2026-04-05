resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "scanner" {
  bucket = "${var.bucket_prefix}-${random_id.bucket_suffix.hex}"
}

resource "aws_s3_bucket_public_access_block" "scanner" {
  bucket = aws_s3_bucket.scanner.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "scanner" {
  bucket = aws_s3_bucket.scanner.id

  rule {
    id     = "expire-batches"
    status = "Enabled"

    filter {
      prefix = "batches/"
    }

    expiration {
      days = 7
    }
  }

  rule {
    id     = "expire-logs"
    status = "Enabled"

    filter {
      prefix = "logs/"
    }

    expiration {
      days = 7
    }
  }

  rule {
    id     = "expire-dated-snapshots"
    status = "Enabled"

    filter {
      prefix = "results/20"
    }

    expiration {
      days = 60
    }
  }
}

resource "aws_s3_bucket_policy" "cloudfront_access" {
  bucket = aws_s3_bucket.scanner.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowCloudFrontOAC"
        Effect = "Allow"
        Principal = {
          Service = "cloudfront.amazonaws.com"
        }
        Action   = "s3:GetObject"
        Resource = [
          "${aws_s3_bucket.scanner.arn}/results/*",
          "${aws_s3_bucket.scanner.arn}/app/*",
          "${aws_s3_bucket.scanner.arn}/symbols/*",
          "${aws_s3_bucket.scanner.arn}/favicon.ico",
        ]
        Condition = {
          StringEquals = {
            "AWS:SourceArn" = aws_cloudfront_distribution.results.arn
          }
        }
      }
    ]
  })
}
