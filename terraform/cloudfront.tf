resource "aws_cloudfront_origin_access_control" "scanner" {
  name                              = "ema-scanner-oac"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_cloudfront_cache_policy" "fifteen_min" {
  name        = "ema-scanner-15min"
  default_ttl = 900
  min_ttl     = 0
  max_ttl     = 900

  parameters_in_cache_key_and_forwarded_to_origin {
    cookies_config { cookie_behavior = "none" }
    headers_config { header_behavior = "none" }
    query_strings_config { query_string_behavior = "none" }
  }
}

resource "aws_cloudfront_cache_policy" "one_hour" {
  name        = "ema-scanner-1hr"
  default_ttl = 3600
  min_ttl     = 0
  max_ttl     = 3600

  parameters_in_cache_key_and_forwarded_to_origin {
    cookies_config { cookie_behavior = "none" }
    headers_config { header_behavior = "none" }
    query_strings_config { query_string_behavior = "none" }
  }
}

resource "aws_cloudfront_distribution" "results" {
  comment             = "EMA Scanner results"
  enabled             = true
  price_class         = "PriceClass_100"
  is_ipv6_enabled     = true
  default_root_object = "app/index.html"

  origin {
    domain_name              = aws_s3_bucket.scanner.bucket_regional_domain_name
    origin_id                = "s3-scanner"
    origin_access_control_id = aws_cloudfront_origin_access_control.scanner.id
  }

  default_cache_behavior {
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "s3-scanner"
    viewer_protocol_policy = "redirect-to-https"
    cache_policy_id        = aws_cloudfront_cache_policy.fifteen_min.id
  }

  ordered_cache_behavior {
    path_pattern           = "css/*"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "s3-scanner"
    viewer_protocol_policy = "redirect-to-https"
    cache_policy_id        = aws_cloudfront_cache_policy.one_hour.id
  }

  ordered_cache_behavior {
    path_pattern           = "js/*"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "s3-scanner"
    viewer_protocol_policy = "redirect-to-https"
    cache_policy_id        = aws_cloudfront_cache_policy.one_hour.id
  }

  ordered_cache_behavior {
    path_pattern           = "app/*"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "s3-scanner"
    viewer_protocol_policy = "redirect-to-https"
    cache_policy_id        = aws_cloudfront_cache_policy.one_hour.id
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}
