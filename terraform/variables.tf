variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "bucket_prefix" {
  type    = string
  default = "ema-scanner"
}

variable "dev_key" {
  type      = string
  sensitive = true
}
