variable "cloudflare_account_id" {
  description = "Cloudflare account ID that owns the R2 bucket."
  type        = string

  validation {
    condition     = can(regex("^[0-9a-fA-F]{32}$", var.cloudflare_account_id))
    error_message = "The Cloudflare account ID must be a 32-character hexadecimal string."
  }
}

variable "r2_bucket_name" {
  description = "Name of the Cloudflare R2 bucket for OSS Pulse lake data."
  type        = string
  default     = "oss-pulse-lake"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.r2_bucket_name))
    error_message = "The R2 bucket name must be 3-63 lowercase letters, digits, or hyphens."
  }
}
