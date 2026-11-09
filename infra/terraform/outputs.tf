output "r2_bucket_name" {
  description = "R2 bucket name to set as R2_BUCKET for OSS Pulse."
  value       = cloudflare_r2_bucket.lake.name
}

output "r2_bucket_id" {
  description = "Cloudflare identifier for the created R2 bucket."
  value       = cloudflare_r2_bucket.lake.id
}

output "r2_endpoint_url" {
  description = "S3-compatible endpoint to set as R2_ENDPOINT_URL."
  value       = "https://${var.cloudflare_account_id}.r2.cloudflarestorage.com"
}
