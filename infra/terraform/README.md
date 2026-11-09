# Cloudflare R2 infrastructure

This Terraform configuration creates the OSS Pulse R2 bucket. It does not
create or print R2 access keys; create an R2 API token with the required bucket
permissions in Cloudflare and keep its access key and secret in environment
variables or a local, untracked `.env` file.

## Requirements

- Terraform 1.5 or newer
- A Cloudflare account with R2 enabled
- A Cloudflare API token with permission to manage R2 storage

## Configure and validate

In PowerShell, provide the account ID and API token to the Terraform provider
through environment variables:

```powershell
$env:TF_VAR_cloudflare_account_id = "<cloudflare-account-id>"
$env:CLOUDFLARE_API_TOKEN = "<cloudflare-api-token>"
terraform -chdir=infra/terraform init
terraform -chdir=infra/terraform validate
terraform -chdir=infra/terraform plan
```

In bash, use `export TF_VAR_cloudflare_account_id=...` and
`export CLOUDFLARE_API_TOKEN=...` instead. Do not put credentials in `.tf`,
`.tfvars`, command-line arguments, or committed files.

Review the plan before applying:

```powershell
terraform -chdir=infra/terraform apply
terraform -chdir=infra/terraform output
```

Terraform reports the bucket name, bucket ID, and S3-compatible endpoint. Set
`R2_BUCKET`, `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, and
`R2_ENDPOINT_URL` in the application environment to use it. The Terraform API
token and R2 S3 credentials are separate credentials.

To remove the bucket, run `terraform -chdir=infra/terraform destroy` only after
confirming its contents are no longer needed.
