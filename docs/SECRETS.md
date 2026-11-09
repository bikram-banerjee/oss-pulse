# GitHub Actions secrets

Configure these values in **Settings → Secrets and variables → Actions** for
the repository. Do not commit credentials to workflow files, `.env.example`,
Terraform variables files, or dashboard source.

| Secret | Required by | What it is | Where to get it |
| --- | --- | --- | --- |
| `R2_ACCOUNT_ID` | Production pipeline | Cloudflare account identifier used by the R2 S3-compatible endpoint. | Cloudflare dashboard → account overview. |
| `R2_ACCESS_KEY_ID` | Production pipeline | Access key for an R2 S3 API token with read/write access to the OSS Pulse bucket. | Cloudflare dashboard → R2 → Manage R2 API Tokens; create a token scoped to the bucket. |
| `R2_SECRET_ACCESS_KEY` | Production pipeline | Secret half of the R2 S3 API credentials. | Shown once when creating the R2 API token. Store it immediately. |
| `R2_BUCKET` | Production pipeline | Name of the R2 bucket containing raw and quarantine Parquet data. | Terraform output `r2_bucket_name` after applying `infra/terraform`. |
| `MOTHERDUCK_TOKEN` | Production pipeline and dashboard deployment | Token used by dbt-duckdb and the Evidence MotherDuck connector. | MotherDuck workspace → Settings / API tokens. |
| `MOTHERDUCK_DATABASE` | Production pipeline and dashboard deployment | MotherDuck database name used by the `prod` dbt target and Evidence source. | Create or select the database in the MotherDuck UI. |
| `CLOUDFLARE_PAGES_API_TOKEN` | Dashboard deployment | Cloudflare API token for deploying the static site to Pages. Grant account-level Cloudflare Pages edit permissions. | Cloudflare dashboard → My Profile → API Tokens → create a custom token with Cloudflare Pages edit permissions. |
| `CLOUDFLARE_ACCOUNT_ID` | Dashboard deployment | Cloudflare account identifier for Wrangler. | Cloudflare dashboard → account overview. |
| `DISCORD_WEBHOOK_URL` | Optional production alerts | Incoming webhook URL for pipeline failure alerts. | In the target Discord channel, edit channel → Integrations → Webhooks → New Webhook. |

`GITHUB_TOKEN` is supplied automatically by GitHub Actions as
`${{ github.token }}` and does not need to be configured as a repository
secret. The metadata asset uses it for authenticated GitHub API requests.

The pipeline runs with R2 and MotherDuck credentials. The production dashboard
build queries MotherDuck and exports its source data into a static site; the
credentials are used only during the build and are not exposed to site visitors.
Local dashboard development defaults to the DuckDB file at `data/oss_pulse.duckdb`.
Local CI runs against committed fixtures and needs no repository secrets.

The Pages workflow deploys to the fixed project name `oss-pulse-dashboard`.
Create that Cloudflare Pages project once (for example with
`npx wrangler pages project create oss-pulse-dashboard --production-branch main`)
before the first deployment.

## Local Terraform credential

Terraform itself is not run by GitHub Actions. When provisioning the R2 bucket
locally, set `CLOUDFLARE_API_TOKEN` to a separate Cloudflare API token scoped
to R2 Storage edit permissions, and set `TF_VAR_cloudflare_account_id` to the
account ID. The `CLOUDFLARE_ACCOUNT_ID` Actions secret is only for Pages
deployment. Keeping provisioning and deployment tokens separate limits the
permissions available to each workflow.
