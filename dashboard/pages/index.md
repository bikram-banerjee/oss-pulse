---
title: OSS Pulse
---

# OSS Pulse

OSS Pulse tracks public GitHub activity and Hacker News mentions to surface
repositories gaining momentum.

## Data pipeline

GitHub events and Hacker News stories are ingested into a partitioned Parquet
lake, transformed with dbt, and stored in the production MotherDuck database.
The production pipeline and data models are being prepared for dashboard
visualizations.

See the [project runbook](https://github.com/bikram-banerjee/oss-pulse/blob/main/docs/RUNBOOK.md)
for pipeline operations and data freshness checks.
