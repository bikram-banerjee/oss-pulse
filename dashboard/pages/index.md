---
title: OSS Pulse
---

# OSS Pulse

Open-source activity, distilled into repository momentum signals.

```sql data_updated
select last_updated from oss_pulse.dashboard_updated
```

```sql home_metrics
select * from oss_pulse.home_kpis
```

<Grid cols=3>
    <BigValue data={home_metrics} value=events_last_7_days title="GitHub events · 7 days" fmt=num0 />
    <BigValue data={home_metrics} value=repos_tracked title="Repositories tracked" fmt=num0 />
    <BigValue data={home_metrics} value=breakouts_today title="Breakouts today" fmt=num0 />
</Grid>

## Breakout radar

Repositories are ranked only when they meet the configured breakout threshold
after a full 14-day baseline. An empty table means no repository currently
meets that threshold.

```sql breakout_repos
select *
from oss_pulse.breakout_radar
where repo_full_name is not null
```

{#if breakout_repos.length > 0}
<DataTable data={breakout_repos} />
{:else}
No repositories currently meet the breakout threshold.
{/if}

## Explore

- [Language trends](languages)
- [Hacker News effect](hn-effect)
- [Pipeline health](pipeline-health)
- [About OSS Pulse](about)

See the [project runbook](https://github.com/bikram-banerjee/oss-pulse/blob/main/docs/RUNBOOK.md)
for pipeline operations and data freshness checks.

---

Data last updated: <Value data={data_updated} column=last_updated fmt="yyyy-mm-dd hh:mm" />
