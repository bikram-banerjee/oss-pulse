---
title: HN Effect
---

# Hacker News effect

Compare average GitHub star velocity in the 48 hours before and after each
Hacker News mention. Velocities are normalized to stars per day.

```sql data_updated
select last_updated from oss_pulse.dashboard_updated
```

```sql hn_summary
select * from oss_pulse.hn_effect_summary
```

<BigValue
    data={hn_summary}
    value=avg_velocity_change
    title="Average change in stars per day after mention"
    fmt="num1"
/>

{hn_summary[0].takeaway}

```sql hn_repositories
select *
from oss_pulse.hn_effect
where repo_full_name is not null
```

<BarChart
    data={hn_repositories}
    x=repo_full_name
    y=avg_stars_per_day
    series=period
    title="Average star velocity before and after HN mentions"
/>

---

Data last updated: <Value data={data_updated} column=last_updated fmt="yyyy-mm-dd hh:mm" />
