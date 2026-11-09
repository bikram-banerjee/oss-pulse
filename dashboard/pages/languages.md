---
title: Languages
---

# Language trends

Weekly stars across the eight most-starred languages in the latest 26 weeks.
Use the legend to focus on a language; weeks without observed activity are not
filled with synthetic values.

```sql data_updated
select last_updated from oss_pulse.dashboard_updated
```

```sql weekly_stars
select week_start, language, stars
from oss_pulse.language_trends
where week_start is not null
```

{#if weekly_stars.length > 0}
    <LineChart
        data={weekly_stars}
        x=week_start
        y=stars
        series=language
        title="Weekly GitHub stars by language"
    />
{:else}
No language trend data is available for the latest 26 weeks.
{/if}

---

Data last updated: <Value data={data_updated} column=last_updated fmt="yyyy-mm-dd hh:mm" />
