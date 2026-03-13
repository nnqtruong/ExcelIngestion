{{
  config(
    materialized='view',
    tags=['marts']
  )
}}
select
  drawer,
  count(*) as task_count,
  round(avg(duration_hours), 2) as avg_duration_hours,
  round(
    100.0 * sum(case when taskstatus = 'Completed' then 1 else 0 end) / nullif(count(*), 0),
    2
  ) as completion_rate
from {{ ref('mart_tasks_enriched') }}
group by drawer
order by task_count desc
