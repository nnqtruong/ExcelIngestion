{{
  config(
    materialized='view',
    tags=['marts']
  )
}}
select
  task_date,
  count(*) as total_tasks,
  sum(case when taskstatus = 'Completed' then 1 else 0 end) as completed_tasks,
  sum(case when taskstatus = 'In Progress' then 1 else 0 end) as in_progress_tasks
from {{ ref('mart_tasks_enriched') }}
where task_date is not null
group by task_date
order by task_date
