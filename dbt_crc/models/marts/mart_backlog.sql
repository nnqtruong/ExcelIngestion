{{
  config(
    materialized='table',
    tags=['marts']
  )
}}

select
  t.drawer,
  t.flowname,
  t.stepname,
  t.taskstatus,
  count(*) as task_count,
  round(avg(datediff('day', t.dateinitiated, current_date)), 1) as avg_age_days
from {{ ref('stg_tasks') }} t
where t.taskstatus != 'Completed'
group by t.drawer, t.flowname, t.stepname, t.taskstatus
