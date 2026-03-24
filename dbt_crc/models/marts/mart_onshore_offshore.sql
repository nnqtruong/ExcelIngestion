{{
  config(
    materialized='table',
    tags=['marts']
  )
}}

select
  em.source_system,
  t.flowname,
  t.stepname,
  count(*) as task_count,
  round(avg(datediff('minute', t.starttime, t.endtime) / 60.0), 2) as avg_handle_hours,
  round(
    sum(case when t.taskstatus = 'Completed' then 1 else 0 end) * 100.0 / nullif(count(*), 0),
    1
  ) as completion_rate
from {{ ref('stg_tasks') }} t
left join {{ ref('stg_employees_master') }} em
  on t.assignedto = em.employee_id
where t.starttime is not null and t.endtime is not null
group by em.source_system, t.flowname, t.stepname
