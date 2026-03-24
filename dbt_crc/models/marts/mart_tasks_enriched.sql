{{
  config(
    materialized='table',
    tags=['marts']
  )
}}

select
  t.*,
  w.teammate,
  w.job_profile as worker_job_profile,
  w.business_title,
  w.management_level,
  w.cost_center,
  w.cost_center_hierarchy,
  w.fte,
  w.direct_manager,
  em.source_system as employee_source,
  datediff('minute', t.starttime, t.endtime) as duration_minutes,
  round(datediff('minute', t.starttime, t.endtime) / 60.0, 2) as duration_hours,
  round(datediff('hour', t.dateinitiated, t.dateended), 2) as lifecycle_hours,
  cast(t.dateinitiated as date) as task_date
from {{ ref('stg_tasks') }} t
left join {{ ref('stg_workers') }} w
  on t.assignedto = lower(trim(cast(w.employee_id as varchar)))
left join {{ ref('stg_employees_master') }} em
  on t.assignedto = em.employee_id
