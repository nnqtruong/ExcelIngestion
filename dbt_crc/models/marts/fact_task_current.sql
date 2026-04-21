{{
  config(
    materialized='table',
    tags=['marts', 'facts']
  )
}}

with ranked_open as (
  select
    -- business key
    taskid,

    -- workflow context
    flowname,
    stepname as current_stepname,

    -- ownership (raw)
    assignedto,

    -- optional business context
    filenumber,
    policynumber,
    carrier,
    acctexec,

    -- queue entry
    dateavailable,

    -- metrics: aging in hours since became available
    round(datediff('hour', dateavailable, current_timestamp), 2) as aging_hours,

    -- snapshot date
    current_date as as_of_date,

    -- deduplication: most recent availability = current state
    row_number() over (
      partition by taskid
      order by dateavailable desc
    ) as rn

  from {{ ref('stg_tasks') }}
  where taskstatus != 'Completed'
    and dateavailable is not null
)

select
  taskid,
  flowname,
  current_stepname,
  assignedto,
  filenumber,
  policynumber,
  carrier,
  acctexec,
  dateavailable,
  as_of_date,
  aging_hours
from ranked_open
where rn = 1
