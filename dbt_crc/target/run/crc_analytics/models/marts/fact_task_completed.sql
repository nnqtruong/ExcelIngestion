
  
    
    

    create  table
      "dev_warehouse"."main"."fact_task_completed__dbt_tmp"
  
    as (
      

with ranked_completed as (
  select
    -- business key
    taskid,

    -- optional identifiers
    filenumber,
    policynumber,

    -- dimensional context
    flowname,
    stepname as final_stepname,
    carrier,
    acctexec,

    -- lifecycle timestamps
    dateinitiated,
    dateended,

    -- atomic metric: end-to-end TAT (hours)
    round(datediff('hour', dateinitiated, dateended), 2) as tat_hours,

    -- business date for time intelligence
    dateended::date as completed_date,

    -- deterministic deduplication: latest dateended wins
    row_number() over (
      partition by taskid
      order by dateended desc
    ) as rn

  from "dev_warehouse"."main"."stg_tasks"
  where taskstatus = 'Completed'
    and dateended is not null
    and dateinitiated is not null
)

select
  taskid,
  filenumber,
  policynumber,
  flowname,
  final_stepname,
  carrier,
  acctexec,
  completed_date,
  dateinitiated,
  dateended,
  tat_hours
from ranked_completed
where rn = 1
    );
  
  