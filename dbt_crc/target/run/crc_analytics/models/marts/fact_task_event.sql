
  
    
    

    create  table
      "dev_warehouse"."main"."fact_task_event__dbt_tmp"
  
    as (
      

with dedup_step_events as (
  select
    taskid,
    flowname,
    stepname,
    assignedto,
    dateavailable,
    starttime,
    endtime,

    -- metrics: queue wait (available -> start)
    case
      when starttime is not null and dateavailable is not null
      then round(datediff('minute', dateavailable, starttime) / 60.0, 2)
      else null
    end as queue_wait_hours,

    -- metrics: work duration (start -> end)
    case
      when endtime is not null and starttime is not null
      then round(datediff('minute', starttime, endtime) / 60.0, 2)
      else null
    end as work_duration_hours,

    -- event date
    dateavailable::date as event_date,

    -- exact-duplicate removal: same (taskid, stepname, dateavailable, starttime, endtime)
    row_number() over (
      partition by
        taskid,
        stepname,
        dateavailable,
        starttime,
        endtime
      order by row_id  -- use row_id as tiebreaker
    ) as rn

  from "dev_warehouse"."main"."stg_tasks"
  where dateavailable is not null
)

select
  taskid,
  flowname,
  stepname,
  assignedto,
  dateavailable,
  starttime,
  endtime,
  event_date,
  queue_wait_hours,
  work_duration_hours
from dedup_step_events
where rn = 1
    );
  
  