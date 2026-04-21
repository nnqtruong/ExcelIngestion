

with completed as (
  -- Use MART 1 as the list of completed taskids
  select taskid
  from "dev_warehouse"."main"."fact_task_completed"
),

events_dedup as (
  -- Dedupe step events for completed tasks only
  select
    t.taskid,
    t.stepname,
    t.dateavailable,
    t.starttime,
    t.endtime,

    -- queue wait hours
    case
      when t.starttime is not null and t.dateavailable is not null
      then datediff('minute', t.dateavailable, t.starttime) / 60.0
      else 0
    end as queue_wait_hours,

    -- work duration hours
    case
      when t.endtime is not null and t.starttime is not null
      then datediff('minute', t.starttime, t.endtime) / 60.0
      else 0
    end as work_duration_hours,

    -- exact-duplicate removal
    row_number() over (
      partition by t.taskid, t.stepname, t.dateavailable, t.starttime, t.endtime
      order by t.row_id
    ) as rn_dedup,

    -- step occurrence counter (for rework detection)
    row_number() over (
      partition by t.taskid, t.stepname
      order by t.dateavailable, t.starttime, t.endtime, t.row_id
    ) as rn_step_occurrence

  from "dev_warehouse"."main"."stg_tasks" t
  inner join completed c on c.taskid = t.taskid
  where t.dateavailable is not null
),

events_clean as (
  select *
  from events_dedup
  where rn_dedup = 1
),

step_hits as (
  select
    taskid,
    stepname,
    count(*) as hit_count
  from events_clean
  group by taskid, stepname
),

task_rollup as (
  select
    e.taskid,

    -- totals
    count(*) as total_step_events,
    count(distinct e.stepname) as distinct_steps,

    -- rework events: 2nd+ occurrence of same stepname
    sum(case when e.rn_step_occurrence > 1 then 1 else 0 end) as rework_step_events,

    -- rework effort attribution
    sum(case when e.rn_step_occurrence > 1 then coalesce(e.queue_wait_hours, 0) else 0 end) as rework_queue_wait_hours,
    sum(case when e.rn_step_occurrence > 1 then coalesce(e.work_duration_hours, 0) else 0 end) as rework_work_hours

  from events_clean e
  group by e.taskid
),

rework_steps_count as (
  select
    taskid,
    sum(case when hit_count > 1 then 1 else 0 end) as rework_steps_count
  from step_hits
  group by taskid
)

select
  tr.taskid,
  tr.total_step_events,
  tr.distinct_steps,
  tr.rework_step_events,
  coalesce(rsc.rework_steps_count, 0) as rework_steps_count,

  case
    when tr.total_step_events > 0
    then round(tr.rework_step_events::double / tr.total_step_events, 4)
    else 0.0
  end as rework_event_rate,

  round(tr.rework_queue_wait_hours, 2) as rework_queue_wait_hours,
  round(tr.rework_work_hours, 2) as rework_work_hours,

  case when tr.rework_step_events > 0 then 1 else 0 end as is_rework_task

from task_rollup tr
left join rework_steps_count rsc on rsc.taskid = tr.taskid