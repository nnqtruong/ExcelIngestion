
with tasks as (
  select * from "dev_warehouse"."main"."stg_tasks"
),
assigned_employee as (
  select userid, division, team from "dev_warehouse"."main"."stg_employees"
),
from_employee as (
  select userid, division, team from "dev_warehouse"."main"."stg_employees"
),
operation_employee as (
  select userid, division, team from "dev_warehouse"."main"."stg_employees"
)
select
  t.row_id,
  t.taskid,
  t.drawer,
  t.policynumber,
  t.filename,
  t.filenumber,
  t.effectivedate,
  t.carrier,
  t.acctexec,
  t.taskdescription,
  t.assignedto,
  t.taskfrom,
  t.operationby,
  t.flowname,
  t.stepname,
  t.sentto,
  t.dateavailable,
  t.dateinitiated,
  t.dateended,
  t.taskstatus,
  t.starttime,
  t.endtime,
  t._source_file,
  t._ingested_at,
  a.division as assigned_division,
  a.team as assigned_team,
  f.division as from_division,
  f.team as from_team,
  o.division as operation_division,
  o.team as operation_team,
  datediff('minute', t.starttime, t.endtime) as duration_minutes,
  round(datediff('minute', t.starttime, t.endtime) / 60.0, 2) as duration_hours,
  round(datediff('hour', t.dateinitiated, t.dateended), 2) as lifecycle_hours,
  cast(t.dateinitiated as date) as task_date
from tasks t
left join assigned_employee a on t.assignedto = lower(trim(a.userid))
left join from_employee f on t.taskfrom = lower(trim(f.userid))
left join operation_employee o on t.operationby = lower(trim(o.userid))