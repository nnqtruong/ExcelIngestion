
  
  create view "dev_warehouse"."main"."stg_tasks__dbt_tmp" as (
    

with source as (
  select * from '../../ExcelIngestion_Data/dev/tasks/analytics/combined.parquet'
),
taskstatus_map as (
  select source_value, target_value from "dev_warehouse"."main"."value_map_taskstatus"
),
flowname_map as (
  select source_value, target_value from "dev_warehouse"."main"."value_map_flowname"
),
normalized as (
  select
    t.row_id,
    t.task_id as taskid,
    regexp_replace(t.drawer, E'\\s+', ' ', 'g') as drawer,
    t.policy_number as policynumber,
    t.filename,
    t.file_number as filenumber,
    t.effective_date as effectivedate,
    t.carrier,
    t.acct_exec as acctexec,
    t.task_description as taskdescription,
    trim(lower(t.assigned_to)) as assignedto,
    trim(lower(t.task_from)) as taskfrom,
    trim(lower(t.operation_by)) as operationby,
    coalesce(vmfn.target_value, t.flow_name) as flowname,
    t.step_name as stepname,
    t.sent_to as sentto,
    t.date_available as dateavailable,
    t.date_initiated as dateinitiated,
    t.date_ended as dateended,
    coalesce(vmts.target_value, t.task_status) as taskstatus,
    t.start_time as starttime,
    t.end_time as endtime,
    t._source_file,
    t._ingested_at
  from source t
  left join taskstatus_map vmts on t.task_status = vmts.source_value
  left join flowname_map vmfn on t.flow_name = vmfn.source_value
)
select * from normalized
  );
