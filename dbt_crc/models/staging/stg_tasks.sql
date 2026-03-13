{{
  config(
    materialized='view'
  )
}}
with source as (
  select * from {{ source('raw', 'tasks') }}
),
taskstatus_map as (
  select source_value, target_value from {{ ref('value_map_taskstatus') }}
),
flowname_map as (
  select source_value, target_value from {{ ref('value_map_flowname') }}
),
normalized as (
  select
    t.row_id,
    t.taskid,
    regexp_replace(t.drawer, E'\\s+', ' ', 'g') as drawer,
    t.policynumber,
    t.filename,
    t.filenumber,
    t.effectivedate,
    t.carrier,
    t.acctexec,
    t.taskdescription,
    trim(lower(t.assignedto)) as assignedto,
    trim(lower(t.taskfrom)) as taskfrom,
    trim(lower(t.operationby)) as operationby,
    coalesce(vmfn.target_value, t.flowname) as flowname,
    t.stepname,
    t.sentto,
    t.dateavailable,
    t.dateinitiated,
    t.dateended,
    coalesce(vmts.target_value, t.taskstatus) as taskstatus,
    t.starttime,
    t.endtime,
    t._source_file,
    t._ingested_at
  from source t
  left join taskstatus_map vmts on t.taskstatus = vmts.source_value
  left join flowname_map vmfn on t.flowname = vmfn.source_value
)
select * from normalized
