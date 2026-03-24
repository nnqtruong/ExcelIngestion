
  
    
    

    create  table
      "dev_warehouse"."main"."mart_turnaround__dbt_tmp"
  
    as (
      

select
  t.drawer,
  t.flowname,
  t.stepname,
  count(*) as completed_count,
  round(avg(datediff('minute', t.starttime, t.endtime) / 60.0), 2) as avg_handle_hours,
  round(avg(datediff('hour', t.dateinitiated, t.dateended)), 2) as avg_lifecycle_hours
from "dev_warehouse"."main"."stg_tasks" t
where t.taskstatus = 'Completed'
  and t.starttime is not null
  and t.endtime is not null
group by t.drawer, t.flowname, t.stepname
    );
  
  