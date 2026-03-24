
  
    
    

    create  table
      "dev_warehouse"."main"."mart_team_capacity__dbt_tmp"
  
    as (
      

select
  w.cost_center_hierarchy as department,
  w.cost_center,
  w.management_level,
  count(*) as headcount,
  sum(w.fte) as total_fte,
  sum(w.scheduled_weekly_hours) as total_weekly_hours
from "dev_warehouse"."main"."stg_workers" w
where w.current_status = 'Active'
group by w.cost_center_hierarchy, w.cost_center, w.management_level
    );
  
  