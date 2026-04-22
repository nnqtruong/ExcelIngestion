
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from (select * from "dev_warehouse"."main"."fact_task_event" where work_duration_hours is not null) dbt_subquery

where not(work_duration_hours >= 0)


  
  
      
    ) dbt_internal_test