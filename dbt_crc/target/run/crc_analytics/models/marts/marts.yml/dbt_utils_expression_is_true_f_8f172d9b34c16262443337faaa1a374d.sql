
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "dev_warehouse"."main"."fact_task_rework"

where not(total_step_events >= 1)


  
  
      
    ) dbt_internal_test