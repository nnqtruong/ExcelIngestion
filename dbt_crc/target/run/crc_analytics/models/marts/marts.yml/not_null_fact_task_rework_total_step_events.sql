
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_step_events
from "dev_warehouse"."main"."fact_task_rework"
where total_step_events is null



  
  
      
    ) dbt_internal_test