
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "dev_warehouse"."main"."fact_task_rework"

where not(rework_event_rate <= 1)


  
  
      
    ) dbt_internal_test