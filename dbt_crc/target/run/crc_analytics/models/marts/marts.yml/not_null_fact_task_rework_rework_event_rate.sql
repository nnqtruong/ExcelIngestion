
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rework_event_rate
from "dev_warehouse"."main"."fact_task_rework"
where rework_event_rate is null



  
  
      
    ) dbt_internal_test