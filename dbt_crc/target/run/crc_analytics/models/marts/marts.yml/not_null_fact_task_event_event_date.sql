
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_date
from "dev_warehouse"."main"."fact_task_event"
where event_date is null



  
  
      
    ) dbt_internal_test