
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select aging_hours
from "dev_warehouse"."main"."fact_task_current"
where aging_hours is null



  
  
      
    ) dbt_internal_test