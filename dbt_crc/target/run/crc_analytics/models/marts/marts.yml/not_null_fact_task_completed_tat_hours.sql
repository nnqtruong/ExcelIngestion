
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tat_hours
from "dev_warehouse"."main"."fact_task_completed"
where tat_hours is null



  
  
      
    ) dbt_internal_test