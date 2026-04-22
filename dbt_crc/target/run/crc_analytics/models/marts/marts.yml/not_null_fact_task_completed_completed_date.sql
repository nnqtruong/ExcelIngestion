
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select completed_date
from "dev_warehouse"."main"."fact_task_completed"
where completed_date is null



  
  
      
    ) dbt_internal_test