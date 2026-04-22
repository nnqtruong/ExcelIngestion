
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_rework_task
from "dev_warehouse"."main"."fact_task_rework"
where is_rework_task is null



  
  
      
    ) dbt_internal_test