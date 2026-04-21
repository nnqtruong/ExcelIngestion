
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select as_of_date
from "dev_warehouse"."main"."fact_task_current"
where as_of_date is null



  
  
      
    ) dbt_internal_test