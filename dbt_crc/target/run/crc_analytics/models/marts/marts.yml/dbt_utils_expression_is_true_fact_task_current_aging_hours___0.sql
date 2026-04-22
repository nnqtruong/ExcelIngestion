
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "dev_warehouse"."main"."fact_task_current"

where not(aging_hours >= 0)


  
  
      
    ) dbt_internal_test