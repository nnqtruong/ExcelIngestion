
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select task_count
from "dev_warehouse"."main"."mart_drawer_performance"
where task_count is null



  
  
      
    ) dbt_internal_test