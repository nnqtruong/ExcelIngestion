
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_tasks
from "dev_warehouse"."main"."mart_daily_volume"
where total_tasks is null



  
  
      
    ) dbt_internal_test