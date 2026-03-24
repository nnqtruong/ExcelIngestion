
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select task_date
from "dev_warehouse"."main"."mart_daily_trend"
where task_date is null



  
  
      
    ) dbt_internal_test