
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tasks_opened
from "dev_warehouse"."main"."mart_daily_trend"
where tasks_opened is null



  
  
      
    ) dbt_internal_test