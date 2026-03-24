
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select completion_rate
from "dev_warehouse"."main"."mart_onshore_offshore"
where completion_rate is null



  
  
      
    ) dbt_internal_test