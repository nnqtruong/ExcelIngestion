
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "dev_warehouse"."main"."mart_onshore_offshore"

where not(completion_rate >= 0)


  
  
      
    ) dbt_internal_test