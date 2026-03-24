
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select drawer
from "dev_warehouse"."main"."mart_backlog"
where drawer is null



  
  
      
    ) dbt_internal_test