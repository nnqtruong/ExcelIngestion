
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select flowname
from "dev_warehouse"."main"."mart_onshore_offshore"
where flowname is null



  
  
      
    ) dbt_internal_test