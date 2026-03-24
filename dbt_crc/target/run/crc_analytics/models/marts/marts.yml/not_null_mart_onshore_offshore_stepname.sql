
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select stepname
from "dev_warehouse"."main"."mart_onshore_offshore"
where stepname is null



  
  
      
    ) dbt_internal_test