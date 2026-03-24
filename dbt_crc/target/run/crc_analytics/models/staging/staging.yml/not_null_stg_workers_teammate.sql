
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select teammate
from "dev_warehouse"."main"."stg_workers"
where teammate is null



  
  
      
    ) dbt_internal_test