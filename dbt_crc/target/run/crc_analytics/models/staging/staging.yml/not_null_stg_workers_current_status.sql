
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select current_status
from "dev_warehouse"."main"."stg_workers"
where current_status is null



  
  
      
    ) dbt_internal_test