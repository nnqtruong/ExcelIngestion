
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select row_id
from "dev_warehouse"."main"."stg_tasks"
where row_id is null



  
  
      
    ) dbt_internal_test