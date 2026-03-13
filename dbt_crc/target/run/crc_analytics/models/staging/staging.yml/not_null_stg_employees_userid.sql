
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select userid
from "dev_warehouse"."main"."stg_employees"
where userid is null



  
  
      
    ) dbt_internal_test