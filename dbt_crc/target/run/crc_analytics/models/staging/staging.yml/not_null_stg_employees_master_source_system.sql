
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select source_system
from "dev_warehouse"."main"."stg_employees_master"
where source_system is null



  
  
      
    ) dbt_internal_test