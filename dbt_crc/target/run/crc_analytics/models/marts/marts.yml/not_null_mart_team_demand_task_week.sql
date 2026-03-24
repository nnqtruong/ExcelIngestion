
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select task_week
from "dev_warehouse"."main"."mart_team_demand"
where task_week is null



  
  
      
    ) dbt_internal_test