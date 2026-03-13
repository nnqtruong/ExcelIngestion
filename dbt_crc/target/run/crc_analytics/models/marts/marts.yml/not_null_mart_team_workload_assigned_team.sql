
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select assigned_team
from "dev_warehouse"."main"."mart_team_workload"
where assigned_team is null



  
  
      
    ) dbt_internal_test