
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select headcount
from "dev_warehouse"."main"."mart_team_capacity"
where headcount is null



  
  
      
    ) dbt_internal_test