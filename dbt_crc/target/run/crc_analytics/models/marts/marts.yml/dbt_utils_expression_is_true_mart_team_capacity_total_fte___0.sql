
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from (select * from "dev_warehouse"."main"."mart_team_capacity" where total_fte is not null) dbt_subquery

where not(total_fte >= 0)


  
  
      
    ) dbt_internal_test