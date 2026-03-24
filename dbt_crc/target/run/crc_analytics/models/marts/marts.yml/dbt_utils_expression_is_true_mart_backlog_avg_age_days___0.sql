
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from (select * from "dev_warehouse"."main"."mart_backlog" where avg_age_days is not null) dbt_subquery

where not(avg_age_days >= 0)


  
  
      
    ) dbt_internal_test