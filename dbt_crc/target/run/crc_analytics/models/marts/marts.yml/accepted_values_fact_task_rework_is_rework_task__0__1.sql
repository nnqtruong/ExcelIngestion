
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        is_rework_task as value_field,
        count(*) as n_records

    from "dev_warehouse"."main"."fact_task_rework"
    group by is_rework_task

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test