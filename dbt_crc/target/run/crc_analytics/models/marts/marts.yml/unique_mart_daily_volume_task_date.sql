
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    task_date as unique_field,
    count(*) as n_records

from "dev_warehouse"."main"."mart_daily_volume"
where task_date is not null
group by task_date
having count(*) > 1



  
  
      
    ) dbt_internal_test