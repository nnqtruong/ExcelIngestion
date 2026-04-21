
  
  create view "dev_warehouse"."main"."stg_employees_master__dbt_tmp" as (
    

with source as (
  select * from '../../ExcelIngestion_Data/dev/employees_master/analytics/combined.parquet'
),

ranked as (
  select
    *,
    row_number() over (
      partition by lower(trim(cast(employee_id as varchar)))
      order by _ingested_at desc nulls last
    ) as rn
  from source
),

cleaned as (
  select
    row_id,
    lower(trim(cast(employee_id as varchar))) as employee_id,
    name,
    supervisory_organization,
    job_profile,
    cost_center,
    hire_date,
    is_recent_hire,
    location,
    term_date,
    is_recent_term,
    term_reason,
    role_disposition,
    combined_hierarchy,
    team_or_office,
    addressable_population,
    email,
    genpact_id,
    genpact_phase,
    genpact_crc_name,
    genpact_mapping,
    source_system,
    _source_file,
    _ingested_at
  from ranked
  where rn = 1
)

select * from cleaned
  );
