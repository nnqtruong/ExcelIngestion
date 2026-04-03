

with source as (
  select * from 'c:\Users\quang\CRC Code\ExcelIngestion_Data/dev/workers/analytics/combined.parquet'
),

cleaned as (
  select
    row_id,
    employee_id,
    teammate,
    current_status,
    country,
    company_hierarchy,
    company,
    location,
    cost_center_hierarchy,
    cost_center,
    operating_segment,
    business_unit,
    supervisory_organization,
    job_profile,
    business_title,
    position,
    management_level,
    hire_date,
    original_hire_date,
    continuous_service_date,
    last_termination_date,
    scheduled_weekly_hours,
    fte,
    full_part_time,
    pay_rate_type,
    exempt_status,
    worker_type,
    worker_sub_type,
    employee_type,
    lower(trim(work_email)) as work_email,
    direct_manager,
    skip_level_manager,
    executive_leader,
    _source_file,
    _ingested_at
  from source
)

select * from cleaned