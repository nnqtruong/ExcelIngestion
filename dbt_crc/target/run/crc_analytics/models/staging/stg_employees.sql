
  
  create view "dev_warehouse"."main"."stg_employees__dbt_tmp" as (
    
with source as (
  select * from '../../ExcelIngestion_Data/dev/dept_mapping/analytics/combined.parquet'
),
normalized as (
  select
    userid,
    id,
    trim(full_name) as full_name,
    case
      when trim(coalesce(title, '')) in ('', 'NULL', 'null', 'N/A', 'n/a') then null
      else title
    end as title,
    case
      when netwarelogin is not null and position(E'\\' in netwarelogin) > 0
      then upper(split_part(netwarelogin, E'\\', 1)) || E'\\' || split_part(netwarelogin, E'\\', 2)
      else netwarelogin
    end as netwarelogin,
    email,
    divisionid,
    division,
    division1,
    teamid,
    team,
    _source_file,
    _ingested_at,
    row_number() over (partition by userid order by _ingested_at desc nulls last) as rn
  from source
)
select
  userid, id, full_name, title, netwarelogin, email,
  divisionid, division, division1, teamid, team,
  _source_file, _ingested_at
from normalized
where rn = 1
  );
