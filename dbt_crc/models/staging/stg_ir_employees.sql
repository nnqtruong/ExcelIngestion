{{
  config(
    materialized='view'
  )
}}
{#
  Insurance Resources employee team mapping.
  Normalizes network logins and standardizes flags.
#}
with source as (
  select * from {{ source('raw', 'ir_employees') }}
),
normalized as (
  select
    t.row_id,
    t.team,
    t.teamid,
    t.city,
    t.user_full_name,
    -- Normalize network login: trim, lowercase for consistency
    trim(lower(t.user_network_login)) as user_network_login,
    upper(t.acctexecflag) as acctexecflag,
    t.multi_team_id,
    t.drawer,
    t.file_type,
    t._source_file,
    t._ingested_at
  from source t
)
select * from normalized
