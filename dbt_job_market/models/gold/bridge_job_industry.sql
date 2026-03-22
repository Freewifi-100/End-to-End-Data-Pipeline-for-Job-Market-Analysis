{{
  config(
    materialized = 'table',
    )
}}

with get_dim_job as (
    select
        job_id,
        industries
    from {{ ref('dim_job') }}
),
get_dim_industries as (
    select
        industry_key,
        industry_name
    from {{ ref('dim_industries') }}
),
bridge_job_industry as (
  select
    job_id,
    industry_name
  from {{ ref('silver_job_industries') }}
)

select
    dj.job_id,
    di.industry_key
from get_dim_job dj
inner join bridge_job_industry bji
    on dj.job_id = bji.job_id
inner join get_dim_industries di
    on bji.industry_name = di.industry_name