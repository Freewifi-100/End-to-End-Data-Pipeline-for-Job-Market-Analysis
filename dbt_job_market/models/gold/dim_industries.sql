{{ config(materialized='table') }}

select distinct
    md5(TRIM(industry_name)) as industry_key,
    industry_name
from {{ ref('silver_job_industries') }}

