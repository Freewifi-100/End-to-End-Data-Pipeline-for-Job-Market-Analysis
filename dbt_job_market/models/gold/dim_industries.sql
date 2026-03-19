{{ config(materialized='table') }}

select distinct
    job_id,
    industry
from {{ ref('silver_job_industries') }}
where industry is not null
