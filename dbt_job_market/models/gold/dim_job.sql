{{ config(materialized='table') }}


select
    distinct
    md5(job_id || scraped_at) as job_key,
    job_id,
    job_title,
    job_website,
    job_url,
    job_function,
    industries,
    seniority_level,
    employment_type,
    job_description

from {{ ref('silver_linkedin_data') }}

