{{ config(materialized='table') }}

with unique_table as(
    select distinct
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
)
select
    md5(concat(job_id, job_description, job_title, industries, employment_type, seniority_level, job_function)) as job_key,
    job_id,
    job_title,
    job_website,
    job_url,
    job_function, 
    industries,
    employment_type,
    seniority_level, 
    job_description
from unique_table


