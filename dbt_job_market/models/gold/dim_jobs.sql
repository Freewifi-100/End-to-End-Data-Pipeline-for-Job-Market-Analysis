{{ config(materialized='table') }}

select distinct
    job_id,
    job_title,
    job_website,
    posted_timestamp,
    num_applicants_int,
    applicant_level,
    seniority_level,
    employment_type,
    scraped_at
from {{ ref('silver_linkedin_data') }}
