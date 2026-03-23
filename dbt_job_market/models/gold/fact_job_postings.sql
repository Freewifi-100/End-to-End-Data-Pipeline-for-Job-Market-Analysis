{{ config(
    materialized='table',
) }}

select
    id as fact_id,
    md5(concat(job_id, job_description, job_title, industries,employment_type, seniority_level, job_function)) as job_key,
    md5(company_name || company_url ) as company_key,
    md5(location) as location_key,
    md5(cast(posted_timestamp as string)) as posted_date_key,
    num_applicants_int as applicantcants_count,
    applicant_level, 
    scraped_at
from {{ ref('silver_linkedin_data') }}

