{{ config(materialized='incremental',
    unique_key = 'id'
    ) 
}}

select
    id,
    job_id,
    job_website,
    search_keyword,
    job_url,
    job_title,
    company_name,
    company_url,
    time_posted,
    {{ convert_time_posted('time_posted', 'scraped_at') }} AS posted_timestamp,
    num_applicants,
    {{get_num_applicants('num_applicants')}} as num_applicants_int,
    {{ get_applicant_level('num_applicants') }} as applicant_level,
    location,
    {{ location_fields('location') }},
    seniority_level,
    employment_type,
    job_function,
    industries,
    job_description,
    scraped_at
from {{ ref('bronze_linkedin_data') }}