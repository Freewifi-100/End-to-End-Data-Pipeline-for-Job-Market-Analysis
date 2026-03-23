{{ config(materialized='incremental') }}
{% set incremental_col = 'scraped_at' %}

with base as(
    
select distinct job_id, job_website, search_keyword, job_url,
    job_title, company_name, company_url, time_posted, num_applicants, 
    location, seniority_level, employment_type, job_function, industries, job_description, scraped_at
from {{ source('source', 'linkedin_data') }}
),
add_id as(
    select
        md5(concat(job_id, search_keyword, scraped_at)) as id,
        job_id,
        job_website,
        search_keyword,
        job_url,
        job_title,
        company_name,
        company_url,
        time_posted,
        num_applicants,
        location,
        seniority_level,
        employment_type,
        job_function,
        industries,
        job_description,
        scraped_at
    from base
)

select * from add_id


{# {% if incremental_flag == 1 %} #}
{% if is_incremental() %}
    {# WHERE {{ incremental_col }} > (select coalesce(max({{ incremental_col }}), '1900-01-01') from {{ ref('bronze_bookings') }}) #}
    WHERE {{ incremental_col }} > (select coalesce(max({{ incremental_col }}), '1900-01-01') from {{ this }})
{% endif %} 