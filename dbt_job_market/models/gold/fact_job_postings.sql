{{ config(
    materialized='table',
) }}
SELECT
    id as fact_id,
    md5(job_id || scraped_at) as job_key,
    md5(company_name || company_url ) as company_key,
    md5(location) as location_key,
    md5(cast(date(posted_timestamp) as string)) as posted_date_key,
    num_applicants_int as applicantcants_count,
    applicant_level,
    scraped_at

FROM {{ ref('silver_linkedin_data') }}
