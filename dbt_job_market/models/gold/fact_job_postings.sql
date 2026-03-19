{{ config(
    materialized='table',
) }}
SELECT
    id,
    job_id,
    search_keyword,
    company_url,
    location

FROM {{ ref('silver_linkedin_data') }}
