{{ config(materialized='table') }}

WITH company_base AS (
    SELECT DISTINCT
        company_name,
        company_url,
        industries
    FROM {{ ref('silver_linkedin_data') }}
)

SELECT
    row_number() OVER (
        ORDER BY coalesce(company_name, ''), coalesce(company_url, '')
    ) AS company_id,
    company_name,
    company_url,
    industries
FROM company_base