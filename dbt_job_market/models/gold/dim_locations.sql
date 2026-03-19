{{ config(materialized='table') }}


SELECT DISTINCT
    location,
    country,
    province,
    district
FROM {{ ref('silver_linkedin_data') }}
WHERE location IS NOT NULL
