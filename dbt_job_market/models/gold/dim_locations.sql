{{ config(materialized='table') }}


SELECT DISTINCT
    md5(location) as location_key,
    location,
    country,
    province,
    district
FROM {{ ref('silver_linkedin_data') }}
WHERE location IS NOT NULL
