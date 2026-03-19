{{ config(materialized='table') }}

SELECT DISTINCT
    company_url,
    company_name

FROM {{ ref('silver_linkedin_data') }}