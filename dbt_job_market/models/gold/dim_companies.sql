{{ config(materialized='table') }}

select distinct
    md5(company_name || company_url ) as company_key,
    company_name,
    company_url
from {{ ref('silver_linkedin_data') }}