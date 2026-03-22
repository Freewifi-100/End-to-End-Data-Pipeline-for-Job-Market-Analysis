{{ config(materialized='table') }}

SELECT DISTINCT
    md5(TRIM(job_function)) as function_key,
    job_function

FROM {{ ref('silver_job_functions') }}
