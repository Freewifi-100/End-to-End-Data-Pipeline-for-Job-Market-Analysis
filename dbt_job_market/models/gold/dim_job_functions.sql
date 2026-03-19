{{ config(materialized='table') }}

SELECT DISTINCT
    job_id,
    job_function

FROM {{ ref('silver_job_functions') }}
