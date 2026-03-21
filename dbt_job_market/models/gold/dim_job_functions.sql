{{ config(materialized='table') }}

SELECT DISTINCT
    function_key,
     job_function

FROM {{ ref('silver_job_functions') }}
