SELECT
distinct
    job_id,
    TRIM(func) AS job_function
FROM {{ ref('bronze_linkedin_data') }}

LATERAL VIEW EXPLODE(
    SPLIT(
        REGEXP_REPLACE(job_function, '\\s+and\\s+', ','), ',\\s*'
    )
) AS func

WHERE 
    TRIM(func) != ''