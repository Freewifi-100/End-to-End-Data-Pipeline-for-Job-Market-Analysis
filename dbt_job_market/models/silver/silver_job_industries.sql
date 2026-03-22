SELECT
    job_id,
    TRIM(industry) AS industry_name
FROM {{ ref('bronze_linkedin_data') }}

LATERAL VIEW EXPLODE(
    {{ split_industries('industries') }}
) AS industry
WHERE TRIM(industry) != ''