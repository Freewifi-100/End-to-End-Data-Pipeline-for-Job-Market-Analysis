SELECT
    md5(TRIM(industry)) as industry_key,
    TRIM(industry) AS industry
FROM {{ ref('bronze_linkedin_data') }}

LATERAL VIEW EXPLODE(
    {{ split_industries('industries') }}
) AS industry
WHERE TRIM(industry) != ''