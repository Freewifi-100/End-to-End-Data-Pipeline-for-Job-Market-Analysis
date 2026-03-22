SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY job_id
            ORDER BY scraped_at DESC
        ) AS rn
    FROM {{ ref('silver_linkedin_data') }}
)
WHERE rn = 1