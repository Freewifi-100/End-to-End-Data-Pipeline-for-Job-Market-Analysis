{% macro convert_time_posted(posted_time, scraped_at) %}

CASE
    WHEN {{ posted_time }} = 'Just now' THEN {{ scraped_at }}

    WHEN {{ posted_time }} LIKE '%minute%' THEN
        timestampadd(
            MINUTE,
            -CAST(REGEXP_EXTRACT({{ posted_time }}, '(\\d+)', 1) AS INT),
            {{ scraped_at }}
        )

    WHEN {{ posted_time }} LIKE '%hour%' THEN
        timestampadd(
            HOUR,
            -CAST(REGEXP_EXTRACT({{ posted_time }}, '(\\d+)', 1) AS INT),
            {{ scraped_at }}
        )

    WHEN {{ posted_time }} LIKE '%day%' THEN
        timestampadd(
            DAY,
            -CAST(REGEXP_EXTRACT({{ posted_time }}, '(\\d+)', 1) AS INT),
            {{ scraped_at }}
        )

    WHEN {{ posted_time }} LIKE '%week%' THEN
        timestampadd(
            WEEK,
            -CAST(REGEXP_EXTRACT({{ posted_time }}, '(\\d+)', 1) AS INT),
            {{ scraped_at }}
        )

    WHEN {{ posted_time }} LIKE '%month%' THEN
        timestampadd(
            MONTH,
            -CAST(REGEXP_EXTRACT({{ posted_time }}, '(\\d+)', 1) AS INT),
            {{ scraped_at }}
        )

    WHEN {{ posted_time }} LIKE '%year%' THEN
        timestampadd(
            YEAR,
            -CAST(REGEXP_EXTRACT({{ posted_time }}, '(\\d+)', 1) AS INT),
            {{ scraped_at }}
        )

    ELSE NULL
END

{% endmacro %}