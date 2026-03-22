{{
  config(
    materialized = 'table',
    )
}}

SELECT DISTINCT
  md5(cast(date(posted_timestamp) as string)) as posted_date_key,
  posted_timestamp,
  year(posted_timestamp)  as posted_year,
  month(posted_timestamp) as posted_month,
  day(posted_timestamp)   as posted_day,
  date_format(posted_timestamp, 'HH:mm:ss') as posted_time
from {{ ref('silver_linkedin_data') }}
