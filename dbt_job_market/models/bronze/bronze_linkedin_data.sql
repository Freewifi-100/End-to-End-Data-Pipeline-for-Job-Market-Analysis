{{ config(materialized='incremental') }}
{% set incremental_col = 'scraped_at' %}

select * from {{ source('source', 'linkedin_data') }}

{# {% if incremental_flag == 1 %} #}
{% if is_incremental() %}
    {# WHERE {{ incremental_col }} > (select coalesce(max({{ incremental_col }}), '1900-01-01') from {{ ref('bronze_bookings') }}) #}
    WHERE {{ incremental_col }} > (select coalesce(max({{ incremental_col }}), '1900-01-01') from {{ this }})
{% endif %} 