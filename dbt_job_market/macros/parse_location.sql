{% macro location_fields(column_name) %}

-- split once
{% set col = column_name %}

{% set loc_array = "SPLIT(" ~ col ~ ", ',\\\\s*')" %}

-- country
CASE
    WHEN {{ col }} LIKE '%Bangkok Metropolitan Area%' THEN 'Thailand'
    WHEN {{ col }} LIKE '%Bangkok City%' THEN 'Thailand'
    WHEN SIZE({{ loc_array }}) == 1 THEN
        TRIM({{ loc_array }}[SIZE({{ loc_array }}) - 1])
END AS country,

-- province
CASE
    WHEN {{ col }} LIKE '%Metropolitan Area%' THEN 'Bangkok'
    WHEN {{ col }} LIKE '%Bangkok City%' THEN 'Bangkok'
    WHEN SIZE({{ loc_array }}) >= 2 THEN
        TRIM({{ loc_array }}[SIZE({{ loc_array }}) - 2])
END AS province,

-- district
CASE
    WHEN SIZE({{ loc_array }}) = 3 THEN
        TRIM({{ loc_array }}[0])
END AS district

{% endmacro %}