{% macro get_num_applicants(column_name) %}

CASE
    WHEN {{ column_name }} LIKE 'Be among%' THEN 24 
    WHEN {{ column_name }} LIKE '%Over%' THEN 201     
    ELSE CAST(REGEXP_EXTRACT({{ column_name }}, '(\\d+)', 1) AS INT)
END

{% endmacro %}