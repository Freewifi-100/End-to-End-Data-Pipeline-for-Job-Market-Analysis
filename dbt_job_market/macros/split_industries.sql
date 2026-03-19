{% macro split_industries(column_name) %}

SPLIT(
    REGEXP_REPLACE({{ column_name }}, '\\s+and\\s+', ','),
    ',\\s*'
)

{% endmacro %}