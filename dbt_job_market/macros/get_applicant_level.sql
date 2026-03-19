{% macro get_applicant_level(column_name) %}

CASE
    WHEN {{ get_num_applicants(column_name) }} < 25 THEN 'low'
    WHEN {{ get_num_applicants(column_name) }} < 100 THEN 'moderate'
    WHEN {{ get_num_applicants(column_name) }} <= 200 THEN 'high'
    ELSE 'very high'
END

{% endmacro %}