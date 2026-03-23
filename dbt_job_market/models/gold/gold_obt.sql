{%
    set configs = [
        {
            "table": "dbt_job_market.silver.silver_linkedin_data",
            "columns": "silver_linkedin_data.*",
            "alias": "silver_linkedin_data",
        },
        {
            "table": "dbt_job_market.silver.silver_job_industries",
            "columns": "job_industries.industry_name as each_industry",
            "alias": "job_industries",
            "join_condition": "silver_linkedin_data.job_id = job_industries.job_id",
        },
        {
            "table": "dbt_job_market.silver.silver_job_functions",
            "columns": "job_functions.job_function as each_function",
            "alias": "job_functions",
            "join_condition": "silver_linkedin_data.job_id = job_functions.job_id",
        },
    ]
%}

select distinct
    {% for config in configs %}
        {{ config['columns'] }}{% if not loop.last %}, {% endif %}
    {% endfor %}
from 
    {% for config in configs %}
        {% if loop.first %}
            {{ config['table'] }} as {{ config['alias'] }}
        {% else %}
            left join {{ config['table'] }} as {{ config['alias'] }}
            on {{ config['join_condition'] }}
        {% endif %}
    {% endfor %}
