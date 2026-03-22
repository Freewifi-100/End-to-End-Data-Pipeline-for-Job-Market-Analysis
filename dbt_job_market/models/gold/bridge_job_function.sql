{{
  config(
    materialized = 'table',
    )
}}

with get_dim_job as (
    select
        job_id,
        job_function
    from {{ ref('dim_job') }}
),
get_dim_functions as (
    select
        function_key,
        job_function
    from {{ ref('dim_functions') }}
),
bridge_job_function as (
  select
    job_id,
    job_function
  from {{ ref('silver_job_functions') }}
)

select
    dj.job_id,
    df.function_key
from get_dim_job dj
inner join bridge_job_function bjf
    on dj.job_id = bjf.job_id
inner join get_dim_functions df
    on bjf.job_function = df.job_function