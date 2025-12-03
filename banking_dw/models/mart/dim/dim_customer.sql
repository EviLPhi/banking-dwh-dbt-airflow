{{ config(materialized='table') }}

with stg_customers as (
    select * from {{ ref('stg_customers') }}
)
select 
    customer_id as customer_key,
    full_name,
    email,
    last_updated_date
from stg_customers