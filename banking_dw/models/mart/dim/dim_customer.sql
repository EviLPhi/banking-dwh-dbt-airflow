{{ config(materialized='table') }}

with stg_customer as (
    select * from {{ ref('stg_customer') }}
)
select 
    customer_id as customer_key,
    full_name,
    email,
    city,
    occupation,
    risk_rating
from stg_customer