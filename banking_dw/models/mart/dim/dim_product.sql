{{ config(materialized='table') }}

with stg_order_items as (
    select * from {{ ref('stg_order_items') }}
)
select distinct
    product_name,
    category
from stg_order_items