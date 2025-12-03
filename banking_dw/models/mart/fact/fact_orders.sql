{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),
order_items as (
    select * from {{ ref('stg_order_items') }}
)

select
    o.order_id,
    o.customer_id as customer_key,
    o.order_date,
    o.order_status,
    i.product_name, -- Sebagai FK ke Dim_Product (Natural Key)
    i.quantity,
    i.unit_price,
    (i.quantity * i.unit_price) as line_total_amount
from orders o
join order_items i on o.order_id = i.order_id