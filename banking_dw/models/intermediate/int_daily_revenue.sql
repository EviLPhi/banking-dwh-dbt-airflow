with orders as (
    select * from {{ ref('stg_orders') }}
),
daily_agg as (
    select
        order_date,
        count(distinct order_id) as total_orders,
        sum(total_amount) as daily_revenue
    from orders
    where order_status = 'completed' -- Hanya hitung yang sukses
    group by 1
)
select * from daily_agg