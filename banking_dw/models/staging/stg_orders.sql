with source as (
    select * from {{ ref('raw_orders') }}
),
renamed as (
    select
        order_id,
        customer_id,
        order_date,
        status as order_status,
        -- Konversi cents ke satuan mata uang biasa
        amount_cents / 100.0 as total_amount
    from source
)
select * from renamed