with source as (
    select * from {{ ref('raw_products') }}
),
renamed as (
    select
        -- Karena product_id di csv unik per baris, kita anggap sebagai item_id
        product_id as item_id,
        order_id,
        product_name,
        category,
        quantity,
        price_cents / 100.0 as unit_price
    from source
)
select * from renamed