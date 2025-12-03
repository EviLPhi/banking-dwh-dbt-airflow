with source as (
    select * from {{ ref('raw_customers') }}
),
renamed as (
    select
        customer_id,
        first_name,
        last_name,
        -- Concatenate nama untuk memudahkan analisis
        first_name || ' ' || last_name as full_name,
        email,
        updated_at as last_updated_date
    from source
)
select * from renamed