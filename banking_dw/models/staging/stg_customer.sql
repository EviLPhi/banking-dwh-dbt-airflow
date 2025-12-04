with source as (
    select * from {{ ref('customers') }}
),
renamed as (
    select
        customer_id,
        first_name,
        last_name,
        -- Menggabungkan nama
        concat(first_name, ' ', last_name) as full_name,
        email,
        city,
        risk_rating,
        income_level,
        occupation,
        last_updated as last_updated_at
    from source
)
select * from renamed