with source as (
    select * from {{ ref('accounts') }}
),
renamed as (
    select
        account_id,
        customer_id,
        account_number,
        account_type,
        account_status,
        current_balance,
        open_date
    from source
)
select * from renamed
