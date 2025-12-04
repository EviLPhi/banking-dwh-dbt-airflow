with source as (
    select * from {{ ref('transactions') }}
),
renamed as (
    select
        transaction_id,
        account_id,
        transaction_date,
        transaction_type,
        amount as transaction_amount,
        status,
        channel,
        location
    from source
)
select * from renamed