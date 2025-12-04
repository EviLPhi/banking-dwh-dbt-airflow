{{ config(materialized='table') }}

with transactions as (
    select * from {{ ref('stg_transaction') }}
),
accounts as (
    select * from {{ ref('stg_account') }}
)

select
    t.transaction_id,
    t.account_id as account_key,
    a.customer_id as customer_key,
    t.transaction_date,
    t.transaction_type,
    t.channel,
    t.transaction_amount,
    t.status
from transactions t
left join accounts a on t.account_id = a.account_id