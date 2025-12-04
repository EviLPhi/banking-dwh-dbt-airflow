with transactions as (
    select * from {{ ref('stg_transaction') }}
),
daily_agg as (
    select
        account_id,
        transaction_date,
        count(transaction_id) as total_transactions,
        sum(case 
            when transaction_type in ('Deposit', 'Transfer In') then transaction_amount 
            when transaction_type in ('Withdrawal', 'Transfer Out', 'Payment') then -transaction_amount
            else 0 
        end) as daily_net_movement
    from transactions
    where status = 'Completed'
    group by 1, 2
)
select * from daily_agg