with t as (
    select
        cast(ct_ca_id as string) as account_id,
        ct_dts as transaction_timestamp,
        ct_amt as amount,
        ct_name as description
    from
        {{ ref('brokerage_cash_transaction') }}
)
select
    a.customer_id,
    t.*
from
    t
join
    {{ ref('accounts') }} a
on
    t.account_id = a.account_id
and
    t.transaction_timestamp between a.effective_timestamp and a.end_timestamp

