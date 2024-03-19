with s1 as (
    select *,
    SAFE_CAST(CO_NAME_OR_CIK AS NUMERIC)  as try_cik
    from {{ source("finwire", "SEC") }}
)
select  
    pts,
    symbol,
    issue_type,
    status,
    name,
    ex_id,
    CAST(sh_out as int64) as sh_out,
    parse_date('%Y%m%d',first_trade_date) as first_trade_date,  
    parse_date('%Y%m%d',first_exchange_date) as first_exchange_date,
    cast(dividend as float64) as dividend,
    try_cik cik,
    case when try_cik is null then co_name_or_cik else null end company_name
from s1