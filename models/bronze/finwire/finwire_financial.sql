
with s1 as (
    select 
        *,
        SAFE_CAST(CO_NAME_OR_CIK AS NUMERIC) as try_cik
    from {{ source("finwire", "FIN") }}
)
select 
    PTS,
    CAST(YEAR AS INT64) as year,
    CAST(QUARTER AS INT64) as quarter,
    PARSE_DATE('%Y%m%d', QUARTER_START_DATE) as quarter_start_date,
    PARSE_DATE('%Y%m%d', POSTING_DATE) as posting_date,
    cast(REVENUE AS FLOAT64) as revenue,
    cast(EARNINGS as FLOAT64) as earnings,
    cast(EPS as FLOAT64) as eps,
    cast(DILUTED_EPS as FLOAT64) as diluted_eps,
    cast(MARGIN as FLOAT64) as margin,
    cast(INVENTORY as FLOAT64) as inventory,
    cast(ASSETS as FLOAT64) as assets,
    cast(LIABILITIES as FLOAT64) as liabilities,
    CAST(SH_OUT AS INT64) as sh_out,
    CAST(DILUTED_SH_OUT AS INT64) as diluted_sh_out,
    try_cik cik,
    case when try_cik is null then CO_NAME_OR_CIK else null end company_name
from s1