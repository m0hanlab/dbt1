with __dbt__cte__wrk_company_financials as (
select
    sk_company_id,
    f.company_id,
    QUARTER_START_DATE,
    sum(eps) over (
        partition by f.company_id
        order by QUARTER_START_DATE
        rows between 4 preceding and current row
    ) - eps sum_basic_eps
from {{ ref("financials") }} f
join {{ ref("dim_company") }} c 
on f.company_id = c.company_id
and f.effective_timestamp between c.effective_timestamp and c.end_timestamp
) SELECT
    s.sk_security_id,
    s.sk_company_id,
    dm_date sk_date_id,
    --dmh.dm_close / sum_basic_eps AS peratio,
    (s.dividend / dmh.dm_close) / 100 yield,
    fifty_two_week_high,
    fifty_two_week_high_date sk_fifty_two_week_high_date,
    fifty_two_week_low,
    fifty_two_week_low_date sk_fifty_two_week_low_date,
    dm_close closeprice,
    dm_high dayhigh,
    dm_low daylow,
    dm_vol volume
FROM {{ ref("daily_market") }} dmh
JOIN {{ ref("dim_security") }} s
ON s.symbol = dmh.dm_s_symb
    AND TIMESTAMP(dmh.dm_date) BETWEEN  s.effective_timestamp and s.end_timestamp
LEFT JOIN {{ ref("wrk_company_financials")}} f
USING (sk_company_id)
