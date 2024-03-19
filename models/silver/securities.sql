select
    symbol,
    issue_type,
    case s.status
        when 'ACTV' then 'Active'
        when 'INAC' then 'Inactive'
        else null
    end status,
    s.name,
    ex_id exchange_id,
    sh_out shares_outstanding,
    first_trade_date,
    first_exchange_date,
    dividend,
    coalesce(c1.name,c2.name) company_name,
    coalesce(c1.company_id, c2.company_id) company_id,
    pts as effective_timestamp,
   ifnull(
        timestamp_add(
            lag(pts) over (
                partition by symbol
                order by
                pts desc
            ),
            interval -1 millisecond
        ),
        timestamp '9999-12-31 23:59:59.999'
    ) as end_timestamp,
    CASE
        WHEN (
            row_number() over (
                partition by symbol
                order by
                pts desc
            ) = 1
        ) THEN TRUE
        ELSE FALSE
    END as IS_CURRENT
FROM {{ ref('finwire_security') }} AS s
LEFT JOIN {{ ref('companies') }} AS c1
ON LPAD(CAST(s.cik AS STRING), LENGTH(c1.company_id), '0') = c1.company_id
AND pts BETWEEN c1.effective_timestamp AND c1.end_timestamp
LEFT JOIN {{ ref('companies') }} AS c2
ON s.company_name = c2.name
AND pts BETWEEN c2.effective_timestamp AND c2.end_timestamp
