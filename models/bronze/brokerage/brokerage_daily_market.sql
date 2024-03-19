
select
    *
from {{ source("brokerage", "DAILY_MARKET") }}
