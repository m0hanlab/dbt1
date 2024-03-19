select
    *
from {{ source("brokerage", "WATCH_HISTORY") }}