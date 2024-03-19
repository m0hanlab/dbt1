
select
    *
from {{ source("finwire", "CMP") }}
