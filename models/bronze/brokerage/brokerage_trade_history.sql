select *
from {{ source('brokerage', 'TRADE_HISTORY') }}
