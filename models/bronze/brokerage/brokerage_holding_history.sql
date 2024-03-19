select *
from {{ source('brokerage', 'HOLDING_HISTORY') }}