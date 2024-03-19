select *
from {{ source('brokerage', 'CASH_TRANSACTION') }}
