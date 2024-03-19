select *
from {{ source('brokerage', 'TRADE') }}
