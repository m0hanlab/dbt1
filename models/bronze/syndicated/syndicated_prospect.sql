select *
from {{ source('syndicated', 'PROSPECT') }}