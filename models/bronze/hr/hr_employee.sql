select *
from {{ source('hr', 'HR') }}