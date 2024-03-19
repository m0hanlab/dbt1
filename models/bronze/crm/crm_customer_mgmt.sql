select *
from {{ source('crm', 'CUSTOMER_MGMT') }}