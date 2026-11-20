select
    cast(product_id as integer)         as product_id,
    product_name,
    category,
    cast(list_price as decimal(10, 2))  as list_price
from {{ source('raw', 'products') }}
