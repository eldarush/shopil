-- ניקוי הזמנות: סכום מטקסט למספר, סטטוס אחיד, תאריכים מעורבים
select
    cast(order_id as integer)                                    as order_id,
    cast(customer_id as integer)                                 as customer_id,
    cast(product_id as integer)                                  as product_id,
    cast(quantity as integer)                                    as quantity,
    try_cast(replace(cast(amount as varchar), '₪', '') as decimal(10, 2)) as amount,
    lower(trim(status))                                          as status,
    coalesce(
        try_strptime(cast(order_date as varchar), '%Y-%m-%d'),
        try_strptime(cast(order_date as varchar), '%d/%m/%Y')
    )::date                                                      as order_date,
    cast(updated_at as timestamp)                                as updated_at
from {{ source('raw', 'orders') }}
