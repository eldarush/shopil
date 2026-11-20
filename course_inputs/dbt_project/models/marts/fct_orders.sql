-- טבלת עובדות: רק הזמנות תקינות, מועשרות במוצר
select
    o.order_id,
    o.customer_id,
    o.product_id,
    p.product_name,
    p.category,
    o.quantity,
    o.amount,
    o.status,
    o.order_date
from {{ ref('stg_orders') }} o
join {{ ref('stg_products') }} p using (product_id)
join {{ ref('stg_customers') }} c using (customer_id)   -- מסנן הזמנות "יתומות"
where o.amount is not null
  and o.order_date is not null
