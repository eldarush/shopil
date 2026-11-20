select
    order_date,
    count(*)     as orders,
    sum(amount)  as revenue
from {{ ref('fct_orders') }}
where status = 'paid'
group by order_date
order by order_date
