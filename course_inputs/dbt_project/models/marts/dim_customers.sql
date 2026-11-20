-- מימד לקוחות עם מדדי ערך (LTV) - שימושי גם כפיצ'רים למודל ML
select
    c.customer_id,
    c.full_name,
    c.email,
    c.city,
    c.signup_date,
    count(o.order_id) filter (where o.status = 'paid')            as paid_orders,
    coalesce(sum(o.amount) filter (where o.status = 'paid'), 0)   as lifetime_value,
    max(o.order_date)                                             as last_order_date
from {{ ref('stg_customers') }} c
left join {{ ref('fct_orders') }} o using (customer_id)
group by all
