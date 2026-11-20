-- Compare each output row with separate scalar queries on the underlying facts.
select m.product_id
from {{ ref('mart_product_reviews') }} m
where m.revenue is distinct from coalesce((
    select sum(o.amount) from {{ ref('fct_orders') }} o
    where o.product_id = m.product_id and o.status = 'paid'
), 0)
   or m.paid_orders is distinct from (
    select count(*) from {{ ref('fct_orders') }} o
    where o.product_id = m.product_id and o.status = 'paid'
)
   or m.review_count is distinct from (
    select count(*) from {{ ref('stg_reviews') }} r where r.product_id = m.product_id
)
   or m.negative_reviews is distinct from (
    select count(*) from {{ ref('stg_reviews') }} r where r.product_id = m.product_id and r.rating <= 2
)
   or m.avg_rating is distinct from (
    select round(avg(r.rating), 2) from {{ ref('stg_reviews') }} r where r.product_id = m.product_id
)
   or not exists (
    select 1 from {{ ref('stg_products') }} p where p.product_id = m.product_id
)
union all
select p.product_id
from {{ ref('stg_products') }} p
where not exists (
    select 1 from {{ ref('mart_product_reviews') }} m where m.product_id = p.product_id
)
