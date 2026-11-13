-- Adapted from exercises/starter/mart_product_reviews.sql in the ShopIL ETL course lab.
with reviews_by_product as (
    select
        product_id,
        count(*) as review_count,
        round(avg(rating), 2) as avg_rating,
        count(*) filter (where rating <= 2) as negative_reviews
    from {{ ref('stg_reviews') }}
    group by product_id
),

paid_orders_by_product as (
    select
        product_id,
        count(*) as paid_orders,
        sum(amount) as revenue
    from {{ ref('fct_orders') }}
    where status = 'paid'
    group by product_id
)

select
    products.product_id,
    products.product_name,
    products.category,
    coalesce(reviews.review_count, 0) as review_count,
    reviews.avg_rating,
    coalesce(reviews.negative_reviews, 0) as negative_reviews,
    coalesce(orders.paid_orders, 0) as paid_orders,
    coalesce(orders.revenue, 0) as revenue
from {{ ref('stg_products') }} as products
left join reviews_by_product as reviews using (product_id)
left join paid_orders_by_product as orders using (product_id)
