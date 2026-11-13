-- Adapted from exercises/starter/stg_reviews.sql in the ShopIL ETL course lab.
with source as (
    select * from {{ source('raw', 'reviews') }}
),

cleaned as (
    select
        cast(review_id as integer) as review_id,
        cast(order_id as integer) as order_id,
        cast(product__id as integer) as product_id,
        cast(rating as integer) as rating,
        nullif(regexp_replace(text, '^[[:space:]\p{Z}]+|[[:space:]\p{Z}]+$', '', 'g'), '') as review_text,
        cast(created_at as timestamptz) as created_at
    from source
    where rating in (1, 2, 3, 4, 5)
)

select *
from cleaned
where review_text is not null
qualify row_number() over (
    partition by review_id
    order by created_at nulls last, order_id, product_id, rating, review_text
) = 1
