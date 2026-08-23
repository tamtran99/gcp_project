-- File trong analyses/ được dbt COMPILE ra SQL nhưng KHÔNG tạo bảng/view.
-- Dùng cho query khảo sát dùng một lần mà vẫn muốn tận dụng ref().
--
-- Xem SQL đã biên dịch:
--   dbt compile --select top_products_by_month
--   -> target/compiled/ecommerce_analytics/analyses/top_products_by_month.sql

with monthly_product_sales as (

    select
        order_month,
        category,
        product_name,
        sum(revenue)    as revenue,
        sum(quantity)   as units_sold

    from {{ ref('fct_order_items') }}
    where is_revenue_order
    group by order_month, category, product_name

),

ranked as (

    select
        *,
        row_number() over (
            partition by order_month
            order by revenue desc
        ) as rn

    from monthly_product_sales

)

select
    order_month,
    rn as rank,
    category,
    product_name,
    revenue,
    units_sold

from ranked
where rn <= 5
order by order_month desc, rn
