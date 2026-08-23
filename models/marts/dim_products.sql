{{
    config(
        materialized = 'table',
        cluster_by = ['category']
    )
}}

-- Bảng chiều sản phẩm: thuộc tính danh mục + hiệu quả bán hàng thực tế.

with products as (

    select * from {{ ref('stg_ecommerce__products') }}

),

order_items as (

    select * from {{ ref('int_order_items_enriched') }}

),

product_metrics as (

    select
        product_id,
        count(distinct order_id)        as total_orders,
        sum(quantity)                   as units_sold,
        sum(net_amount)                 as total_revenue,
        sum(gross_profit)               as total_gross_profit,
        avg(discount_pct)               as avg_discount_pct,
        max(order_date)                 as last_sold_date

    from order_items
    where is_revenue_order
    group by product_id

),

final as (

    select
        p.product_id,
        p.product_name,
        p.category,
        p.brand,
        p.unit_price,
        p.cost_price,
        p.unit_gross_profit,
        p.gross_margin_pct,
        p.is_discontinued,

        coalesce(m.total_orders, 0)         as total_orders,
        coalesce(m.units_sold, 0)           as units_sold,
        coalesce(m.total_revenue, 0)        as total_revenue,
        coalesce(m.total_gross_profit, 0)   as total_gross_profit,

        round(m.avg_discount_pct, 2)        as avg_discount_pct,
        m.last_sold_date,

        -- Xếp hạng doanh thu trong từng ngành hàng
        rank() over (
            partition by p.category
            order by coalesce(m.total_revenue, 0) desc
        )                                   as revenue_rank_in_category,

        -- Sản phẩm chưa bán được lần nào -> ứng viên cần review
        m.product_id is null                as is_never_sold

    from products as p
    left join product_metrics as m
        on p.product_id = m.product_id

)

select * from final
