-- Singular test: doanh thu ở fct_orders phải khớp tổng dòng hàng ở
-- fct_order_items. Nếu lệch nghĩa là logic rollup đã sai ở đâu đó.
--
-- Quy ước dbt: test PASS khi câu SQL này trả về 0 dòng.
-- Chạy riêng:  dbt test --select assert_order_revenue_matches_items

with order_level as (

    select
        order_id,
        revenue as order_revenue
    from {{ ref('fct_orders') }}

),

item_level as (

    select
        order_id,
        sum(revenue) as items_revenue
    from {{ ref('fct_order_items') }}
    group by order_id

),

compared as (

    select
        o.order_id,
        o.order_revenue,
        i.items_revenue,
        abs(o.order_revenue - coalesce(i.items_revenue, 0)) as diff

    from order_level as o
    left join item_level as i
        on o.order_id = i.order_id

)

select *
from compared
-- Ngưỡng 0.01 để bỏ qua sai lệch làm tròn ở đơn vị xu
where diff > 0.01
