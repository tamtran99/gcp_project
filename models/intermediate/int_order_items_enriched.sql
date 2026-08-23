{{
    config(
        materialized = 'table',
        partition_by = {
            'field': 'order_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by = ['product_id']
    )
}}

-- Ghép dòng hàng với header đơn và thông tin sản phẩm.
--
-- Mặc định tầng intermediate trong dbt_project.yml là 'ephemeral' — dbt
-- chèn SQL thẳng vào model cha dưới dạng CTE, không tạo đối tượng nào
-- trên warehouse. Hợp lý khi model trung gian chỉ có 1-2 nơi dùng.
--
-- Nhưng model này được 4 mart dùng lại (dim_customers, dim_products,
-- fct_orders, fct_order_items). Để ephemeral thì BigQuery phải chạy lại
-- cả join 3 bảng 4 lần, tức trả tiền quét dữ liệu 4 lần. Nên ở đây ghi
-- đè thành 'table': tính một lần, bốn mart cùng đọc.

with order_items as (

    select * from {{ ref('stg_ecommerce__order_items') }}

),

orders as (

    select * from {{ ref('stg_ecommerce__orders') }}

),

products as (

    select * from {{ ref('stg_ecommerce__products') }}

),

joined as (

    select
        oi.order_item_id,
        oi.order_id,
        oi.product_id,

        o.customer_id,
        o.order_date,
        o.order_month,
        o.order_year,
        o.order_status,
        o.payment_method,
        o.is_revenue_order,

        p.product_name,
        p.category,
        p.brand,
        p.cost_price,

        oi.quantity,
        oi.unit_price,
        oi.discount_pct,
        oi.gross_amount,
        oi.discount_amount,
        oi.net_amount,

        -- Giá vốn hàng bán của dòng này
        p.cost_price * oi.quantity          as cost_amount,

        -- Lợi nhuận gộp = doanh thu thực thu - giá vốn
        oi.net_amount - (p.cost_price * oi.quantity)
                                            as gross_profit

    from order_items as oi
    inner join orders as o
        on oi.order_id = o.order_id
    -- left join: nếu sản phẩm bị xoá khỏi danh mục thì vẫn giữ dòng hàng,
    -- không được làm mất doanh thu đã phát sinh
    left join products as p
        on oi.product_id = p.product_id

)

select * from joined
