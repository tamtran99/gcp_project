-- Ghép dòng hàng với header đơn và thông tin sản phẩm.
--
-- Model này materialize là "ephemeral" (khai báo ở dbt_project.yml):
-- dbt sẽ chèn thẳng SQL này vào model cha dưới dạng CTE thay vì tạo
-- bảng/view riêng. Hợp lý vì nó chỉ là bước trung gian, được nhiều mart
-- dùng lại nhưng bản thân không ai query trực tiếp.

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
