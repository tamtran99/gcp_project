{{
    config(
        materialized = 'incremental',
        unique_key = 'order_id',
        incremental_strategy = 'merge',
        partition_by = {
            'field': 'order_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by = ['customer_id', 'order_status']
    )
}}

-- Bảng fact ở mức ĐƠN HÀNG (một dòng = một order).
--
-- Vì sao incremental: bảng đơn hàng chỉ lớn dần theo thời gian. Chạy lại
-- toàn bộ mỗi ngày là lãng phí tiền quét dữ liệu. Với materialized
-- 'incremental', dbt chỉ xử lý phần dữ liệu mới rồi MERGE vào bảng cũ.
--
-- Chạy lại từ đầu khi cần (ví dụ vừa sửa logic):  dbt run --full-refresh

with orders as (

    select * from {{ ref('stg_ecommerce__orders') }}

    {% if is_incremental() %}
    -- Lùi lại 7 ngày thay vì đúng max(order_date): đơn hàng có thể được
    -- cập nhật trạng thái muộn (pending -> shipped -> completed), cần
    -- quét lại cửa sổ này để bắt được thay đổi.
    where order_date >= (
        select date_sub(max(order_date), interval 7 day) from {{ this }}
    )
    {% endif %}

),

order_items as (

    select * from {{ ref('int_order_items_enriched') }}

),

item_rollup as (

    select
        order_id,
        count(*)                        as line_item_count,
        count(distinct product_id)      as distinct_product_count,
        sum(quantity)                   as total_quantity,
        sum(gross_amount)               as gross_amount,
        sum(discount_amount)            as discount_amount,
        sum(net_amount)                 as net_amount,
        sum(cost_amount)                as cost_amount,
        sum(gross_profit)               as gross_profit

    from order_items
    group by order_id

),

final as (

    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_month,
        o.order_week,
        o.order_year,
        o.order_status,
        o.payment_method,
        o.is_revenue_order,

        coalesce(i.line_item_count, 0)          as line_item_count,
        coalesce(i.distinct_product_count, 0)   as distinct_product_count,
        coalesce(i.total_quantity, 0)           as total_quantity,

        coalesce(i.gross_amount, 0)             as gross_amount,
        coalesce(i.discount_amount, 0)          as discount_amount,
        coalesce(i.net_amount, 0)               as net_amount,
        coalesce(i.cost_amount, 0)              as cost_amount,
        coalesce(i.gross_profit, 0)             as gross_profit,

        -- Chỉ ghi nhận doanh thu cho đơn hợp lệ. Đơn huỷ/trả vẫn giữ lại
        -- trong bảng để phân tích tỉ lệ huỷ, nhưng revenue = 0.
        case
            when o.is_revenue_order then coalesce(i.net_amount, 0)
            else 0
        end                                     as revenue,

        -- Dấu thời gian dbt build ra dòng này, hữu ích khi debug
        current_timestamp()                     as dbt_updated_at

    from orders as o
    left join item_rollup as i
        on o.order_id = i.order_id

)

select * from final
