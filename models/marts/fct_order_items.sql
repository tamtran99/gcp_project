{{
    config(
        materialized = 'table',
        partition_by = {
            'field': 'order_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by = ['category', 'product_id']
    )
}}

-- Bảng fact ở mức DÒNG HÀNG (một dòng = một order_item).
--
-- Đây là bảng chi tiết nhất, dùng khi cần phân tích theo sản phẩm /
-- ngành hàng. fct_orders trả lời "đơn hàng này bao nhiêu tiền",
-- fct_order_items trả lời "tiền đó đến từ sản phẩm nào".

select
    order_item_id,
    order_id,
    customer_id,
    product_id,

    order_date,
    order_month,
    order_year,
    order_status,
    payment_method,
    is_revenue_order,

    product_name,
    category,
    brand,

    quantity,
    unit_price,
    discount_pct,
    gross_amount,
    discount_amount,
    net_amount,
    cost_amount,
    gross_profit,

    case
        when is_revenue_order then net_amount
        else 0
    end                     as revenue

from {{ ref('int_order_items_enriched') }}
