-- Dòng chi tiết đơn hàng, tính sẵn thành tiền trước và sau chiết khấu.
--
-- Lưu ý về kiểu số: unit_price là NUMERIC. Không chia cho 100.0 (FLOAT64)
-- vì BigQuery không tự ép NUMERIC với FLOAT64. Viết dạng
-- "* (100 - discount_pct) / 100" để mọi phép tính ở lại trong NUMERIC,
-- tránh sai số thập phân khi cộng dồn tiền.

with source as (

    select * from {{ source('ecommerce', 'order_items') }}

),

cleaned as (

    select
        order_item_id,
        order_id,
        product_id,

        quantity,
        unit_price,
        coalesce(discount_pct, 0)                       as discount_pct,

        -- Thành tiền trước chiết khấu
        unit_price * quantity                           as gross_amount,

        -- Số tiền được giảm
        unit_price * quantity * coalesce(discount_pct, 0) / 100
                                                        as discount_amount,

        -- Thành tiền thực thu
        unit_price * quantity * (100 - coalesce(discount_pct, 0)) / 100
                                                        as net_amount

    from source

)

select * from cleaned
