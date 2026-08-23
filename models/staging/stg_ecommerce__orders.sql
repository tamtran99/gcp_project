-- Header đơn hàng đã chuẩn hoá, kèm vài cột lịch dùng lại nhiều lần.
--
-- var('start_date') khai báo trong dbt_project.yml. Lọc ngay tại staging
-- giúp mọi model phía sau chỉ quét đúng phần partition cần thiết.

with source as (

    select *
    from {{ source('ecommerce', 'orders') }}
    where order_date >= date('{{ var("start_date") }}')

),

cleaned as (

    select
        order_id,
        customer_id,
        order_date,

        lower(trim(order_status))       as order_status,
        lower(trim(payment_method))     as payment_method,

        -- Cột lịch để group theo tháng/quý mà không phải viết lại
        date_trunc(order_date, month)   as order_month,
        date_trunc(order_date, week)    as order_week,
        extract(year from order_date)   as order_year,

        -- Quy tắc nghiệp vụ: chỉ đơn không huỷ/không trả mới tính doanh thu.
        -- Định nghĩa một lần ở đây, tầng sau chỉ việc dùng.
        lower(trim(order_status)) not in ('cancelled', 'returned')
            as is_revenue_order

    from source

)

select * from cleaned
