{{
    config(
        materialized = 'table',
        partition_by = {
            'field': 'order_date',
            'data_type': 'date',
            'granularity': 'day'
        }
    )
}}

-- Bảng tổng hợp doanh thu theo NGÀY — nguồn cho dashboard điều hành.
--
-- Bảng này nhỏ (vài trăm dòng) nên dashboard query cực rẻ, thay vì bắt
-- BI tool quét lại hàng triệu dòng fact mỗi lần mở biểu đồ.

with orders as (

    select * from {{ ref('fct_orders') }}

),

daily as (

    select
        order_date,
        order_month,
        order_year,

        count(*)                                        as total_orders,
        countif(is_revenue_order)                       as revenue_orders,
        countif(order_status = 'cancelled')             as cancelled_orders,
        countif(order_status = 'returned')              as returned_orders,
        count(distinct customer_id)                     as active_customers,

        sum(total_quantity)                             as units_sold,
        sum(revenue)                                    as revenue,
        sum(if(is_revenue_order, discount_amount, 0))   as discount_amount,
        sum(if(is_revenue_order, gross_profit, 0))      as gross_profit

    from orders
    group by order_date, order_month, order_year

),

final as (

    select
        order_date,
        order_month,
        order_year,

        total_orders,
        revenue_orders,
        cancelled_orders,
        returned_orders,
        active_customers,
        units_sold,

        revenue,
        discount_amount,
        gross_profit,

        round(safe_divide(revenue, revenue_orders), 2)  as avg_order_value,
        round(
            safe_divide(cancelled_orders, total_orders) * 100, 2
        )                                               as cancellation_rate_pct,
        round(
            safe_divide(gross_profit, revenue) * 100, 2
        )                                               as gross_margin_pct,

        -- Doanh thu trung bình động 7 ngày, làm mượt biến động cuối tuần
        round(avg(revenue) over (
            order by order_date
            rows between 6 preceding and current row
        ), 2)                                           as revenue_7d_avg,

        -- So với cùng kỳ hôm trước, để phát hiện sụt giảm bất thường
        lag(revenue) over (order by order_date)         as revenue_prev_day

    from daily

)

select * from final
