{{
    config(
        materialized = 'table',
        cluster_by = ['country']
    )
}}

-- Bảng chiều khách hàng: thuộc tính gốc + các chỉ số hành vi mua hàng.
-- Đây là bảng BI hay dùng nhất nên gộp sẵn metric vào, tránh để dashboard
-- phải tự join lại fact mỗi lần.

with customers as (

    select * from {{ ref('stg_ecommerce__customers') }}

),

order_items as (

    select * from {{ ref('int_order_items_enriched') }}

),

-- Tổng hợp ở mức khách hàng, chỉ tính đơn có doanh thu
customer_metrics as (

    select
        customer_id,
        count(distinct order_id)     as total_orders,
        sum(quantity)                as total_items_bought,
        sum(net_amount)              as lifetime_value,
        sum(gross_profit)            as lifetime_gross_profit,
        min(order_date)              as first_order_date,
        max(order_date)              as last_order_date

    from order_items
    where is_revenue_order
    group by customer_id

),

final as (

    select
        c.customer_id,
        c.full_name,
        c.first_name,
        c.last_name,
        c.email,
        c.email_domain,
        c.country,
        c.city,
        c.signup_date,
        c.is_active,

        -- Khách chưa mua lần nào -> 0 chứ không phải NULL, để dashboard
        -- cộng/trung bình không bị lệch
        coalesce(m.total_orders, 0)             as total_orders,
        coalesce(m.total_items_bought, 0)       as total_items_bought,
        coalesce(m.lifetime_value, 0)           as lifetime_value,
        coalesce(m.lifetime_gross_profit, 0)    as lifetime_gross_profit,

        m.first_order_date,
        m.last_order_date,

        -- Giá trị đơn hàng trung bình
        round(
            safe_divide(m.lifetime_value, m.total_orders), 2
        )                                       as avg_order_value,

        -- Số ngày từ đăng ký tới đơn đầu tiên (đo tốc độ kích hoạt)
        date_diff(m.first_order_date, c.signup_date, day)
                                                as days_to_first_order,

        date_diff(current_date(), m.last_order_date, day)
                                                as days_since_last_order,

        -- Phân khúc vòng đời. Ngưỡng churn lấy từ var trong dbt_project.yml
        case
            when m.total_orders is null then 'never_purchased'
            when date_diff(current_date(), m.last_order_date, day)
                 > {{ var('churn_threshold_days') }} then 'churned'
            when m.total_orders = 1 then 'one_time'
            when m.total_orders between 2 and 5 then 'repeat'
            else 'loyal'
        end                                     as customer_segment

    from customers as c
    left join customer_metrics as m
        on c.customer_id = m.customer_id

)

select * from final
