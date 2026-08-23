-- Làm sạch danh mục sản phẩm và tính sẵn biên lợi nhuận đơn vị.

with source as (

    select * from {{ source('ecommerce', 'products') }}

),

cleaned as (

    select
        product_id,
        trim(product_name)                  as product_name,
        category,
        brand,

        unit_price,
        cost_price,

        -- Lợi nhuận gộp trên một đơn vị, theo giá niêm yết
        unit_price - cost_price             as unit_gross_profit,

        -- safe_divide trả NULL thay vì lỗi khi unit_price = 0
        round(
            safe_divide(unit_price - cost_price, unit_price) * 100, 2
        )                                   as gross_margin_pct,

        coalesce(is_discontinued, false)    as is_discontinued

    from source

)

select * from cleaned
