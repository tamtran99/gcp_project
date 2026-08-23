-- Làm sạch bảng khách hàng: chuẩn hoá email, xử lý ô rỗng, đổi tên cột.
-- Không join, không tính toán nghiệp vụ — đó là việc của tầng sau.

with source as (

    select * from {{ source('ecommerce', 'customers') }}

),

cleaned as (

    select
        customer_id,

        trim(first_name)                                as first_name,
        trim(last_name)                                 as last_name,
        trim(first_name) || ' ' || trim(last_name)      as full_name,

        -- Email thô có thể là "  AN.NGUYEN12@EXAMPLE.COM "
        lower(trim(email))                              as email,

        -- Lấy phần domain để phân tích theo nhà cung cấp mail
        split(lower(trim(email)), '@')[safe_offset(1)]  as email_domain,

        country,

        -- Chuỗi rỗng trong CSV nên trở thành NULL, không phải ''
        nullif(trim(city), '')                          as city,

        signup_date,
        coalesce(is_active, false)                      as is_active

    from source

)

select * from cleaned
