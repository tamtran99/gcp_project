---
description: Tạo model dbt mới đúng quy ước phân tầng của repo
---

Tạo model dbt mới cho: $ARGUMENTS

Trước khi viết, xác định model thuộc tầng nào và tuân thủ đúng ràng buộc của tầng đó:

**staging/** — một model ↔ đúng một bảng nguồn.
- Tên: `stg_<nguồn>__<bảng>` (hai dấu gạch dưới)
- Chỉ đổi tên cột, ép kiểu, làm sạch: `lower(trim(email))`, `nullif(trim(city), '')`
- **Không join, không logic nghiệp vụ.** Vi phạm điều này là lỗi thiết kế, không phải style
- Materialized `view` (đã set mặc định ở dbt_project.yml)
- Đọc nguồn bằng `{{ source('ecommerce', 'ten_bang') }}`

**intermediate/** — bước trung gian, tên `int_<mô tả>`.
- Mặc định `ephemeral`. Chỉ ghi đè thành `table` khi model được **nhiều mart** dùng lại
- Nếu ghi đè, viết rõ lý do trong comment đầu file (xem int_order_items_enriched.sql)

**marts/** — sản phẩm cuối cho BI.
- Tiền tố: `dim_` (bảng chiều) / `fct_` (bảng fact) / `agg_` (tổng hợp sẵn)
- Materialized `table`, partition theo cột ngày hay lọc, cluster theo cột hay dùng trong where/join
- Bảng fact lớn cân nhắc `incremental`

Sau khi viết file .sql:
1. Khai báo model + test trong file `_*.yml` cùng thư mục. Cú pháp dbt 1.12: tham số của
   generic test phải lồng dưới `arguments:`
2. Chạy `dbt build --select <ten_model>` để kiểm tra

Nhắc lại hai bẫy của repo này: giữ mọi phép tính tiền trong NUMERIC (đừng chia cho `100.0`),
và dùng `var()` cho ngưỡng dùng chung thay vì hardcode.
